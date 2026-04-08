//! Orchestration layer for hfs3: mirror and pull operations.
//!
//! `mirror_repo` streams files from HuggingFace to S3 with memory-aware concurrency.
//! `pull_repo` downloads all files from S3 to a local directory.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use tokio::sync::Semaphore;

use crate::concurrency::{chunk_size_for_transfer, plan_transfer, plan_transfer_with_memory};
use crate::config::AppConfig;
use crate::error::Hfs3Error;
use crate::hf::{download_file_stream, list_repo_files};
use crate::s3::{S3Ops, UploadParams};
use crate::stats::{spawn_progress_reporter, CountingStream, TransferStats};
use crate::types::{MirrorResult, PullResult, RepoRef};

/// Mirror a HuggingFace repo to S3.
///
/// Lists all files in the repo, computes a memory-aware transfer plan,
/// then streams each file from HF directly into S3 multipart upload
/// with bounded concurrency via a semaphore.
///
/// Individual file failures are logged and skipped; the operation
/// continues with remaining files.
pub async fn mirror_repo(config: &AppConfig, repo: &RepoRef) -> Result<MirrorResult, Hfs3Error> {
    let start = Instant::now();

    let http_client = Client::new();
    let s3_ops = Arc::new(S3Ops::new(config.aws_region.as_deref()).await?);

    let token = config.hf_token.as_deref();
    let repo_type_str = repo.repo_type.to_string();
    let s3_prefix = config.s3_prefix_for(&repo_type_str, &repo.repo_id);

    // List files from HuggingFace
    eprintln!("Listing files for {}/{} ...", repo_type_str, repo.repo_id);
    let files = list_repo_files(&http_client, repo, token).await?;
    eprintln!("Found {} files", files.len());

    if files.is_empty() {
        return Ok(MirrorResult {
            repo_id: repo.repo_id.clone(),
            repo_type: repo_type_str,
            bucket: config.s3_bucket.clone(),
            prefix: s3_prefix,
            files_transferred: 0,
            bytes_transferred: 0,
            duration_secs: start.elapsed().as_secs_f64(),
            stats: None,
        });
    }

    // Compute transfer plan with memory-aware concurrency
    let file_refs: Vec<(&str, u64)> = files.iter().map(|f| (f.path.as_str(), f.size)).collect();
    let plan = match plan_transfer(&file_refs) {
        Ok(p) => p,
        Err(_) => {
            // Fallback: if /proc/meminfo is unavailable, assume 4 GB available
            eprintln!("Warning: could not read /proc/meminfo, using fallback memory estimate");
            plan_transfer_with_memory(&file_refs, 4 * 1024 * 1024 * 1024)
        }
    };

    eprintln!(
        "Transfer plan: max_concurrent={}, chunk_size={}MB, max_parts_in_flight={}, available_memory={}MB",
        plan.max_concurrent,
        plan.chunk_size / (1024 * 1024),
        plan.max_parts_in_flight,
        plan.available_memory / (1024 * 1024),
    );

    // Initialize transfer statistics
    let file_info: Vec<(String, u64)> = files.iter().map(|f| (f.path.clone(), f.size)).collect();
    let stats = Arc::new(TransferStats::new(
        &file_info,
        plan.max_concurrent,
        plan.available_memory,
    ));
    let reporter = spawn_progress_reporter(Arc::clone(&stats), Duration::from_secs(2));

    let semaphore = Arc::new(Semaphore::new(plan.max_concurrent));
    let http_client = Arc::new(http_client);
    let available_memory = plan.available_memory;
    let max_parts = plan.max_parts_in_flight;

    let mut handles = Vec::with_capacity(files.len());

    for (file_idx, file) in files.into_iter().enumerate() {
        let sem = Arc::clone(&semaphore);
        let s3 = Arc::clone(&s3_ops);
        let client = Arc::clone(&http_client);
        let bucket = config.s3_bucket.clone();
        let key = format!("{}/{}", s3_prefix, file.path);
        let file_size = file.size;
        let file_path = file.path.clone();
        let repo_clone = repo.clone();
        let token_owned = config.hf_token.clone();
        let stats_clone = Arc::clone(&stats);

        let handle = tokio::spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|e| Hfs3Error::S3(format!("semaphore error: {e}")))?;

            // RAII guard: marks file active now, marks failed on drop unless completed
            let guard = stats_clone.begin_file(file_idx);

            // Per-file upload params: memory-aware chunk size + concurrent parts
            let upload_params = UploadParams {
                chunk_size: chunk_size_for_transfer(file_size, available_memory),
                max_parts_in_flight: max_parts,
            };

            eprintln!(
                "  Uploading {} ({} bytes, {}MB chunks, {} parts) -> s3://{}/{}",
                file_path,
                file_size,
                upload_params.chunk_size / (1024 * 1024),
                upload_params.max_parts_in_flight,
                bucket,
                key
            );

            let result: Result<(String, u64), Hfs3Error> = async {
                let (stream, _content_length) =
                    download_file_stream(&client, &repo_clone, &file_path, token_owned.as_deref())
                        .await?;

                // Wrap download stream to count bytes
                let dl_stats = Arc::clone(&stats_clone);
                let counting_stream = CountingStream::new(stream, move |n: usize| {
                    dl_stats.add_downloaded(file_idx, n as u64);
                });

                // Upload with per-part progress callback
                let ul_stats = Arc::clone(&stats_clone);
                let bytes = s3
                    .upload_multipart_stream_with_progress(
                        &bucket,
                        &key,
                        counting_stream,
                        file_size,
                        &upload_params,
                        move |part_bytes| ul_stats.part_uploaded(file_idx, part_bytes),
                    )
                    .await?;

                eprintln!("  Done: {} ({} bytes)", file_path, bytes);
                Ok((file_path, bytes))
            }
            .await;

            if result.is_ok() {
                guard.complete();
            }
            // On error, guard drops and marks file failed automatically

            result
        });

        handles.push(handle);
    }

    // Collect results, tolerating individual failures
    let mut files_ok: usize = 0;
    let mut total_bytes: u64 = 0;

    for handle in handles {
        match handle.await {
            Ok(Ok((_path, bytes))) => {
                files_ok += 1;
                total_bytes += bytes;
            }
            Ok(Err(e)) => {
                eprintln!("  Error transferring file: {e}");
            }
            Err(join_err) => {
                eprintln!("  Task panicked: {join_err}");
            }
        }
    }

    // Stop the progress reporter
    reporter.abort();

    let duration = start.elapsed().as_secs_f64();
    let stats_report = stats.report();

    eprintln!(
        "Mirror complete: {}/{} files, {} bytes in {:.1}s ({:.1} MB/s down, {:.1} MB/s up)",
        files_ok,
        stats.total_files,
        total_bytes,
        duration,
        stats_report.download_mbps,
        stats_report.upload_mbps,
    );

    Ok(MirrorResult {
        repo_id: repo.repo_id.clone(),
        repo_type: repo_type_str,
        bucket: config.s3_bucket.clone(),
        prefix: s3_prefix,
        files_transferred: files_ok,
        bytes_transferred: total_bytes,
        duration_secs: duration,
        stats: Some(stats_report),
    })
}

/// Pull a mirrored repo from S3 to a local directory.
///
/// Lists all objects under the repo's S3 prefix, then downloads each
/// to the destination directory, preserving the relative path structure.
pub async fn pull_repo(
    config: &AppConfig,
    repo: &RepoRef,
    dest: &Path,
) -> Result<PullResult, Hfs3Error> {
    let start = Instant::now();

    let s3_ops = S3Ops::new(config.aws_region.as_deref()).await?;
    let repo_type_str = repo.repo_type.to_string();
    let s3_prefix = config.s3_prefix_for(&repo_type_str, &repo.repo_id);

    eprintln!(
        "Pulling s3://{}/{} -> {}",
        config.s3_bucket,
        s3_prefix,
        dest.display()
    );

    let (files_downloaded, bytes_downloaded) = s3_ops
        .download_all(&config.s3_bucket, &s3_prefix, dest)
        .await?;

    let duration = start.elapsed().as_secs_f64();
    eprintln!(
        "Pull complete: {} files, {} bytes in {:.1}s",
        files_downloaded, bytes_downloaded, duration,
    );

    Ok(PullResult {
        repo_id: repo.repo_id.clone(),
        repo_type: repo_type_str,
        dest: dest.to_path_buf(),
        files_downloaded,
        bytes_downloaded,
        duration_secs: duration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RepoType;

    #[test]
    fn test_mirror_empty_file_list_produces_zero_result() {
        // Verify that a MirrorResult with zero files is well-formed
        let result = MirrorResult {
            repo_id: "owner/repo".to_string(),
            repo_type: "model".to_string(),
            bucket: "test-bucket".to_string(),
            prefix: "hfs3-mirror/model/owner--repo".to_string(),
            files_transferred: 0,
            bytes_transferred: 0,
            duration_secs: 0.0,
            stats: None,
        };

        assert_eq!(result.files_transferred, 0);
        assert_eq!(result.bytes_transferred, 0);
        assert_eq!(result.repo_id, "owner/repo");
        assert_eq!(result.repo_type, "model");
        assert_eq!(result.bucket, "test-bucket");
        assert_eq!(result.prefix, "hfs3-mirror/model/owner--repo");

        // Verify JSON serialization
        let json = serde_json::to_string(&result).expect("MirrorResult should serialize to JSON");
        assert!(json.contains("\"files_transferred\":0"));
        assert!(json.contains("\"bytes_transferred\":0"));
        assert!(json.contains("\"repo_id\":\"owner/repo\""));
    }

    #[test]
    fn test_pull_result_serializes_to_json() {
        let result = PullResult {
            repo_id: "meta-llama/Llama-2-7b".to_string(),
            repo_type: "model".to_string(),
            dest: std::path::PathBuf::from("/tmp/llama"),
            files_downloaded: 12,
            bytes_downloaded: 13456789,
            duration_secs: 45.2,
        };

        let json = serde_json::to_string(&result).expect("PullResult should serialize to JSON");
        assert!(json.contains("\"repo_id\":\"meta-llama/Llama-2-7b\""));
        assert!(json.contains("\"repo_type\":\"model\""));
        assert!(json.contains("\"files_downloaded\":12"));
        assert!(json.contains("\"bytes_downloaded\":13456789"));
        assert!(json.contains("\"duration_secs\":45.2"));
        assert!(json.contains("/tmp/llama"));
    }

    #[test]
    fn test_mirror_result_with_nonzero_counts() {
        let result = MirrorResult {
            repo_id: "user/dataset".to_string(),
            repo_type: "dataset".to_string(),
            bucket: "prod-bucket".to_string(),
            prefix: "hfs3-mirror/dataset/user--dataset".to_string(),
            files_transferred: 5,
            bytes_transferred: 1024 * 1024 * 100,
            duration_secs: 12.5,
            stats: None,
        };

        assert_eq!(result.files_transferred, 5);
        assert_eq!(result.bytes_transferred, 104_857_600);

        let json = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("should parse back");
        assert_eq!(parsed["files_transferred"], 5);
        assert_eq!(parsed["bytes_transferred"], 104_857_600u64);
    }

    #[test]
    fn test_s3_prefix_construction_matches_config() {
        let config = AppConfig {
            s3_bucket: "bucket".to_string(),
            s3_prefix: "hfs3-mirror".to_string(),
            hf_token: None,
            aws_region: None,
        };
        let repo = RepoRef {
            repo_id: "meta-llama/Llama-2-7b".to_string(),
            repo_type: RepoType::Model,
            revision: "main".to_string(),
        };

        let prefix = config.s3_prefix_for(&repo.repo_type.to_string(), &repo.repo_id);
        assert_eq!(prefix, "hfs3-mirror/model/meta-llama--Llama-2-7b");
    }
}
