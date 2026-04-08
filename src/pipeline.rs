//! Orchestration layer for hfs3: mirror and pull operations.
//!
//! `mirror_repo` streams files from HuggingFace to S3 with memory-aware concurrency.
//! `pull_repo` downloads all files from S3 to a local directory.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use reqwest::Client;
use tokio::sync::Semaphore;

use crate::concurrency::{plan_transfer, plan_transfer_with_memory};
use crate::config::AppConfig;
use crate::error::Hfs3Error;
use crate::hf::{download_file_stream, list_repo_files};
use crate::s3::S3Ops;
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
        "Transfer plan: max_concurrent={}, chunk_size={}MB, available_memory={}MB",
        plan.max_concurrent,
        plan.chunk_size / (1024 * 1024),
        plan.available_memory / (1024 * 1024),
    );

    let semaphore = Arc::new(Semaphore::new(plan.max_concurrent));
    let http_client = Arc::new(http_client);

    let mut handles = Vec::with_capacity(files.len());

    for file in files {
        let sem = Arc::clone(&semaphore);
        let s3 = Arc::clone(&s3_ops);
        let client = Arc::clone(&http_client);
        let bucket = config.s3_bucket.clone();
        let key = format!("{}/{}", s3_prefix, file.path);
        let file_size = file.size;
        let file_path = file.path.clone();
        let repo_clone = repo.clone();
        let token_owned = config.hf_token.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|e| Hfs3Error::S3(format!("semaphore error: {e}")))?;

            eprintln!("  Uploading {} ({} bytes) -> s3://{}/{}", file_path, file_size, bucket, key);

            let (stream, _content_length) = download_file_stream(
                &client,
                &repo_clone,
                &file_path,
                token_owned.as_deref(),
            )
            .await?;

            let bytes = s3
                .upload_multipart_stream(&bucket, &key, stream, file_size)
                .await?;

            eprintln!("  Done: {} ({} bytes)", file_path, bytes);

            Ok::<(String, u64), Hfs3Error>((file_path, bytes))
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

    let duration = start.elapsed().as_secs_f64();
    eprintln!(
        "Mirror complete: {}/{} files, {} bytes in {:.1}s",
        files_ok,
        files_ok,
        total_bytes,
        duration,
    );

    Ok(MirrorResult {
        repo_id: repo.repo_id.clone(),
        repo_type: repo_type_str,
        bucket: config.s3_bucket.clone(),
        prefix: s3_prefix,
        files_transferred: files_ok,
        bytes_transferred: total_bytes,
        duration_secs: duration,
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
        };

        assert_eq!(result.files_transferred, 5);
        assert_eq!(result.bytes_transferred, 104_857_600);

        let json = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("should parse back");
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
