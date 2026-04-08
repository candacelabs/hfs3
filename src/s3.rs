//! S3 operations for hfs3: multipart upload, download, and listing.
//!
//! Uses aws-sdk-s3 for all S3 interactions. Supports streaming multipart
//! uploads (zero-copy from HF download stream) and adaptive chunk sizing.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as S3Client;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use std::path::Path;
use tokio::io::AsyncWriteExt;

use crate::error::Hfs3Error;

// Re-export chunk_size_for_file so consumers can use it via s3 module.
pub use crate::concurrency::chunk_size_for_file;

/// Threshold below which we use put_object instead of multipart upload.
const PUT_OBJECT_THRESHOLD: u64 = 8 * 1024 * 1024; // 8 MB

/// S3 operations wrapper around aws-sdk-s3 client.
pub struct S3Ops {
    client: S3Client,
}

impl S3Ops {
    /// Create a new S3Ops from AWS config, with an optional region override.
    pub async fn new(region: Option<&str>) -> Result<Self, Hfs3Error> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(r) = region {
            config_loader = config_loader.region(aws_config::Region::new(r.to_owned()));
        }
        let sdk_config = config_loader.load().await;
        let client = S3Client::new(&sdk_config);
        Ok(Self { client })
    }

    /// Create S3Ops from an existing client (useful for testing).
    pub fn from_client(client: S3Client) -> Self {
        Self { client }
    }

    /// Upload a byte stream to S3 using put_object (small files) or multipart upload (large files).
    ///
    /// Returns total bytes uploaded.
    pub async fn upload_multipart_stream(
        &self,
        bucket: &str,
        key: &str,
        stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
    ) -> Result<u64, Hfs3Error> {
        if file_size < PUT_OBJECT_THRESHOLD {
            self.upload_small(bucket, key, stream).await
        } else {
            self.upload_multipart(bucket, key, stream, file_size).await
        }
    }

    /// Upload a small file using put_object (collect entire body first).
    async fn upload_small(
        &self,
        bucket: &str,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
    ) -> Result<u64, Hfs3Error> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(Hfs3Error::Http)?;
            buf.extend_from_slice(&chunk);
        }
        let total = buf.len() as u64;
        let body = ByteStream::from(buf.freeze());

        tracing::info!(bucket, key, bytes = total, "put_object (small file)");

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|e| Hfs3Error::S3(format!("put_object failed for {key}: {e}")))?;

        Ok(total)
    }

    /// Upload a large file using multipart upload with adaptive chunk sizing.
    async fn upload_multipart(
        &self,
        bucket: &str,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
    ) -> Result<u64, Hfs3Error> {
        let chunk_size = chunk_size_for_file(file_size);

        // Create multipart upload
        let create_resp = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Hfs3Error::S3(format!("create_multipart_upload failed for {key}: {e}")))?;

        let upload_id = create_resp
            .upload_id()
            .ok_or_else(|| Hfs3Error::S3("no upload_id returned".into()))?
            .to_string();

        let mut completed_parts: Vec<CompletedPart> = Vec::new();
        let mut part_number: i32 = 1;
        let mut total_bytes: u64 = 0;
        let mut buf = BytesMut::with_capacity(chunk_size);

        // Buffer stream into chunk-sized parts and upload each
        let result: Result<(), Hfs3Error> = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(Hfs3Error::Http)?;
                buf.extend_from_slice(&chunk);

                while buf.len() >= chunk_size {
                    let part_data = buf.split_to(chunk_size).freeze();
                    let part_len = part_data.len() as u64;

                    let part =
                        self.upload_part(bucket, key, &upload_id, part_number, part_data)
                            .await?;
                    completed_parts.push(part);

                    total_bytes += part_len;
                    tracing::info!(
                        bucket,
                        key,
                        part_number,
                        part_bytes = part_len,
                        total_bytes,
                        "uploaded part"
                    );
                    part_number += 1;
                }
            }

            // Upload remaining bytes as final part
            if !buf.is_empty() {
                let part_data = buf.freeze();
                let part_len = part_data.len() as u64;

                let part =
                    self.upload_part(bucket, key, &upload_id, part_number, part_data)
                        .await?;
                completed_parts.push(part);

                total_bytes += part_len;
                tracing::info!(
                    bucket,
                    key,
                    part_number,
                    part_bytes = part_len,
                    total_bytes,
                    "uploaded final part"
                );
            }

            Ok(())
        }
        .await;

        // On failure, abort the multipart upload
        if let Err(e) = result {
            tracing::warn!(
                bucket,
                key,
                upload_id = %upload_id,
                "aborting multipart upload due to error"
            );
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e);
        }

        // Complete the multipart upload
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| {
                Hfs3Error::S3(format!("complete_multipart_upload failed for {key}: {e}"))
            })?;

        tracing::info!(
            bucket,
            key,
            total_bytes,
            parts = part_number,
            "multipart upload complete"
        );

        Ok(total_bytes)
    }

    /// Upload a single part of a multipart upload.
    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<CompletedPart, Hfs3Error> {
        let body = ByteStream::from(data);

        let resp = self
            .client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                Hfs3Error::S3(format!("upload_part {part_number} failed for {key}: {e}"))
            })?;

        let etag = resp
            .e_tag()
            .ok_or_else(|| Hfs3Error::S3(format!("no ETag for part {part_number}")))?
            .to_string();

        Ok(CompletedPart::builder()
            .e_tag(etag)
            .part_number(part_number)
            .build())
    }

    /// Download an object from S3 to a local file.
    ///
    /// Creates parent directories if needed. Returns bytes written.
    pub async fn download_to_file(
        &self,
        bucket: &str,
        key: &str,
        dest: &Path,
    ) -> Result<u64, Hfs3Error> {
        // Create parent directories
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Hfs3Error::S3(format!("get_object failed for {key}: {e}")))?;

        let mut body = resp.body.into_async_read();
        let mut file = tokio::fs::File::create(dest).await?;
        let bytes_written = tokio::io::copy(&mut body, &mut file).await?;
        file.flush().await?;

        tracing::info!(
            bucket,
            key,
            dest = %dest.display(),
            bytes = bytes_written,
            "downloaded to file"
        );

        Ok(bytes_written)
    }

    /// List all objects under a prefix, handling pagination.
    ///
    /// Returns a list of (key, size) tuples.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, u64)>, Hfs3Error> {
        let mut objects: Vec<(String, u64)> = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.client.list_objects_v2().bucket(bucket).prefix(prefix);

            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| Hfs3Error::S3(format!("list_objects_v2 failed: {e}")))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let size = obj.size().unwrap_or(0) as u64;
                    objects.push((key.to_string(), size));
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        tracing::info!(bucket, prefix, count = objects.len(), "listed objects");
        Ok(objects)
    }

    /// Download all objects under a prefix to a local directory.
    ///
    /// Strips the prefix from keys to form local paths.
    /// Returns (files_downloaded, total_bytes).
    pub async fn download_all(
        &self,
        bucket: &str,
        prefix: &str,
        dest_dir: &Path,
    ) -> Result<(usize, u64), Hfs3Error> {
        let objects = self.list_objects(bucket, prefix).await?;
        let mut files_downloaded: usize = 0;
        let mut total_bytes: u64 = 0;

        // Normalize prefix for stripping (ensure trailing slash)
        let strip_prefix = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };

        for (key, _size) in &objects {
            // Strip the prefix to get relative path
            let relative = key.strip_prefix(&strip_prefix).unwrap_or(key);

            if relative.is_empty() {
                continue;
            }

            let dest_path = dest_dir.join(relative);
            let bytes = self.download_to_file(bucket, key, &dest_path).await?;
            total_bytes += bytes;
            files_downloaded += 1;
        }

        tracing::info!(
            bucket,
            prefix,
            files = files_downloaded,
            bytes = total_bytes,
            dest = %dest_dir.display(),
            "download_all complete"
        );

        Ok((files_downloaded, total_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: usize = 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn test_chunk_size_tiny_file() {
        assert_eq!(chunk_size_for_file(1), 8 * MB);
    }

    #[test]
    fn test_chunk_size_zero() {
        assert_eq!(chunk_size_for_file(0), 8 * MB);
    }

    #[test]
    fn test_chunk_size_small_file() {
        assert_eq!(chunk_size_for_file(500 * 1024 * 1024), 8 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_below_1gb() {
        assert_eq!(chunk_size_for_file(GB - 1), 8 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_at_1gb() {
        assert_eq!(chunk_size_for_file(GB), 64 * MB);
    }

    #[test]
    fn test_chunk_size_medium_file() {
        assert_eq!(chunk_size_for_file(2 * GB), 64 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_below_5gb() {
        assert_eq!(chunk_size_for_file(5 * GB - 1), 64 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_at_5gb() {
        assert_eq!(chunk_size_for_file(5 * GB), 128 * MB);
    }

    #[test]
    fn test_chunk_size_large_file() {
        assert_eq!(chunk_size_for_file(10 * GB), 128 * MB);
    }

    #[test]
    fn test_chunk_size_very_large_file() {
        assert_eq!(chunk_size_for_file(100 * GB), 128 * MB);
    }
}
