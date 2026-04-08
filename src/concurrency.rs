//! Memory-aware concurrency planning for hfs3.
//!
//! Reads /proc/meminfo to determine available system memory, then computes
//! an adaptive transfer plan: chunk size based on largest file, and max
//! concurrent transfers based on available RAM.
//!
//! All computation functions are pure and testable without /proc/meminfo.
//! Use `plan_transfer_with_memory` for deterministic testing.

use crate::error::Hfs3Error;

const MB: usize = 1024 * 1024;
const GB: u64 = 1024 * 1024 * 1024;

/// Transfer plan computed from file sizes and available system memory.
#[derive(Debug, Clone)]
pub struct TransferPlan {
    /// Max files to transfer in parallel.
    pub max_concurrent: usize,
    /// Bytes per S3 multipart part.
    pub chunk_size: usize,
    /// Bytes available from /proc/meminfo.
    pub available_memory: u64,
}

/// Adaptive chunk size based on file size.
///   < 1 GB  ->  8 MB chunks
///   < 5 GB  -> 64 MB chunks
///   >= 5 GB -> 128 MB chunks
///
/// This function is defined here rather than in s3.rs so that the concurrency
/// module can compile independently. s3.rs may re-export or call this function.
pub fn chunk_size_for_file(file_size: u64) -> usize {
    if file_size < GB {
        8 * MB
    } else if file_size < 5 * GB {
        64 * MB
    } else {
        128 * MB
    }
}

/// Read MemAvailable from /proc/meminfo.
/// Returns bytes (the file reports kB, multiply by 1024).
pub fn available_memory_bytes() -> Result<u64, Hfs3Error> {
    let contents = std::fs::read_to_string("/proc/meminfo")
        .map_err(Hfs3Error::Io)?;

    for line in contents.lines() {
        if line.starts_with("MemAvailable:") {
            // Format: "MemAvailable:   12345678 kB"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: u64 = parts[1].parse()
                    .map_err(|_| Hfs3Error::Parse("Failed to parse MemAvailable".into()))?;
                tracing::info!(mem_available_kb = kb, mem_available_bytes = kb * 1024, "Read /proc/meminfo");
                return Ok(kb * 1024);
            }
        }
    }
    Err(Hfs3Error::Parse("MemAvailable not found in /proc/meminfo".into()))
}

/// Calculate max concurrent file transfers.
///
/// Assumes each concurrent transfer buffers ~2 chunks in flight
/// (one being uploaded, one being filled from download).
///
/// Formula: available_memory / (chunk_size * 2)
/// Floor: 2 (always allow at least 2 concurrent transfers)
/// Cap: 32 (avoid overwhelming the network/S3)
pub fn calculate_max_concurrency(chunk_size: usize, available_memory: u64) -> usize {
    let bytes_per_transfer = (chunk_size as u64) * 2;
    if bytes_per_transfer == 0 {
        return 2;
    }
    let calculated = (available_memory / bytes_per_transfer) as usize;
    calculated.clamp(2, 32)
}

/// Compute a transfer plan from file list and system memory.
///
/// Uses the largest file to determine chunk size (conservative),
/// then computes concurrency from available RAM.
pub fn plan_transfer(files: &[(&str, u64)]) -> Result<TransferPlan, Hfs3Error> {
    let available = available_memory_bytes()?;
    let max_file_size = files.iter().map(|(_, s)| *s).max().unwrap_or(0);
    let chunk_size = chunk_size_for_file(max_file_size);
    let max_concurrent = calculate_max_concurrency(chunk_size, available);

    tracing::info!(
        available_memory_mb = available / (1024 * 1024),
        chunk_size_mb = chunk_size / (1024 * 1024),
        max_concurrent,
        file_count = files.len(),
        largest_file_mb = max_file_size / (1024 * 1024),
        "Transfer plan computed"
    );

    Ok(TransferPlan {
        max_concurrent,
        chunk_size,
        available_memory: available,
    })
}

/// Same as plan_transfer but accepts pre-computed available memory.
/// Used in tests and when /proc/meminfo is unavailable.
pub fn plan_transfer_with_memory(
    files: &[(&str, u64)],
    available_memory: u64,
) -> TransferPlan {
    let max_file_size = files.iter().map(|(_, s)| *s).max().unwrap_or(0);
    let chunk_size = chunk_size_for_file(max_file_size);
    let max_concurrent = calculate_max_concurrency(chunk_size, available_memory);

    tracing::info!(
        available_memory_mb = available_memory / (1024 * 1024),
        chunk_size_mb = chunk_size / (1024 * 1024),
        max_concurrent,
        file_count = files.len(),
        largest_file_mb = max_file_size / (1024 * 1024),
        "Transfer plan computed"
    );

    TransferPlan {
        max_concurrent,
        chunk_size,
        available_memory,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_max_concurrency_normal() {
        // 16 GB available, 8 MB chunks
        // 16*1024*1024*1024 / (8*1024*1024 * 2) = 1024 -> capped at 32
        let result = calculate_max_concurrency(8 * 1024 * 1024, 16 * 1024 * 1024 * 1024);
        assert_eq!(result, 32);
    }

    #[test]
    fn test_calculate_max_concurrency_low_memory() {
        // 100 MB available, 64 MB chunks
        // 100*1024*1024 / (64*1024*1024 * 2) = 0 -> floored at 2
        let result = calculate_max_concurrency(64 * 1024 * 1024, 100 * 1024 * 1024);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_calculate_max_concurrency_moderate() {
        // 1 GB available, 8 MB chunks
        // 1024*1024*1024 / (8*1024*1024 * 2) = 64 -> capped at 32
        let result = calculate_max_concurrency(8 * 1024 * 1024, 1024 * 1024 * 1024);
        assert_eq!(result, 32);
    }

    #[test]
    fn test_calculate_max_concurrency_small_memory() {
        // 50 MB available, 8 MB chunks
        // 50*1024*1024 / (8*1024*1024 * 2) = 3
        let result = calculate_max_concurrency(8 * 1024 * 1024, 50 * 1024 * 1024);
        assert_eq!(result, 3);
    }

    #[test]
    fn test_calculate_max_concurrency_floor() {
        // 1 MB available, 8 MB chunks -> 0, floored to 2
        let result = calculate_max_concurrency(8 * 1024 * 1024, 1024 * 1024);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_calculate_max_concurrency_zero_chunk() {
        // Edge case: 0 chunk size -> floor at 2
        let result = calculate_max_concurrency(0, 1024 * 1024 * 1024);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_plan_transfer_with_memory() {
        let files = vec![
            ("small.txt", 1024u64),
            ("big.bin", 500 * 1024 * 1024),  // 500 MB -> 8 MB chunks
        ];
        let plan = plan_transfer_with_memory(&files, 1024 * 1024 * 1024); // 1 GB
        assert_eq!(plan.chunk_size, 8 * 1024 * 1024);
        assert!(plan.max_concurrent >= 2);
        assert!(plan.max_concurrent <= 32);
    }

    #[test]
    fn test_plan_transfer_with_large_files() {
        let files = vec![
            ("huge.safetensors", 6 * 1024 * 1024 * 1024u64), // 6 GB -> 128 MB chunks
        ];
        let plan = plan_transfer_with_memory(&files, 32 * 1024 * 1024 * 1024); // 32 GB
        assert_eq!(plan.chunk_size, 128 * 1024 * 1024);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_available_memory_bytes_reads_procfs() {
        // On Linux, /proc/meminfo should exist and return > 0
        let mem = available_memory_bytes().unwrap();
        assert!(mem > 0, "MemAvailable should be positive");
        // Sanity: should be less than 1 TB
        assert!(mem < 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_size_for_file_small() {
        // < 1 GB -> 8 MB
        assert_eq!(chunk_size_for_file(500 * 1024 * 1024), 8 * MB);
        assert_eq!(chunk_size_for_file(1024), 8 * MB);
    }

    #[test]
    fn test_chunk_size_for_file_medium() {
        // 1 GB <= size < 5 GB -> 64 MB
        assert_eq!(chunk_size_for_file(GB), 64 * MB);
        assert_eq!(chunk_size_for_file(2 * GB), 64 * MB);
    }

    #[test]
    fn test_chunk_size_for_file_large() {
        // >= 5 GB -> 128 MB
        assert_eq!(chunk_size_for_file(5 * GB), 128 * MB);
        assert_eq!(chunk_size_for_file(10 * GB), 128 * MB);
    }
}
