use std::path::Path;
use std::process::Command;

use crate::error::Hfs3Error;

/// Build a Docker image from a directory containing a Dockerfile.
///
/// Equivalent to: docker build -t {tag} .
/// Runs in the context_dir directory.
pub async fn build_image(context_dir: &Path, tag: &str) -> Result<(), Hfs3Error> {
    // Check Dockerfile exists first
    if !context_dir.join("Dockerfile").exists() {
        return Err(Hfs3Error::Docker(format!(
            "No Dockerfile in {}",
            context_dir.display()
        )));
    }

    let dir = context_dir.to_path_buf();
    let tag = tag.to_string();

    eprintln!("Running: docker build -t {} .", tag);

    // Use spawn_blocking since Command::output() is blocking
    let output = tokio::task::spawn_blocking(move || {
        Command::new("docker")
            .args(["build", "-t", &tag, "."])
            .current_dir(&dir)
            .output()
    })
    .await
    .map_err(|e| Hfs3Error::Docker(format!("spawn error: {e}")))?
    .map_err(|e| Hfs3Error::Docker(format!("docker build failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Hfs3Error::Docker(format!("docker build failed: {stderr}")));
    }

    Ok(())
}

/// Run a Docker image, mapping a port to localhost.
///
/// Equivalent to: docker run --rm -p {port}:{port} [extra_args...] {tag}
pub async fn run_image(
    tag: &str,
    port: u16,
    extra_args: Option<&[&str]>,
) -> Result<(), Hfs3Error> {
    let mut cmd_args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "-p".to_string(),
        format!("{port}:{port}"),
    ];
    if let Some(extras) = extra_args {
        cmd_args.extend(extras.iter().map(|s| s.to_string()));
    }
    cmd_args.push(tag.to_string());

    eprintln!("Running: docker {}", cmd_args.join(" "));

    let output = tokio::task::spawn_blocking(move || {
        Command::new("docker").args(&cmd_args).status()
    })
    .await
    .map_err(|e| Hfs3Error::Docker(format!("spawn error: {e}")))?
    .map_err(|e| Hfs3Error::Docker(format!("docker run failed: {e}")))?;

    if !output.success() {
        return Err(Hfs3Error::Docker(
            "docker run exited with non-zero status".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Helper: create a unique temporary directory under std::env::temp_dir().
    /// Returns the path. Caller is responsible for cleanup.
    /// NOTE: Uses std::env::temp_dir() instead of tempfile crate because
    /// Cargo.toml is owned by A0 and does not include tempfile in dev-dependencies.
    fn make_temp_dir(suffix: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("hfs3_test_docker_{}", suffix));
        // Clean up any leftover from a prior run
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    #[tokio::test]
    async fn test_missing_dockerfile_raises() {
        let dir = make_temp_dir("missing_dockerfile");
        let result = build_image(dir.as_path(), "test-tag").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("No Dockerfile"),
            "Error should mention missing Dockerfile: {err_msg}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_missing_dockerfile_includes_path() {
        let dir = make_temp_dir("missing_dockerfile_path");
        let result = build_image(dir.as_path(), "test-tag").await;
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains(dir.to_str().unwrap()),
            "Error should contain the directory path: {err_msg}"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
