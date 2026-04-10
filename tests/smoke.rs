//! Smoke tests for the hfs3 binary.
//!
//! These exercise the compiled CLI as a subprocess to verify basic
//! behavior: help output, argument validation, error messages, and
//! exit codes. No network or AWS credentials required.

use std::process::Command;

/// Path to the compiled binary (cargo puts it in target/debug/).
fn hfs3() -> Command {
    Command::new(env!("CARGO_BIN_EXE_hfs3"))
}

// ── Help & version ──────────────────────────────────────────────

#[test]
fn help_exits_zero() {
    let out = hfs3().arg("--help").output().unwrap();
    assert!(out.status.success(), "hfs3 --help should exit 0");
}

#[test]
fn help_mentions_subcommands() {
    let out = hfs3().arg("--help").output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("mirror"), "help should mention mirror");
    assert!(stdout.contains("pull"), "help should mention pull");
    assert!(stdout.contains("run"), "help should mention run");
}

#[test]
fn mirror_help_exits_zero() {
    let out = hfs3().args(["mirror", "--help"]).output().unwrap();
    assert!(out.status.success(), "hfs3 mirror --help should exit 0");
}

#[test]
fn pull_help_exits_zero() {
    let out = hfs3().args(["pull", "--help"]).output().unwrap();
    assert!(out.status.success(), "hfs3 pull --help should exit 0");
}

#[test]
fn run_help_exits_zero() {
    let out = hfs3().args(["run", "--help"]).output().unwrap();
    assert!(out.status.success(), "hfs3 run --help should exit 0");
}

// ── Missing arguments ───────────────────────────────────────────

#[test]
fn no_subcommand_exits_nonzero() {
    let out = hfs3().output().unwrap();
    assert!(!out.status.success(), "hfs3 with no args should fail");
}

#[test]
fn mirror_no_repo_exits_nonzero() {
    let out = hfs3().arg("mirror").output().unwrap();
    assert!(
        !out.status.success(),
        "hfs3 mirror with no repo should fail"
    );
}

#[test]
fn pull_no_repo_exits_nonzero() {
    let out = hfs3().arg("pull").output().unwrap();
    assert!(!out.status.success(), "hfs3 pull with no repo should fail");
}

#[test]
fn run_no_repo_exits_nonzero() {
    let out = hfs3().arg("run").output().unwrap();
    assert!(!out.status.success(), "hfs3 run with no repo should fail");
}

#[test]
fn unknown_subcommand_exits_nonzero() {
    let out = hfs3().arg("yolo").output().unwrap();
    assert!(!out.status.success(), "unknown subcommand should fail");
}

// ── Repo ID validation ──────────────────────────────────────────

#[test]
fn mirror_invalid_repo_no_slash() {
    let out = hfs3()
        .arg("mirror")
        .arg("just-a-name")
        .env("HFS3_S3_BUCKET", "dummy")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("owner/name"),
        "should mention expected format, got: {stderr}"
    );
}

// ── Config validation ───────────────────────────────────────────

#[test]
fn mirror_missing_bucket_env() {
    // Run from /tmp to avoid .env being loaded by dotenvy
    let out = hfs3()
        .arg("mirror")
        .arg("owner/repo")
        .current_dir("/tmp")
        .env_remove("HFS3_S3_BUCKET")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("HFS3_S3_BUCKET"),
        "should mention missing env var, got: {stderr}"
    );
}

#[test]
fn pull_missing_bucket_env() {
    let out = hfs3()
        .arg("pull")
        .arg("owner/repo")
        .current_dir("/tmp")
        .env_remove("HFS3_S3_BUCKET")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("HFS3_S3_BUCKET"),
        "should mention missing env var, got: {stderr}"
    );
}
