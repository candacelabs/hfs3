"""CLI entrypoint — thin orchestration over composable pieces."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from hfs3 import config, docker, hf, s3


def cmd_mirror(args: argparse.Namespace) -> None:
    """Download from HF and upload to S3."""
    cfg = config.Config.from_env()
    ref = hf.parse_repo_url(args.repo)

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Downloading {ref.repo_id} ({ref.repo_type}) from HuggingFace...")
        local_path = hf.download_repo(ref, Path(tmpdir), token=cfg.hf_token)
        print(f"Downloaded to {local_path}")

        s3_prefix = f"{cfg.s3_prefix}/{ref.repo_type}/{ref.repo_id.replace('/', '--')}"
        print(f"Uploading to s3://{cfg.s3_bucket}/{s3_prefix}/...")
        count = s3.upload_dir(local_path, cfg.s3_bucket, s3_prefix)
        print(f"Uploaded {count} files.")


def cmd_pull(args: argparse.Namespace) -> None:
    """Download from S3 to local directory."""
    cfg = config.Config.from_env()
    ref = hf.parse_repo_url(args.repo)

    dest = Path(args.dest)
    s3_prefix = f"{cfg.s3_prefix}/{ref.repo_type}/{ref.repo_id.replace('/', '--')}"
    print(f"Pulling s3://{cfg.s3_bucket}/{s3_prefix}/ to {dest}...")
    count = s3.download_dir(cfg.s3_bucket, s3_prefix, dest)
    print(f"Downloaded {count} files.")


def cmd_run(args: argparse.Namespace) -> None:
    """Pull from S3, build Docker image, and run it."""
    cfg = config.Config.from_env()
    ref = hf.parse_repo_url(args.repo)

    dest = Path(args.dest)
    s3_prefix = f"{cfg.s3_prefix}/{ref.repo_type}/{ref.repo_id.replace('/', '--')}"

    if not dest.exists() or args.force:
        print(f"Pulling from S3...")
        s3.download_dir(cfg.s3_bucket, s3_prefix, dest)

    tag = f"hfs3/{ref.repo_id}".lower()
    print(f"Building Docker image {tag}...")
    docker.build_image(dest, tag)

    port = args.port
    print(f"Running on port {port}...")
    docker.run_image(tag, port=port)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="hfs3", description="Mirror HuggingFace to S3"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # mirror
    p_mirror = sub.add_parser("mirror", help="Download from HF and upload to S3")
    p_mirror.add_argument("repo", help="HF repo ID or URL")
    p_mirror.set_defaults(func=cmd_mirror)

    # pull
    p_pull = sub.add_parser("pull", help="Download from S3 to local")
    p_pull.add_argument("repo", help="HF repo ID or URL")
    p_pull.add_argument("--dest", default="./repo", help="Local destination directory")
    p_pull.set_defaults(func=cmd_pull)

    # run
    p_run = sub.add_parser("run", help="Pull from S3, build and run Docker image")
    p_run.add_argument("repo", help="HF repo ID or URL")
    p_run.add_argument(
        "--dest", default="./repo", help="Local directory for repo files"
    )
    p_run.add_argument("--port", type=int, default=7860, help="Port to expose")
    p_run.add_argument(
        "--force",
        action="store_true",
        help="Re-download from S3 even if local dir exists",
    )
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args(argv)
    try:
        args.func(args)
    except EnvironmentError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
