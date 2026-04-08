"""Build and run Docker images from local repo snapshots."""

from __future__ import annotations

import subprocess
from pathlib import Path


def build_image(context_dir: Path, tag: str) -> str:
    """Build a Docker image from context_dir. Returns the image tag."""
    if not (context_dir / "Dockerfile").exists():
        raise FileNotFoundError(f"No Dockerfile in {context_dir}")
    subprocess.run(
        ["docker", "build", "-t", tag, "."],
        cwd=context_dir,
        check=True,
    )
    return tag


def run_image(tag: str, port: int = 7860, extra_args: list[str] | None = None) -> None:
    """Run a Docker image, mapping port to localhost."""
    cmd = ["docker", "run", "--rm", "-p", f"{port}:{port}", *(extra_args or []), tag]
    subprocess.run(cmd, check=True)
