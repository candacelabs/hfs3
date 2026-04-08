"""Download HuggingFace repos to a local directory."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from huggingface_hub import snapshot_download


@dataclass(frozen=True)
class RepoRef:
    """Parsed HuggingFace repo reference."""

    repo_id: str  # e.g. "meta-llama/Llama-2-7b" or "user/my-space"
    repo_type: str  # "model", "dataset", or "space"
    revision: str  # git revision, default "main"


def parse_repo_url(url_or_id: str) -> RepoRef:
    """Parse a HF URL or repo ID into a RepoRef.

    Accepts:
        - "meta-llama/Llama-2-7b"                    -> model
        - "https://huggingface.co/meta-llama/Llama-2-7b"
        - "https://huggingface.co/spaces/user/my-space"
        - "https://huggingface.co/datasets/user/my-ds"
    """
    url_or_id = url_or_id.strip().rstrip("/")

    # Strip HF URL prefix
    for prefix in ("https://huggingface.co/", "http://huggingface.co/"):
        if url_or_id.startswith(prefix):
            url_or_id = url_or_id[len(prefix) :]
            break

    # Detect repo type from path prefix
    repo_type = "model"
    for rt in ("spaces", "datasets"):
        if url_or_id.startswith(rt + "/"):
            repo_type = rt.rstrip("s")  # "spaces" -> "space", "datasets" -> "dataset"
            url_or_id = url_or_id[len(rt) + 1 :]
            break

    # Extract revision if present (tree/branch syntax)
    parts = url_or_id.split("/tree/", 1)
    repo_id = parts[0]
    revision = parts[1] if len(parts) > 1 else "main"

    if "/" not in repo_id:
        raise ValueError(f"Invalid repo ID (need 'owner/name'): {repo_id}")

    return RepoRef(repo_id=repo_id, repo_type=repo_type, revision=revision)


def download_repo(ref: RepoRef, local_dir: Path, token: str | None = None) -> Path:
    """Download a HF repo snapshot to local_dir.

    Returns the path to the downloaded snapshot directory.
    """
    local_dir.mkdir(parents=True, exist_ok=True)
    dest = local_dir / ref.repo_type / ref.repo_id.replace("/", "--")
    return Path(
        snapshot_download(
            repo_id=ref.repo_id,
            repo_type=ref.repo_type,
            revision=ref.revision,
            local_dir=str(dest),
            token=token,
        )
    )
