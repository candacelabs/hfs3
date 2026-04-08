"""Sync local directories to/from S3 using boto3."""

from __future__ import annotations

from pathlib import Path

import boto3


def upload_dir(
    local_dir: Path,
    bucket: str,
    prefix: str,
    s3_client: boto3.client | None = None,
) -> int:
    """Upload all files in local_dir to s3://bucket/prefix/.

    Returns the number of files uploaded.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    count = 0
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        key = f"{prefix}/{path.relative_to(local_dir)}"
        s3_client.upload_file(str(path), bucket, key)
        count += 1
    return count


def download_dir(
    bucket: str,
    prefix: str,
    local_dir: Path,
    s3_client: boto3.client | None = None,
) -> int:
    """Download all objects under s3://bucket/prefix/ to local_dir.

    Returns the number of files downloaded.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    # Ensure prefix ends with / for clean key stripping
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    paginator = s3_client.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix) :]
            if not rel:
                continue
            dest = local_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3_client.download_file(bucket, key, str(dest))
            count += 1
    return count
