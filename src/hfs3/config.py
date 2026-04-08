"""Configuration — all from env vars, no hidden magic."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Runtime configuration. All fields sourced from environment."""

    s3_bucket: str
    s3_prefix: str  # e.g. "hfs3-mirror" — objects go under s3://bucket/prefix/repo_type/owner--repo/
    hf_token: str | None

    @classmethod
    def from_env(cls) -> "Config":
        bucket = os.environ.get("HFS3_S3_BUCKET")
        if not bucket:
            raise EnvironmentError("HFS3_S3_BUCKET is required")
        return cls(
            s3_bucket=bucket,
            s3_prefix=os.environ.get("HFS3_S3_PREFIX", "hfs3-mirror"),
            hf_token=os.environ.get("HF_TOKEN"),
        )
