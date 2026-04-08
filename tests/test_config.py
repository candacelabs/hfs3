"""Tests for config — pure env var parsing."""

import os

import pytest

from hfs3.config import Config


class TestConfig:
    def test_from_env_minimal(self, monkeypatch):
        monkeypatch.setenv("HFS3_S3_BUCKET", "my-bucket")
        monkeypatch.delenv("HFS3_S3_PREFIX", raising=False)
        monkeypatch.delenv("HF_TOKEN", raising=False)

        cfg = Config.from_env()
        assert cfg.s3_bucket == "my-bucket"
        assert cfg.s3_prefix == "hfs3-mirror"
        assert cfg.hf_token is None

    def test_from_env_full(self, monkeypatch):
        monkeypatch.setenv("HFS3_S3_BUCKET", "prod-bucket")
        monkeypatch.setenv("HFS3_S3_PREFIX", "custom/prefix")
        monkeypatch.setenv("HF_TOKEN", "hf_abc123")

        cfg = Config.from_env()
        assert cfg.s3_bucket == "prod-bucket"
        assert cfg.s3_prefix == "custom/prefix"
        assert cfg.hf_token == "hf_abc123"

    def test_missing_bucket_raises(self, monkeypatch):
        monkeypatch.delenv("HFS3_S3_BUCKET", raising=False)
        with pytest.raises(EnvironmentError, match="HFS3_S3_BUCKET"):
            Config.from_env()
