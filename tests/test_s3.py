"""Tests for s3 upload/download — uses moto to mock S3, no real AWS calls."""

import boto3
import pytest
from moto import mock_aws

from hfs3.s3 import download_dir, upload_dir


@pytest.fixture
def s3_bucket():
    """Create a mock S3 bucket and yield (client, bucket_name)."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        client.create_bucket(Bucket=bucket)
        yield client, bucket


class TestUploadDir:
    def test_uploads_all_files(self, tmp_path, s3_bucket):
        client, bucket = s3_bucket
        # Create test files
        (tmp_path / "a.txt").write_text("hello")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "b.txt").write_text("world")

        count = upload_dir(tmp_path, bucket, "prefix", s3_client=client)

        assert count == 2
        # Verify files exist in S3
        resp = client.list_objects_v2(Bucket=bucket, Prefix="prefix/")
        keys = sorted(o["Key"] for o in resp["Contents"])
        assert keys == ["prefix/a.txt", "prefix/sub/b.txt"]

    def test_empty_dir_uploads_nothing(self, tmp_path, s3_bucket):
        client, bucket = s3_bucket
        count = upload_dir(tmp_path, bucket, "prefix", s3_client=client)
        assert count == 0

    def test_skips_directories(self, tmp_path, s3_bucket):
        client, bucket = s3_bucket
        (tmp_path / "subdir").mkdir()
        (tmp_path / "file.txt").write_text("content")
        count = upload_dir(tmp_path, bucket, "prefix", s3_client=client)
        assert count == 1


class TestDownloadDir:
    def test_downloads_all_files(self, tmp_path, s3_bucket):
        client, bucket = s3_bucket
        # Upload test files to S3
        client.put_object(Bucket=bucket, Key="prefix/a.txt", Body=b"hello")
        client.put_object(Bucket=bucket, Key="prefix/sub/b.txt", Body=b"world")

        dest = tmp_path / "out"
        count = download_dir(bucket, "prefix", dest, s3_client=client)

        assert count == 2
        assert (dest / "a.txt").read_text() == "hello"
        assert (dest / "sub" / "b.txt").read_text() == "world"

    def test_empty_prefix_downloads_nothing(self, tmp_path, s3_bucket):
        client, bucket = s3_bucket
        dest = tmp_path / "out"
        count = download_dir(bucket, "nonexistent", dest, s3_client=client)
        assert count == 0


class TestRoundTrip:
    def test_upload_then_download_preserves_content(self, tmp_path, s3_bucket):
        client, bucket = s3_bucket

        # Create source
        src = tmp_path / "src"
        src.mkdir()
        (src / "model.bin").write_bytes(b"\x00\x01\x02\x03")
        (src / "config.json").write_text('{"key": "value"}')

        # Upload
        upload_dir(src, bucket, "mirror/model", s3_client=client)

        # Download to different dir
        dest = tmp_path / "dest"
        download_dir(bucket, "mirror/model", dest, s3_client=client)

        # Compare
        assert (dest / "model.bin").read_bytes() == b"\x00\x01\x02\x03"
        assert (dest / "config.json").read_text() == '{"key": "value"}'
