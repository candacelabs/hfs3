"""Tests for docker module — no real Docker needed, tests the logic."""

import pytest

from hfs3.docker import build_image


class TestBuildImage:
    def test_missing_dockerfile_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No Dockerfile"):
            build_image(tmp_path, "test-tag")
