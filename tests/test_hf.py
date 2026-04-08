"""Tests for hf.parse_repo_url — pure parsing, no network."""

import pytest

from hfs3.hf import RepoRef, parse_repo_url


class TestParseRepoUrl:
    def test_bare_model_id(self):
        ref = parse_repo_url("meta-llama/Llama-2-7b")
        assert ref == RepoRef(
            repo_id="meta-llama/Llama-2-7b", repo_type="model", revision="main"
        )

    def test_model_url(self):
        ref = parse_repo_url("https://huggingface.co/meta-llama/Llama-2-7b")
        assert ref == RepoRef(
            repo_id="meta-llama/Llama-2-7b", repo_type="model", revision="main"
        )

    def test_space_url(self):
        ref = parse_repo_url("https://huggingface.co/spaces/user/my-space")
        assert ref == RepoRef(
            repo_id="user/my-space", repo_type="space", revision="main"
        )

    def test_dataset_url(self):
        ref = parse_repo_url("https://huggingface.co/datasets/user/my-ds")
        assert ref == RepoRef(
            repo_id="user/my-ds", repo_type="dataset", revision="main"
        )

    def test_url_with_revision(self):
        ref = parse_repo_url("https://huggingface.co/meta-llama/Llama-2-7b/tree/dev")
        assert ref == RepoRef(
            repo_id="meta-llama/Llama-2-7b", repo_type="model", revision="dev"
        )

    def test_trailing_slash_stripped(self):
        ref = parse_repo_url("https://huggingface.co/spaces/user/my-space/")
        assert ref.repo_id == "user/my-space"

    def test_whitespace_stripped(self):
        ref = parse_repo_url("  meta-llama/Llama-2-7b  ")
        assert ref.repo_id == "meta-llama/Llama-2-7b"

    def test_invalid_no_slash(self):
        with pytest.raises(ValueError, match="Invalid repo ID"):
            parse_repo_url("just-a-name")

    def test_invalid_bare_namespace(self):
        with pytest.raises(ValueError, match="Invalid repo ID"):
            parse_repo_url("https://huggingface.co/spaces/onlynamespace")
