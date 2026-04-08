use bytes::Bytes;
use futures::Stream;
use reqwest::Client;

use crate::error::Hfs3Error;
use crate::types::{HfFileEntry, RepoRef, RepoType};

const HF_BASE: &str = "https://huggingface.co";

/// Build the HF tree API URL for listing repo files.
fn api_url(repo: &RepoRef) -> String {
    let type_segment = match repo.repo_type {
        RepoType::Model => "models",
        RepoType::Dataset => "datasets",
        RepoType::Space => "spaces",
    };
    format!(
        "{}/api/{}/{}/tree/{}?recursive=true",
        HF_BASE, type_segment, repo.repo_id, repo.revision
    )
}

/// Build the HF resolve URL for downloading a file.
fn download_url(repo: &RepoRef, file_path: &str) -> String {
    let type_prefix = match repo.repo_type {
        RepoType::Model => String::new(),
        RepoType::Dataset => "datasets/".to_string(),
        RepoType::Space => "spaces/".to_string(),
    };
    format!(
        "{}/{}{}/resolve/{}/{}",
        HF_BASE, type_prefix, repo.repo_id, repo.revision, file_path
    )
}

/// JSON shape returned by the HF tree API (superset of what we need).
#[derive(serde::Deserialize)]
struct HfTreeEntry {
    #[serde(rename = "type")]
    entry_type: String,
    path: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    oid: String,
}

/// List files in a HuggingFace repo via the tree API.
///
/// Calls `GET /api/{models|datasets|spaces}/{repo_id}/tree/{revision}?recursive=true`,
/// filters to entries with `type == "file"`, and maps to [`HfFileEntry`].
pub async fn list_repo_files(
    client: &Client,
    repo: &RepoRef,
    token: Option<&str>,
) -> Result<Vec<HfFileEntry>, Hfs3Error> {
    let url = api_url(repo);

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req.send().await?;

    if !resp.status().is_success() {
        return Err(Hfs3Error::HfApi(format!(
            "GET {} returned {}",
            url,
            resp.status()
        )));
    }

    let entries: Vec<HfTreeEntry> = resp.json().await?;

    let files = entries
        .into_iter()
        .filter(|e| e.entry_type == "file")
        .map(|e| HfFileEntry {
            path: e.path,
            size: e.size,
            oid: e.oid,
        })
        .collect();

    Ok(files)
}

/// Download a single file from HuggingFace as a byte stream.
///
/// Returns `(stream, content_length)`. The stream yields `Bytes` chunks
/// suitable for piping into S3 multipart upload.
pub async fn download_file_stream(
    client: &Client,
    repo: &RepoRef,
    file_path: &str,
    token: Option<&str>,
) -> Result<(impl Stream<Item = Result<Bytes, reqwest::Error>>, Option<u64>), Hfs3Error> {
    let url = download_url(repo, file_path);

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req.send().await?;

    if !resp.status().is_success() {
        return Err(Hfs3Error::HfApi(format!(
            "GET {} returned {}",
            url,
            resp.status()
        )));
    }

    let content_length = resp.content_length();
    let stream = resp.bytes_stream();

    Ok((stream, content_length))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model_repo() -> RepoRef {
        RepoRef {
            repo_id: "meta-llama/Llama-2-7b".to_string(),
            repo_type: RepoType::Model,
            revision: "main".to_string(),
        }
    }

    fn dataset_repo() -> RepoRef {
        RepoRef {
            repo_id: "user/my-dataset".to_string(),
            repo_type: RepoType::Dataset,
            revision: "main".to_string(),
        }
    }

    fn space_repo() -> RepoRef {
        RepoRef {
            repo_id: "user/my-space".to_string(),
            repo_type: RepoType::Space,
            revision: "v2".to_string(),
        }
    }

    #[test]
    fn test_api_url_model() {
        let repo = model_repo();
        assert_eq!(
            api_url(&repo),
            "https://huggingface.co/api/models/meta-llama/Llama-2-7b/tree/main?recursive=true"
        );
    }

    #[test]
    fn test_api_url_dataset() {
        let repo = dataset_repo();
        assert_eq!(
            api_url(&repo),
            "https://huggingface.co/api/datasets/user/my-dataset/tree/main?recursive=true"
        );
    }

    #[test]
    fn test_download_url_model() {
        let repo = model_repo();
        assert_eq!(
            download_url(&repo, "config.json"),
            "https://huggingface.co/meta-llama/Llama-2-7b/resolve/main/config.json"
        );
    }

    #[test]
    fn test_download_url_space() {
        let repo = space_repo();
        assert_eq!(
            download_url(&repo, "app.py"),
            "https://huggingface.co/spaces/user/my-space/resolve/v2/app.py"
        );
    }
}
