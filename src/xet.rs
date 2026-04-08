//! Xet-based download support for HuggingFace repos.
//!
//! When a repo uses xet storage (indicated by `xetHash` in the tree API response),
//! files can be downloaded via xet-core's CAS (Content-Addressed Storage) system
//! instead of plain HTTP. This provides chunk-based deduplication and concurrent
//! CAS fetches.
//!
//! # Auth flow
//!
//! 1. Fetch connection info from HF Hub:
//!    `GET /api/{type}s/{repo}/xet-read-token/{revision}`
//! 2. Response headers contain: `X-Xet-Cas-Url`, `X-Xet-Access-Token`, `X-Xet-Token-Expiration`
//! 3. Create `XetDownloadStreamGroup` with endpoint + token
//! 4. Per file: `XetFileInfo::new(hash, size)` → `download_stream()` → `Bytes` chunks

use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::Stream;
use reqwest::Client;

use xet::xet_session::{XetDownloadStreamGroup, XetFileInfo, XetSessionBuilder};

use crate::error::Hfs3Error;
use crate::types::{RepoRef, RepoType};

// HF response headers for xet connection info
const HEADER_XET_CAS_URL: &str = "x-xet-cas-url";
const HEADER_XET_ACCESS_TOKEN: &str = "x-xet-access-token";
const HEADER_XET_TOKEN_EXPIRATION: &str = "x-xet-token-expiration";

/// Re-fetch token if it expires within this many seconds.
const TOKEN_REFRESH_MARGIN_SECS: u64 = 120;

/// Connection info for xet CAS storage, obtained from HF Hub.
#[derive(Debug, Clone)]
pub struct XetConnectionInfo {
    pub endpoint: String,
    pub access_token: String,
    pub expiration: u64,
}

impl XetConnectionInfo {
    /// Returns true if the token is expired or will expire within the safety margin.
    pub fn is_expiring(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.expiration <= now + TOKEN_REFRESH_MARGIN_SECS
    }
}

/// Manages xet download sessions with automatic token refresh.
///
/// Holds the current connection info and download group. When the token
/// is about to expire, `ensure_fresh()` re-fetches from the Hub and
/// rebuilds the download group.
pub struct XetDownloader {
    client: Client,
    repo: RepoRef,
    token: Option<String>,
    conn: XetConnectionInfo,
    group: XetDownloadStreamGroup,
}

impl XetDownloader {
    /// Create a new downloader for the given repo.
    ///
    /// Fetches the initial xet connection info and creates a download group.
    pub async fn new(
        client: &Client,
        repo: &RepoRef,
        token: Option<&str>,
    ) -> Result<Self, Hfs3Error> {
        let conn = fetch_xet_connection_info(client, repo, token).await?;
        let group = create_download_group(&conn).await?;

        Ok(Self {
            client: client.clone(),
            repo: repo.clone(),
            token: token.map(String::from),
            conn,
            group,
        })
    }

    /// Ensure the token is fresh, rebuilding the download group if needed.
    pub async fn ensure_fresh(&mut self) -> Result<(), Hfs3Error> {
        if self.conn.is_expiring() {
            tracing::info!("xet token expiring, refreshing...");
            self.conn = fetch_xet_connection_info(
                &self.client,
                &self.repo,
                self.token.as_deref(),
            )
            .await?;
            self.group = create_download_group(&self.conn).await?;
        }
        Ok(())
    }

    /// Get a cheap clone of the underlying download group for use in spawned tasks.
    pub fn group(&self) -> XetDownloadStreamGroup {
        self.group.clone()
    }

    /// Create a streaming download for a single file, returning a boxed Stream.
    ///
    /// The stream yields `Bytes` chunks in file order, suitable for piping
    /// into our S3 multipart upload pipeline.
    pub async fn download_stream(
        &self,
        xet_hash: &str,
        file_size: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Hfs3Error>> + Send>>, Hfs3Error> {
        let info = XetFileInfo::new(xet_hash.to_string(), file_size);

        let stream = self
            .group
            .download_stream(info, None)
            .await
            .map_err(|e| Hfs3Error::Xet(format!("xet download_stream failed: {e}")))?;

        // Adapt XetDownloadStream (next() -> Result<Option<Bytes>>) into futures::Stream.
        // Errors are terminal: S3 consumer stops on first Err, drop cancels the xet stream.
        let adapted = futures::stream::unfold(stream, |mut s| async move {
            match s.next().await {
                Ok(Some(chunk)) => Some((Ok(chunk), s)),
                Ok(None) => None,
                Err(e) => Some((Err(Hfs3Error::Xet(format!("xet stream error: {e}"))), s)),
            }
        });

        Ok(Box::pin(adapted))
    }
}

/// Fetch xet connection info from the HF Hub token endpoint.
///
/// Calls `GET /api/{type}s/{repo_id}/xet-read-token/{revision}` and parses
/// the `X-Xet-*` response headers.
pub async fn fetch_xet_connection_info(
    client: &Client,
    repo: &RepoRef,
    token: Option<&str>,
) -> Result<XetConnectionInfo, Hfs3Error> {
    fetch_xet_connection_info_with_base(client, "https://huggingface.co", repo, token).await
}

async fn fetch_xet_connection_info_with_base(
    client: &Client,
    base_url: &str,
    repo: &RepoRef,
    token: Option<&str>,
) -> Result<XetConnectionInfo, Hfs3Error> {
    let type_segment = match repo.repo_type {
        RepoType::Model => "models",
        RepoType::Dataset => "datasets",
        RepoType::Space => "spaces",
    };

    let url = format!(
        "{}/api/{}/{}/xet-read-token/{}",
        base_url, type_segment, repo.repo_id, repo.revision
    );

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req.send().await.map_err(|e| {
        Hfs3Error::Xet(format!("failed to fetch xet token from {url}: {e}"))
    })?;

    if !resp.status().is_success() {
        return Err(Hfs3Error::Xet(format!(
            "xet token endpoint returned {}: {url}",
            resp.status()
        )));
    }

    let headers = resp.headers();

    let endpoint = headers
        .get(HEADER_XET_CAS_URL)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Hfs3Error::Xet("missing X-Xet-Cas-Url header".into()))?
        .to_string();

    let access_token = headers
        .get(HEADER_XET_ACCESS_TOKEN)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Hfs3Error::Xet("missing X-Xet-Access-Token header".into()))?
        .to_string();

    let expiration: u64 = headers
        .get(HEADER_XET_TOKEN_EXPIRATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| Hfs3Error::Xet("missing or invalid X-Xet-Token-Expiration header".into()))?;

    Ok(XetConnectionInfo {
        endpoint,
        access_token,
        expiration,
    })
}

/// Create a `XetDownloadStreamGroup` for downloading xet-enabled files.
async fn create_download_group(
    conn: &XetConnectionInfo,
) -> Result<XetDownloadStreamGroup, Hfs3Error> {
    let session = XetSessionBuilder::new()
        .build()
        .map_err(|e| Hfs3Error::Xet(format!("failed to create xet session: {e}")))?;

    let group = session
        .new_download_stream_group()
        .map_err(|e| Hfs3Error::Xet(format!("failed to create download group builder: {e}")))?
        .with_endpoint(&conn.endpoint)
        .with_token_info(&conn.access_token, conn.expiration)
        .build()
        .await
        .map_err(|e| Hfs3Error::Xet(format!("failed to build download group: {e}")))?;

    Ok(group)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_repo() -> RepoRef {
        RepoRef {
            repo_id: "org/my-model".to_string(),
            repo_type: RepoType::Model,
            revision: "main".to_string(),
        }
    }

    #[tokio::test]
    async fn test_fetch_connection_info_success() {
        let server = MockServer::start().await;

        let response = ResponseTemplate::new(200)
            .append_header("X-Xet-Cas-Url", "https://cas.example.com")
            .append_header("X-Xet-Access-Token", "test-token-abc")
            .append_header("X-Xet-Token-Expiration", "1700000000");

        Mock::given(method("GET"))
            .and(path_regex(r"/api/models/.+/xet-read-token/.+"))
            .respond_with(response)
            .mount(&server)
            .await;

        let client = Client::new();
        let info = fetch_xet_connection_info_with_base(
            &client,
            &server.uri(),
            &test_repo(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(info.endpoint, "https://cas.example.com");
        assert_eq!(info.access_token, "test-token-abc");
        assert_eq!(info.expiration, 1700000000);
    }

    #[tokio::test]
    async fn test_fetch_connection_info_with_auth() {
        let server = MockServer::start().await;

        let response = ResponseTemplate::new(200)
            .append_header("X-Xet-Cas-Url", "https://cas.example.com")
            .append_header("X-Xet-Access-Token", "gated-token")
            .append_header("X-Xet-Token-Expiration", "9999999999");

        Mock::given(method("GET"))
            .and(path_regex(r"/api/models/.+/xet-read-token/.+"))
            .and(wiremock::matchers::header("Authorization", "Bearer my-hf-token"))
            .respond_with(response)
            .mount(&server)
            .await;

        // Without token → 404
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = Client::new();

        // With token → success
        let info = fetch_xet_connection_info_with_base(
            &client,
            &server.uri(),
            &test_repo(),
            Some("my-hf-token"),
        )
        .await
        .unwrap();
        assert_eq!(info.access_token, "gated-token");

        // Without token → error
        let err = fetch_xet_connection_info_with_base(
            &client,
            &server.uri(),
            &test_repo(),
            None,
        )
        .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_fetch_connection_info_missing_headers() {
        let server = MockServer::start().await;

        // Response is 200 but missing xet headers
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client = Client::new();
        let err = fetch_xet_connection_info_with_base(
            &client,
            &server.uri(),
            &test_repo(),
            None,
        )
        .await;

        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("X-Xet-Cas-Url"));
    }

    #[tokio::test]
    async fn test_fetch_connection_info_repo_types() {
        let server = MockServer::start().await;

        let response = ResponseTemplate::new(200)
            .append_header("X-Xet-Cas-Url", "https://cas.example.com")
            .append_header("X-Xet-Access-Token", "tok")
            .append_header("X-Xet-Token-Expiration", "1000");

        // Mount for spaces
        Mock::given(method("GET"))
            .and(path_regex(r"/api/spaces/.+/xet-read-token/.+"))
            .respond_with(response)
            .mount(&server)
            .await;

        let client = Client::new();
        let space_repo = RepoRef {
            repo_id: "user/my-space".to_string(),
            repo_type: RepoType::Space,
            revision: "main".to_string(),
        };

        let info = fetch_xet_connection_info_with_base(
            &client,
            &server.uri(),
            &space_repo,
            None,
        )
        .await
        .unwrap();
        assert_eq!(info.endpoint, "https://cas.example.com");
    }

    #[test]
    fn test_connection_info_expiry_check() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Token expiring in 10 seconds → should be flagged as expiring
        let expiring = XetConnectionInfo {
            endpoint: "https://cas.example.com".into(),
            access_token: "tok".into(),
            expiration: now + 10,
        };
        assert!(expiring.is_expiring());

        // Token valid for 10 minutes → not expiring
        let fresh = XetConnectionInfo {
            endpoint: "https://cas.example.com".into(),
            access_token: "tok".into(),
            expiration: now + 600,
        };
        assert!(!fresh.is_expiring());
    }
}
