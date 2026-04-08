use thiserror::Error;

#[derive(Error, Debug)]
pub enum Hfs3Error {
    #[error("config error: {0}")]
    Config(String),

    #[error("HuggingFace API error: {0}")]
    HfApi(String),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Docker error: {0}")]
    Docker(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("xet error: {0}")]
    Xet(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_config_error_display() {
        let err = Hfs3Error::Config("missing bucket".into());
        assert_eq!(err.to_string(), "config error: missing bucket");
    }

    #[test]
    fn test_hf_api_error_display() {
        let err = Hfs3Error::HfApi("401 unauthorized".into());
        assert_eq!(err.to_string(), "HuggingFace API error: 401 unauthorized");
    }

    #[test]
    fn test_s3_error_display() {
        let err = Hfs3Error::S3("bucket not found".into());
        assert_eq!(err.to_string(), "S3 error: bucket not found");
    }

    #[test]
    fn test_io_error_from() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file missing");
        let err: Hfs3Error = io_err.into();
        assert!(err.to_string().contains("I/O error"));
    }

    #[test]
    fn test_docker_error_display() {
        let err = Hfs3Error::Docker("daemon not running".into());
        assert_eq!(err.to_string(), "Docker error: daemon not running");
    }

    #[test]
    fn test_parse_error_display() {
        let err = Hfs3Error::Parse("invalid URL".into());
        assert_eq!(err.to_string(), "parse error: invalid URL");
    }
}
