# Scope Decisions

All decisions were provided upfront by the user. No Q&A was needed.

## Why Rust instead of staying Python

- Single binary distribution, no runtime dependencies
- Zero-copy streaming for large model files (reqwest -> S3 multipart)
- Memory-aware concurrency via /proc/meminfo
- The user wants hardware-respectful tooling (see copilot-instructions.md)

## Why HF REST API instead of huggingface_hub crate

- No mature Rust HF client crate exists
- The REST API is simple: one endpoint for file listing, one for download
- Direct reqwest streaming gives us byte-level control for S3 piping

## Why adaptive chunk sizing

- HuggingFace model files range from KB (config.json) to 10+ GB (safetensors)
- S3 multipart upload has a 10,000 part limit
- 8MB chunks for small files = fast completion, low memory
- 128MB chunks for huge files = stays under part limit (5GB / 128MB = ~40 parts)

## Why memory-aware concurrency

- Downloading multiple large files simultaneously can exhaust RAM
- Each concurrent transfer buffers ~2 chunks (download buffer + upload buffer)
- /proc/meminfo gives actual available memory, not total
- Floor at 2 ensures progress even on tiny instances

## Why no mock S3 in Rust tests

- Python used moto (mature S3 mock). No equivalent in Rust.
- Pure logic (chunk sizing, concurrency math, URL construction) is unit tested.
- S3 multipart upload is tested by actually running against real S3 (or localstack).
- Integration tests cover the HF API path end-to-end.

## What was kept from Python

- tests/ directory stays as behavioral spec reference
- The parse_repo_url logic is ported 1:1 with identical test cases
- The S3 key layout (prefix/repo_type/owner--name/path) is preserved
- The env var names and defaults are identical
- The three subcommands (mirror, pull, run) with the same CLI args
