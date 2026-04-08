# hfs3

Mirror HuggingFace repos to S3 for air-gapped deployment.

Single Rust binary. Streams files directly from the HuggingFace REST API into S3 multipart uploads — no intermediate files, no disk required. Memory-aware concurrency reads `/proc/meminfo` to limit parallel transfers.

## Install

```bash
just build-release
# binary at target/release/hfs3
```

## Setup

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HFS3_S3_BUCKET` | yes | — | S3 bucket for mirrored repos |
| `HFS3_S3_PREFIX` | no | `hfs3-mirror` | Key prefix within the bucket |
| `HF_TOKEN` | no | — | HuggingFace auth token (for gated repos) |
| `AWS_REGION` | no | — | AWS region for S3 client |

AWS credentials are resolved via the standard SDK chain (env vars, `~/.aws/credentials`, IAM role, etc).

## Usage

### Mirror a repo from HuggingFace to S3

```bash
hfs3 mirror meta-llama/Llama-2-7b
```

Accepts bare repo IDs or full URLs:

```bash
hfs3 mirror https://huggingface.co/meta-llama/Llama-2-7b
hfs3 mirror https://huggingface.co/datasets/user/my-dataset
hfs3 mirror https://huggingface.co/spaces/user/my-space
```

### Pull a mirrored repo from S3 to local disk

```bash
hfs3 pull meta-llama/Llama-2-7b --dest ./my-model
```

### Pull, build, and run as a Docker container

```bash
hfs3 run user/my-space --port 7860
```

### Quick test

```bash
just example
```

Mirrors [`hf-internal-testing/tiny-random-bert`](https://huggingface.co/hf-internal-testing/tiny-random-bert) (~1MB) to your S3 bucket.

## Output

Progress goes to stderr. A JSON summary prints to stdout on completion:

```json
{
  "repo_id": "hf-internal-testing/tiny-random-bert",
  "repo_type": "model",
  "bucket": "my-bucket",
  "prefix": "hfs3-mirror/model/hf-internal-testing--tiny-random-bert",
  "files_transferred": 10,
  "bytes_transferred": 28183891,
  "duration_secs": 8.3
}
```

## S3 key layout

```
s3://{bucket}/{prefix}/{repo_type}/{owner}--{name}/{file_path}
```

Example: `s3://my-bucket/hfs3-mirror/model/meta-llama--Llama-2-7b/config.json`

## Adaptive chunk sizing

| File size | Chunk size |
|-----------|------------|
| < 1 GB | 8 MB |
| < 5 GB | 64 MB |
| >= 5 GB | 128 MB |

Files under 8 MB skip multipart and use a single `PutObject`.

## Development

Requires a devcontainer (Rust toolchain, just, awscli). All `just` recipes run inside the container automatically:

```bash
just dev        # rebuild the devcontainer
just build      # build
just test       # run tests
just clippy     # lint
just fmt        # format
just example    # mirror a tiny test repo
```

## License

MIT
