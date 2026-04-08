# AGENTS.md

## What this is

HF-to-S3 mirror. Downloads HuggingFace repos/spaces to S3 so they can be deployed on air-gapped servers that have S3 access but no HF access.

## Commands

```
just install          # uv sync --group dev
just test             # run all tests
just test -k test_s3  # run one test file/pattern
just mirror <repo>    # HF -> S3 (needs HFS3_S3_BUCKET)
just pull <repo>      # S3 -> local
just run <repo>       # S3 -> local -> docker build+run
```

## Architecture

Four modules in `src/hfs3/`, each independently testable:

- `hf.py` — parse HF URLs/IDs, download repos via `huggingface_hub.snapshot_download`
- `s3.py` — upload/download directories to/from S3 via `boto3`
- `docker.py` — build and run Docker images from local dirs
- `cli.py` — thin orchestrator wiring the above together
- `config.py` — env var parsing (`HFS3_S3_BUCKET`, `HFS3_S3_PREFIX`, `HF_TOKEN`)

## Env vars

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HFS3_S3_BUCKET` | yes | — | S3 bucket for mirrored repos |
| `HFS3_S3_PREFIX` | no | `hfs3-mirror` | Key prefix within the bucket |
| `HF_TOKEN` | no | — | HuggingFace auth token for gated repos |

## Testing conventions

- S3 tests use `moto` (`@mock_aws`) — no real AWS credentials needed
- HF download is not unit-tested (requires network); `hf.parse_repo_url` is pure and fully tested
- Docker tests only check preconditions (e.g. missing Dockerfile); no real Docker calls in tests
- Every module takes its dependencies as explicit arguments (e.g. `s3_client=`) for testability

## Tooling

- **uv** for package management (not pip directly)
- **just** as task runner — all commands go through the justfile
- **pytest** for tests
- **moto** for S3 mocking
- **src layout**: code in `src/hfs3/`, tests in `tests/`
