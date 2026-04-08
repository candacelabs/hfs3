# hfs3 — Mirror HuggingFace to S3

default:
    @just --list

# Ensure devcontainer is running, then exec a command with host AWS creds
[private]
dc +cmd:
    #!/usr/bin/env bash
    set -euo pipefail
    devcontainer up --workspace-folder . > /dev/null 2>&1
    # Resolve host AWS credentials and forward into the container
    eval "$(aws configure export-credentials --format env 2>/dev/null || true)"
    remote_env=()
    [ -n "${AWS_ACCESS_KEY_ID:-}" ] && remote_env+=(--remote-env "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID")
    [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && remote_env+=(--remote-env "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY")
    [ -n "${AWS_SESSION_TOKEN:-}" ] && remote_env+=(--remote-env "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN")
    [ -n "${AWS_REGION:-}" ] && remote_env+=(--remote-env "AWS_REGION=$AWS_REGION")
    devcontainer exec --workspace-folder . "${remote_env[@]}" {{cmd}}

# Spin up the devcontainer (force rebuild)
dev:
    devcontainer up --workspace-folder . --remove-existing-container

# Run an arbitrary command inside the devcontainer
exec +cmd:
    just dc {{cmd}}

build:
    just dc cargo build

build-release:
    just dc cargo build --release

test *args='':
    just dc cargo test {{args}}

test-v *args='':
    just dc cargo test {{args}} -- --nocapture

check:
    just dc cargo check

clippy:
    just dc cargo clippy -- -D warnings

fmt:
    just dc cargo fmt

fmt-check:
    just dc cargo fmt -- --check

lint: fmt-check clippy

clean:
    just dc cargo clean

# Main workflows
# Mirror a tiny test repo (~1MB) to verify setup
example:
    just mirror hf-internal-testing/tiny-random-bert

mirror repo:
    just dc cargo run -- mirror "{{repo}}"

pull repo dest='./repo':
    just dc cargo run -- pull "{{repo}}" --dest "{{dest}}"

run repo dest='./repo' port='7860':
    just dc cargo run -- run "{{repo}}" --dest "{{dest}}" --port "{{port}}"
