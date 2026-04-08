FROM rust:1-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    awscli \
    pkg-config \
    libssl-dev \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install just
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

RUN groupadd --gid 1000 vscode \
    && useradd --uid 1000 --gid 1000 -m vscode

WORKDIR /workspaces/hfs3
