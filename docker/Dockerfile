# Multi-stage build for IVM control plane API
FROM rust:1.77-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.toml
COPY crates ./crates
COPY examples ./examples

RUN cargo build --release -p ivm-api --features kafka

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/ivm-api /usr/local/bin/ivm-api

ENV CHECKPOINT_DIR=/data/checkpoints
RUN mkdir -p /data/checkpoints

EXPOSE 8080
CMD ["ivm-api"]
