#!/usr/bin/env bash
set -euo pipefail

cargo check --workspace 2>&1 | tee /tmp/ivm-check.log
cargo test -p ivm-core -p ivm-operators -p ivm-planner \
  -p ivm-kafka-cdc -p ivm-parquet -p ivm-runtime 2>&1 | tee /tmp/ivm-test.log
cargo test -p ivm-integration-tests 2>&1 | tee /tmp/ivm-integration.log
cargo check -p ivm-api 2>&1 | tee /tmp/ivm-api.log
cargo bench -p ivm-operators 2>&1 | tee /tmp/ivm-bench.log | grep -E "time:" || true
