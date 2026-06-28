Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

cargo check --workspace
cargo test -p ivm-core -p ivm-operators -p ivm-planner -p ivm-kafka-cdc -p ivm-parquet -p ivm-runtime
cargo test -p ivm-integration-tests
cargo check -p ivm-api
cargo bench -p ivm-operators 2>&1 | Select-String 'time:'
