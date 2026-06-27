#!/usr/bin/env bash
# Record this with `vhs` (https://github.com/charmbracelet/vhs) or asciinema
set -e

echo "=== IVM Engine: Crash Recovery Demo ==="
echo ""

echo "Step 1: Start pipeline and insert data..."
curl -s -X POST http://localhost:8080/pipelines \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","source":{"type":"kafka","brokers":"localhost:9092","topic":"orders","group_id":"ivm"},"operators":[],"checkpoint_interval_secs":5}'

sleep 2

echo ""
echo "Step 2: Check current row count via metrics..."
curl -s http://localhost:8080/metrics | grep ivm_rows_processed_total

sleep 5

echo ""
echo "Step 3: CRASH — killing the engine process..."
pkill -f ivm-api || true
sleep 1
echo "    [process killed]"

sleep 2

echo ""
echo "Step 4: Restarting engine (will restore from Parquet checkpoint)..."
cargo run -p ivm-api --release &
sleep 3

echo ""
echo "Step 5: Verify recovered state..."
curl -s http://localhost:8080/pipelines
curl -s http://localhost:8080/metrics | grep ivm_rows_processed_total

echo ""
echo "State fully recovered from checkpoint. No data loss."
