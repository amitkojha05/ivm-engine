# ivm-engine

**DBSP-inspired incremental computation engine in Rust** that ingests CDC streams from Postgres WAL and Kafka, maintains SQL-like views (joins, aggregations, filters) incrementally over change batches, and checkpoints state to Parquet. Avro-encoded wire format, exactly-once delivery, and fault recovery via WAL replay.

## What this demonstrates

| Feldera concern | This project |
|---|---|
| Rust proficiency | Entire engine written in Rust |
| DBSP / incremental computation | Z-set delta model implemented from scratch |
| SQL → operator graph | `ivm-planner` compiles SQL to logical plans via sqlparser |
| Connectors: Kafka + Postgres WAL | Real CDC source connectors (Debezium + pgoutput) |
| Formats: Avro + Parquet | Wire format (Avro) + checkpoint format (Parquet/Snappy) |
| Exactly-once + fault recovery | Offset checkpointing + Parquet state snapshots |
| Observability | Prometheus metrics + Grafana dashboard |
| Kubernetes control plane | Docker Compose locally; K8s CRD manifests included |

## Architecture

```
SQL Query
    ↓
┌─────────────────┐
│   SQL Planner   │  (sqlparser → LogicalPlan → Operator Graph)
└────────┬────────┘
         ↓
┌─────────────────────────────────────────┐
│           Incremental Operators          │
│  Filter → Join (Z-set) → Aggregate      │
│  Δ(A ⋈ B) = ΔA⋈B + A⋈ΔB + ΔA⋈ΔB      │
└────────┬────────────────────┬───────────┘
         │                    │
  ┌──────┴──────┐      ┌──────┴──────┐
  │  Kafka CDC  │      │ Postgres WAL│
  │  (Debezium) │      │ (pgoutput)  │
  └─────────────┘      └─────────────┘
         ↓
┌─────────────────┐
│ Parquet Checkpt │  epoch-keyed, Snappy compressed
└─────────────────┘
         ↓
┌─────────────────┐
│  Axum REST API  │  /pipelines  /metrics
└─────────────────┘
         ↓
  Prometheus + Grafana
```

## Repository layout

```
ivm-engine/
├── crates/
│   ├── core/              # Z-set types, Batch, Row
│   ├── operators/         # Filter, Map, Join, Aggregate, Union
│   ├── planner/           # SQL → LogicalPlan → operator executor
│   ├── connectors/
│   │   ├── kafka_cdc/     # Kafka CDC (Debezium envelopes)
│   │   └── pg_wal/        # Postgres logical replication + pgoutput parser
│   ├── formats/
│   │   ├── avro/          # Schema registry + Avro encode/decode
│   │   └── parquet/       # Checkpoint serialisation + restore
│   ├── runtime/           # Pipeline scheduler, metrics, checkpoints
│   └── api/               # REST control plane + /metrics
├── docker/                # Compose, Prometheus, Grafana, K8s
├── examples/word_count/   # Kafka → filter → count → Parquet
├── scripts/               # Crash recovery demo scripts
└── tests/integration/     # End-to-end + recovery tests
```

## Quick start

### Prerequisites

- Rust stable (`rustup` recommended)
- Docker + Docker Compose (for Kafka, Postgres, Schema Registry, Prometheus, Grafana)

### Build

```bash
cd ivm-engine
cargo build --release
cargo test --workspace

# Full Kafka connector (requires CMake on Linux/macOS; Docker uses this automatically):
cargo build --release -p ivm-api --features kafka
```

### Run local infrastructure

```bash
cd docker
docker compose up -d
# Kafka :9092, Postgres :5432, API :8080, Prometheus :9090, Grafana :3000
```

### Start the control plane

```bash
cargo run -p ivm-api
# API available at http://localhost:8080
# Metrics at http://localhost:8080/metrics
```

### SQL planner example

```bash
# CLI
cargo run -p sql_demo -- "SELECT customer_id, SUM(amount) FROM orders WHERE amount > 100 GROUP BY customer_id"
cargo run -p sql_demo -- --execute "SELECT * FROM orders WHERE amount > 50"

# REST
curl -X POST http://localhost:8080/sql/plan \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT customer_id, amount FROM orders WHERE amount > 100"}'

# Pipeline with SQL (topic name must match FROM table)
curl -X POST http://localhost:8080/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-sql",
    "source": {"type":"kafka","brokers":"localhost:9092","topic":"orders","group_id":"ivm"},
    "sql": "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
    "checkpoint_interval_secs": 60
  }'
```

### Create a pipeline

```bash
curl -X POST http://localhost:8080/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "word-count",
    "source": {
      "type": "kafka",
      "brokers": "localhost:9092",
      "topic": "words",
      "group_id": "ivm-word-count"
    },
    "operators": [
      {"type": "aggregate_count", "key_column": "word"}
    ],
    "checkpoint_interval_secs": 60
  }'

curl -X POST http://localhost:8080/pipelines/word-count/start
curl http://localhost:8080/metrics
```

### Postgres WAL pipeline

```bash
# 1. Start Postgres with logical replication enabled
cd docker && docker compose up -d postgres

# 2. Create a pipeline sourced from WAL
curl -X POST http://localhost:8080/pipelines \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "orders-wal",
    "source": {
      "type": "pg_wal",
      "conn_str": "postgres://postgres:postgres@localhost:5432/ivm",
      "slot": "ivm_slot",
      "publication": "ivm_pub"
    },
    "sql": "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
    "checkpoint_interval_secs": 30
  }'

curl -X POST http://localhost:8080/pipelines/orders-wal/start

# 3. Insert rows into Postgres and watch the pipeline update in real time
psql postgres://postgres:postgres@localhost:5432/ivm \
  -c "INSERT INTO orders VALUES (1, 1, 500.00), (2, 1, 250.00), (3, 2, 100.00);"

curl http://localhost:8080/metrics | grep ivm_rows
```

## Why incremental?

Traditional approach: re-run the full query on every change.
This approach (DBSP): maintain only the *delta* of the result.

For a query like `SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id`:
- Traditional: scan all orders on every insert → O(n)
- DBSP incremental: process only the new rows → O(Δ)

This is the same theory powering [Feldera](https://feldera.com), based on
[DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://arxiv.org/abs/2203.16684)
by McSherry, Ryzhyk, et al.

## Benchmarks

Run: `cargo bench -p ivm-operators`

Results on Intel Core i5 (your numbers from the corrected `iter_batched` harness):

| Operation | Batch Size | Throughput | Median latency |
|-----------|-----------|------------|----------------|
| Filter    | 100K rows | ~1.05M rows/sec | 95.05 ms |
| Filter    | 10K rows  | ~1.42M rows/sec | 7.06 ms |
| Join      | 10K rows  | ~560K rows/sec | 17.85 ms |
| Join      | 1K rows   | ~908K rows/sec | 1.10 ms |

Throughput is bounded by `HashMap<String, Value>` key lookup per row (one
string hash per predicate column). A columnar or struct-based row format
would reduce this by 10–20×; the dynamic format was chosen to keep the
DBSP semantics clear.

HTML reports: `target/criterion/`

## Crash recovery demo

```bash
# Integration test (no Docker required):
cargo test -p ivm-integration-tests --test recovery_test

# Live demo script (requires running stack):
bash scripts/demo_recovery.sh

# Record GIF with vhs:
vhs scripts/recovery.tape
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/sql/plan` | Compile SQL to logical plan |
| POST | `/sql/execute` | Execute SQL against inline source batches |
| GET | `/metrics` | Prometheus metrics |
| POST | `/pipelines` | Create pipeline |
| GET | `/pipelines` | List pipelines |
| GET | `/pipelines/:name` | Get pipeline details |
| POST | `/pipelines/:name/start` | Start pipeline |
| POST | `/pipelines/:name/stop` | Stop pipeline |
| DELETE | `/pipelines/:name` | Delete pipeline |

## Key design points

- **Z-set model**: Implements the differentiated dataflow model from [DBSP](https://github.com/feldera/dbsp) — every change is a `(row, weight)` pair.

- **SQL planner**: Parses `SELECT` / `WHERE` / `GROUP BY` / `JOIN` via sqlparser into a logical plan executed by the operator graph.

- **Incremental join**: `Δ(A ⋈ B) = ΔA ⋈ B_old + A_old ⋈ ΔB + ΔA ⋈ ΔB` with persistent history.

- **Parquet checkpoints**: `restore_zset_checkpoint()` reloads the latest epoch snapshot after crash.

- **Postgres WAL (poll mode)**: `PgWalConnector::poll_batch` uses
  `pg_logical_slot_get_changes` — works without a REPLICATION role,
  suitable for local dev and docker-compose.

- **Postgres WAL (streaming mode)**: `WalStreamConnector::stream_events`
  wraps `pg_logical_slot_get_changes` in a channel-backed async stream,
  giving the scheduler a `Stream<Item = WalEvent>` interface. Events are
  batched per transaction and flushed on `Commit`; errors trigger automatic
  reconnect. The transport is poll-based (`tokio-postgres 0.7` does not
  expose the CopyBoth protocol); upgrading to raw `START_REPLICATION`
  requires only replacing the inner poll loop — the scheduler, operator,
  and checkpoint layers need no changes.

- **Prometheus**: `ivm_rows_processed_total`, `ivm_checkpoint_duration_seconds`,
  `ivm_pipelines_running`, and more at `/metrics`.

## License

MIT