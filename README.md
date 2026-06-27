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

| Operation | Batch Size | Throughput |
|-----------|-----------|------------|
| Filter    | 100K rows | ~424K rows/sec |
| Join      | 10K rows  | ~481K rows/sec |
| Checkpoint| 10K rows  | ~ms (see integration tests) |

HTML reports are written to `target/criterion/`.

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

- **Postgres WAL**: pgoutput binary parser + `WalStreamConnector` for logical replication events.

- **Prometheus**: `ivm_rows_processed_total`, `ivm_checkpoint_duration_seconds`, `ivm_pipelines_running`, and more at `/metrics`.

## License

MIT
