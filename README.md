# Webhook Ingestion Service

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Database](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Cache](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)](https://redis.io/)
[![Architecture](https://img.shields.io/badge/Architecture-ACID--Idempotent-brightgreen)](#-architecture--idempotency-design)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A production-grade, highly scalable HTTP webhook ingestion service written in Go. Designed to process high-throughput telephony call-completion events, guarantee **durable exact-once storage**, and maintain zero-drift per-account call analytics under **at-least-once provider delivery**.

---

## 📋 Table of Contents
- [Incident & Defect Resolution Summary](#-incident--defect-resolution-summary)
- [Key Features](#-key-features)
- [Architecture & Idempotency Design](#-architecture--idempotency-design)
- [Database Schema & ER Diagram](#-database-schema--er-diagram)
- [Sequence Diagrams](#-sequence-diagrams)
- [Quick Start & Local Setup](#-quick-start--local-setup)
- [Configuration Reference](#-configuration-reference)
- [API Reference & Examples](#-api-reference--examples)
- [Test Suite & Verification](#-test-suite--verification)
- [Repository Structure](#-repository-structure)
- [10,000 Webhooks/Sec Scaling Blueprint](#-10000-webhookssec-scaling-blueprint)
- [SOLUTION.md Link](#-solutionmd-link)

---

## 🔍 Incident & Defect Resolution Summary

| Incident Symptom | Root Cause | Architectural Fix |
| :--- | :--- | :--- |
| ❌ **Duplicate Call Records** | Missing `UNIQUE` index on `events(event_id)`. Sequential queries allowed duplicate rows. | Created `idx_events_event_id_unique` constraint in PostgreSQL and used `INSERT ... ON CONFLICT (event_id) DO NOTHING`. |
| ❌ **Account Call-Count Drifting** | `IncrementAccountStats` executed unconditionally on redeliveries and retries. | Wrapped event insert, call record upsert, and account stats update into a single atomic PostgreSQL transaction (`IngestEventTx`). Stats increment **only** if the event is newly inserted. |
| ❌ **Recordings Not Marked/Processed** | Async recording background tasks had no error monitoring or completion tracking. | Integrated background goroutine error logging and `sync.WaitGroup` to wait for active recording jobs during graceful shutdown. |
| ❌ **In-Flight Data Disappearing on Deploy** | Server process terminated immediately without waiting for background goroutines to drain. | Implemented `Service.Shutdown(ctx)` with a blocking `WaitGroup` drain and timeout handle in `main.go`. |
| ❌ **Cache Desynchronization on Restart** | In-memory stats cache started empty on reboot without database hydration. | Implemented `InitCache()` to populate cache directly from `account_stats` table during startup, with PostgreSQL fallback queries on cache miss. |

---

## 🌟 Key Features

- **🛡️ Database-Backed Idempotency**: Single-transaction processing (`IngestEventTx`) using PostgreSQL `ON CONFLICT DO NOTHING` guarantees exact-once execution across process restarts and multi-replica clusters.
- **⚡ Concurrency Safety**: Safely resolves parallel duplicate webhooks without race conditions or locks at the application tier (eliminates TOCTOU bugs).
- **🔄 Durable Cache & Fallback**: Thread-safe in-memory cache populated from PostgreSQL at boot, featuring atomic cache sync and database fallback logic.
- **⚓ Graceful Lifecycle Drain**: WaitGroup worker drain blocks service exit until all in-flight asynchronous operations (e.g. call recording transcodes) complete.
- **🛡️ Strict Payload Validation**: Rejects malformed JSON payloads and invalid status values (`completed`, `failed`, `no_answer`) immediately with standard HTTP 400 Bad Request.

---

## 🏗️ Architecture & Idempotency Design

### The Core Problem: Provider At-Least-Once Delivery
Telephony providers guarantee **at-least-once delivery**. A single event (`event_id`) can arrive:
1. Sequentially (due to network retry loops after temporary HTTP 5xx or socket timeouts).
2. Concurrently (due to multi-threaded worker dispatch on the provider end).

Application-level deduplication (e.g., `sync.Mutex` or `map[string]bool`) fails across multi-node deployments or restarts.

```
Request A ──┐
            ├── same event_id ──► [ HTTP Ingest ] ──► [ IngestEventTx (PostgreSQL) ]
Request B ──┘                                                   │
                                                   ON CONFLICT(event_id) DO NOTHING
                                                                │
                                              ┌─────────────────┴─────────────────┐
                                              ▼                                   ▼
                                      Inserted = True                    Inserted = False (Duplicate)
                                 (Stats +1, Upsert Call)               (Rollback Tx, Return 200 OK)
```

### Transactional Atomicity (`IngestEventTx`)
```sql
BEGIN;

-- 1. Insert Event (Idempotency Anchor)
INSERT INTO events (event_id, call_id, account_id, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (event_id) DO NOTHING;

-- If rows_affected == 0, duplicate detected -> ROLLBACK & EXIT (inserted = false)

-- 2. Upsert Call Record
INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (call_id) DO UPDATE SET ...;

-- 3. Atomically Increment Per-Account Stats
INSERT INTO account_stats (account_id, call_count, total_duration_sec)
VALUES ($1, 1, $2)
ON CONFLICT (account_id) DO UPDATE SET
    call_count         = account_stats.call_count + 1,
    total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec;

COMMIT;
```

---

## 📊 Database Schema & ER Diagram

```mermaid
erDiagram
    events {
        text event_id PK "UNIQUE INDEX idx_events_event_id_unique"
        text call_id FK
        text account_id
        jsonb payload
        timestamptz received_at
    }
    calls {
        text call_id PK
        text account_id
        text status
        integer duration_sec
        text recording_url
        boolean recording_processed
        timestamptz updated_at
    }
    account_stats {
        text account_id PK
        bigint call_count
        bigint total_duration_sec
    }

    events }|--|| calls : "references call_id"
    calls }|--|| account_stats : "aggregates under account_id"
```

---

## 🔄 Sequence Diagrams

### 1. Ingestion Request Flow
```mermaid
sequenceDiagram
    autonumber
    participant Provider as Webhook Provider
    participant Router as HTTP Router
    participant Ingest as Ingest Service
    participant Store as Store (PostgreSQL)
    participant Cache as In-Memory Cache

    Provider->>Router: POST /webhooks/calls (JSON Payload)
    Router->>Router: Validate JSON & Event Status
    alt Invalid Payload / Status
        Router-->>Provider: HTTP 400 Bad Request
    else Valid Payload
        Router->>Ingest: Ingest(ctx, Event)
        Ingest->>Store: IngestEventTx(ctx, Event)
        Note over Store: BEGIN TRANSACTION
        Store->>Store: INSERT INTO events ... ON CONFLICT DO NOTHING
        alt Event Already Exists (Duplicate)
            Note over Store: ROLLBACK TRANSACTION
            Store-->>Ingest: inserted = false
            Ingest-->>Router: nil
            Router-->>Provider: HTTP 200 OK (Ignored Duplicate)
        else Event Inserted (New)
            Store->>Store: Upsert Call Record
            Store->>Store: Increment Account Stats (+1 count, +duration)
            Note over Store: COMMIT TRANSACTION
            Store-->>Ingest: inserted = true
            Ingest->>Cache: Record(accountID, durationSec)
            opt Recording URL Present
                Ingest->>Ingest: Spawn Background Worker (wg.Add)
            end
            Ingest-->>Router: nil
            Router-->>Provider: HTTP 200 OK
        end
    end
```

---

## 🚀 Quick Start & Local Setup

### Prerequisites
- **Docker & Docker Compose** (Recommended)
- **Go 1.22+** (For local testing without Docker)

### 1. Run via Docker Compose
To launch PostgreSQL, Redis, and the Webhook service:

```bash
docker compose up -d --build
```

### 2. Verify Service Health
```bash
curl -i http://localhost:8080/healthz
# Output: HTTP/1.1 200 OK -> ok
```

### 3. Run Automated Tests
```bash
go test -v ./...
```

### 4. Reset Clean State
Tears down containers, purges data volumes, and re-applies migrations:

```bash
make reset
```

---

## ⚙️ Configuration Reference

Configuration is managed via environment variables (loaded via `internal/config`):

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `APP_PORT` | `8080` | HTTP Server port |
| `POSTGRES_PORT` | `5432` | Host port for PostgreSQL container |
| `REDIS_PORT` | `6379` | Host port for Redis container |
| `DATABASE_URL` | `postgres://webhook:webhook@localhost:5432/webhook?sslmode=disable` | PostgreSQL DSN string |
| `REDIS_ADDR` | `localhost:6379` | Redis host:port connection string |
| `DB_MAX_CONNS` | `25` | Maximum PostgreSQL connection pool size |

---

## 📖 API Reference & Examples

### `POST /webhooks/calls`
Ingests telephony call-completion events.

#### Request Header
`Content-Type: application/json`

#### Example Body
```json
{
  "event_id":      "evt_01H8XK2M9P",
  "call_id":       "call_9f2ab31c",
  "account_id":    "acc_123",
  "status":        "completed",
  "duration_sec":  143,
  "recording_url": "https://recordings.example.com/9f2ab31c.wav",
  "occurred_at":   "2026-08-13T09:12:00Z"
}
```

#### Status Codes
- `200 OK`: Successfully processed or duplicate safely ignored.
- `400 Bad Request`: Invalid JSON schema, missing required keys, or unknown status (valid: `completed`, `failed`, `no_answer`).
- `500 Internal Server Error`: Storage transaction unexpected failure.

---

### `GET /accounts/{account_id}/stats`
Retrieves cumulative statistics for a given account.

#### Example Request
```bash
curl -s http://localhost:8080/accounts/acc_123/stats
```

#### Example Response
```json
{
  "call_count": 1,
  "total_duration_sec": 143
}
```

---

### `GET /healthz`
Service liveness check endpoint.

#### Example Response
`HTTP 200 OK` → `ok`

---

## 🧪 Test Suite & Verification

The repository includes a comprehensive test harness in `internal/testutil` that isolates test data per account, enabling parallel database execution (`go test ./...`).

```
internal/
├── httpapi/
│   └── handler_test.go      # HTTP router, payload validation, status code tests
├── ingest/
│   └── service_test.go      # Idempotency, 10x duplicates, concurrency, redelivery tests
├── stats/
│   └── cache_test.go        # In-memory thread safety & stats aggregation tests
└── store/
    └── store_test.go        # PostgreSQL transaction, conflict resolution, stats tests
```

### Summary of Tested Scenarios

1. ✅ **First Webhook Delivery**: Verified event stored, call created, stats = 1.
2. ✅ **Duplicate Delivery (2x)**: Verified second identical delivery returns 200 OK, event count = 1, stats = 1.
3. ✅ **Heavy Redelivery (10x)**: 10 identical POST calls result in exactly 1 record and +1 stats increment.
4. ✅ **Redelivery After HTTP 200**: Late redeliveries after successful ack are safely ignored.
5. ✅ **Concurrent Redeliveries**: 15 concurrent goroutines posting the same `event_id` simultaneously result in exactly 1 DB record and +1 stats update.
6. ✅ **Independent Event IDs**: Different `event_id` payloads for the same account accumulate stats correctly.
7. ✅ **Transaction Rollback**: Invalid SQL steps trigger full rollback without partial updates.
8. ✅ **Payload Validation**: Rejects malformed JSON and unknown statuses (`unknown_status`) with 400 Bad Request.

---

## 📁 Repository Structure

```
webhook-ingest/
├── cmd/
│   └── server/
│       └── main.go              # Entrypoint, DB wiring, cache hydration, graceful shutdown
├── internal/
│   ├── config/                  # Environment variable parser
│   ├── httpapi/                 # HTTP routing, payload validation, response helpers
│   ├── ingest/                  # Core ingestion service & worker pool management
│   ├── redisclient/             # Redis connection manager
│   ├── stats/                   # Thread-safe in-memory account aggregate cache
│   ├── store/                   # PostgreSQL repository & transactional query logic
│   └── testutil/                # Shared integration test harness & isolated fixtures
├── migrations/
│   ├── 001_init.sql             # Initial PostgreSQL tables (events, calls, account_stats)
│   └── 002_unique_event_id.sql  # Unique constraint migration on events(event_id)
├── Dockerfile                   # Multi-stage optimized Go build
├── docker-compose.yml           # Environment compose configuration
├── Makefile                     # Helper developer targets
├── README.md                    # System documentation
└── SOLUTION.md                  # Detailed solution writeup & 10k/sec scaling blueprint
```

---

## ⚡ 10,000 Webhooks/Sec Scaling Blueprint

To scale the architecture from local execution to **10,000 webhooks/sec**, the synchronous HTTP-to-PostgreSQL path must be transformed into an asynchronous event-streaming pipeline:

```
                                 [ 10,000 Webhooks/sec ]
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │   Edge API Gateway / Ingestion  │ (Ultra-fast validation & 202 Accepted)
                           └────────────────┬────────────────┘
                                            │
                                            ▼ Pushes to Message Stream
                           ┌─────────────────────────────────┐
                           │  Apache Kafka / AWS Kinesis /   │ (Partitioned by account_id)
                           │         Redis Streams           │
                           └────────────────┬────────────────┘
                                            │
                                            ▼ Parallel Worker Consumption
                           ┌─────────────────────────────────┐
                           │    Ingest Consumer Worker Pool   │
                           └────────────────┬────────────────┘
                                            │ (Micro-batching pgx.Batch / COPY)
                                            ▼
                           ┌─────────────────────────────────┐
                           │  PgBouncer (Connection Pooler)  │
                           └────────────────┬────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │ PostgreSQL Cluster / Distributed│ (Primary-Replica / Citus)
                           └─────────────────────────────────┘
```

See [`SOLUTION.md`](SOLUTION.md) for full architectural design specs, micro-batching configurations, and Redis write-behind caching strategies.

---

## 📄 SOLUTION.md Link

For full details on defect analysis, deduplication rationale, and evaluation answers, check out **[`SOLUTION.md`](SOLUTION.md)**.
