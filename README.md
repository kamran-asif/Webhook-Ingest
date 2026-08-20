# Webhook Ingestion Service

[![CI Test Suite](https://github.com/kamran-asif/Webhook-Ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/kamran-asif/Webhook-Ingest/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Database](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Cache](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)](https://redis.io/)
[![Architecture](https://img.shields.io/badge/Architecture-ACID--Idempotent-brightgreen)](#-architecture--idempotency-design)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A production-grade, highly scalable HTTP webhook ingestion service written in Go. Designed to process high-throughput telephony call-completion events, guarantee **durable exact-once processing semantics (EOPS)**, and maintain zero-drift per-account call analytics under **at-least-once provider delivery**.

---

> [!IMPORTANT]
> **Production Incident Solved**: This repository addresses critical defects including duplicate event storage, aggregate statistics drift, and unhandled background worker goroutine crashes during deployments. All fixes are verified with 100% test suite passing.

---

## 📋 Table of Contents
- [Incident & Defect Resolution Summary](#-incident--defect-resolution-summary)
- [Key Features](#-key-features)
- [Architecture & Idempotency Design](#-architecture--idempotency-design)
- [Database Schema & ER Diagram](#-database-schema--er-diagram)
- [Sequence Diagrams](#-sequence-diagrams)
- [📚 System Engineering & Domain Terminology](#-system-engineering--domain-terminology)
- [Quick Start & Local Setup](#-quick-start--local-setup)
- [Configuration Reference](#-configuration-reference)
- [API Reference & Examples](#-api-reference--examples)
- [Test Suite & Verification](#-test-suite--verification)
- [Repository Structure](#-repository-structure)
- [10,000 Webhooks/Sec Scaling Blueprint](#-10000-webhookssec-scaling-blueprint)
- [SOLUTION.md Link](#-solutionmd-link)

---

## 🔍 Incident & Defect Resolution Summary

| Incident Symptom | Root Cause Term | Architectural Fix & Terminology |
| :--- | :--- | :--- |
| ❌ **Duplicate Call Records** | Missing **B-Tree Unique Constraint** on `events(event_id)`. Non-atomic sequential SELECT-then-INSERT. | Applied **Storage-Tier Uniqueness** (`idx_events_event_id_unique`) and **UPSERT** (`INSERT ... ON CONFLICT DO NOTHING`). |
| ❌ **Account Call-Count Drifting** | **Unbounded Redelivery Side-Effects** & non-transactional statistic updates. | Encapsulated operations in a single **ACID Transaction** (`IngestEventTx`). Statistics increment **only** if the event is newly inserted. |
| ❌ **Recordings Not Marked/Processed** | **Unmonitored Asynchronous Goroutines** lacking exception propagation. | Integrated structured error logging (`slog`) and worker synchronization via **Goroutine Lifecycle Tracking**. |
| ❌ **In-Flight Data Disappearing on Deploy** | **Abrupt Process Termination** without worker drain signal. | Implemented **Graceful Worker Drain** (`Service.Shutdown`) using `sync.WaitGroup` and context timeouts. |
| ❌ **Cache Desynchronization on Restart** | **Cold-Start Empty State** in volatility memory without storage hydration. | Executed **Cache Hydration / Warming** (`InitCache()`) on boot with **Fallback Lookups** on cache miss. |

---

## 🌟 Key Features

- **🛡️ Database-Backed Idempotency**: Single-transaction processing (`IngestEventTx`) using PostgreSQL `ON CONFLICT DO NOTHING` guarantees **Exact-Once Processing Semantics (EOPS)** across horizontal app clusters.
- **⚡ Concurrency & TOCTOU Prevention**: Resolves parallel duplicate webhooks atomically at the database engine level, eliminating **Time-of-Check to Time-of-Use (TOCTOU)** race conditions.
- **🔄 Durable Cache & Fallback**: Thread-safe in-memory cache populated from PostgreSQL at boot, featuring **Optimistic Read-Side CQRS** and fallback querying.
- **⚓ Graceful Lifecycle Drain**: WaitGroup worker drain blocks SIGTERM process shutdown until all in-flight asynchronous operations (e.g. call recording transcodes) complete.
- **🛡️ Strict Contract Validation**: Enforces RFC-compliant HTTP API contracts, rejecting malformed JSON schemas and illegal statuses with standard **400 Bad Request**.

---

## 🏗️ Architecture & Idempotency Design

### The Core Problem: Provider At-Least-Once Delivery
Telephony providers guarantee **At-Least-Once Delivery**. A single event (`event_id`) can arrive:
1. **Sequentially**: Due to network retry loops after temporary HTTP 5xx errors or socket timeouts.
2. **Concurrently**: Due to multi-threaded worker dispatch on the provider side.

> [!NOTE]
> Application-level deduplication (e.g., `sync.Mutex` or `map[string]bool`) fails across multi-node deployments or restarts due to lack of shared memory state.

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

-- 2. Upsert Call Record (State Sync)
INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (call_id) DO UPDATE SET ...;

-- 3. Atomically Increment Per-Account Aggregate Stats
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
        text call_id FK "Foreign Key to calls.call_id"
        text account_id "Partitioning / Tenant Identifier"
        jsonb payload "Raw Webhook JSON Delivery"
        timestamptz received_at "Ingestion Timestamp"
    }
    calls {
        text call_id PK "Primary Call Entity Key"
        text account_id "Tenant Identifier"
        text status "Call Status (completed|failed|no_answer)"
        integer duration_sec "Call Duration in Seconds"
        text recording_url "Media Recording Location"
        boolean recording_processed "Async Job Status Flag"
        timestamptz updated_at "State Mutation Timestamp"
    }
    account_stats {
        text account_id PK "Tenant Identifier"
        bigint call_count "Aggregate Completed Call Count"
        bigint total_duration_sec "Aggregate Total Duration Seconds"
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
    Router->>Router: Contract Validation (JSON & Event Status)
    alt Invalid Payload / Status
        Router-->>Provider: HTTP 400 Bad Request
    else Valid Payload
        Router->>Ingest: Ingest(ctx, Event)
        Ingest->>Store: IngestEventTx(ctx, Event)
        Note over Store: BEGIN TRANSACTION (ACID Boundary)
        Store->>Store: INSERT INTO events ... ON CONFLICT DO NOTHING
        alt Event Already Exists (Duplicate Delivery)
            Note over Store: ROLLBACK TRANSACTION
            Store-->>Ingest: inserted = false
            Ingest-->>Router: nil
            Router-->>Provider: HTTP 200 OK (Idempotent Ack)
        else Event Inserted (New Delivery)
            Store->>Store: Upsert Call Entity State
            Store->>Store: Increment Account Aggregates (+1 count, +duration)
            Note over Store: COMMIT TRANSACTION
            Store-->>Ingest: inserted = true
            Ingest->>Cache: Record(accountID, durationSec)
            opt Recording URL Present
                Ingest->>Ingest: Spawn Worker Goroutine (wg.Add)
            end
            Ingest-->>Router: nil
            Router-->>Provider: HTTP 200 OK
        end
    end
```

---

## 📚 System Engineering & Domain Terminology

| Technical Term | Definition & Context in System |
| :--- | :--- |
| **At-Least-Once Delivery** | Provider delivery semantic where webhooks are retried until an HTTP 2xx acknowledgement is received, potentially delivering duplicate events. |
| **Exact-Once Processing Semantics (EOPS)** | Guarantee that regardless of duplicate deliveries, state mutations (database inserts & aggregate stats increments) execute exactly once. |
| **Idempotency Key (`event_id`)** | Unique payload identifier used by the service to recognize and discard duplicate deliveries safely. |
| **Time-of-Check to Time-of-Use (TOCTOU)** | Concurrency bug where checking state (`SELECT EXISTS`) and updating state (`INSERT`) are separate operations, causing race conditions. Solved via atomic SQL transactions. |
| **ACID Transaction Boundary** | Database isolation (`BEGIN ... COMMIT`) wrapping event insertion, call upsert, and stats updates to guarantee atomicity and rollbacks on failure. |
| **UPSERT (`ON CONFLICT DO NOTHING`)** | Atomic database primitive combining insertion and collision detection in a single query execution step. |
| **CQRS (Command Query Responsibility Segregation)** | Separation of high-throughput write paths (PostgreSQL transaction) and low-latency read paths (In-memory stats cache lookups). |
| **Cache Hydration / Warming** | Bootstrapping the in-memory cache from PostgreSQL on server startup (`InitCache()`) to eliminate cold-start cache misses. |
| **Graceful Worker Drain** | Orderly process shutdown mechanism (`sync.WaitGroup`) waiting for active asynchronous goroutines to complete before SIGTERM process termination. |
| **Micro-Batching** | Optimization technique aggregating thousands of stream events into multi-row SQL transactions (`pgx.Batch` / `COPY`) to minimize Write-Ahead Logging (WAL) I/O overhead. |
| **Transaction-Level Pooling (PgBouncer)** | Database proxy mechanism multiplexing thousands of short-lived client connections over a bounded pool of persistent PostgreSQL connections. |

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

> [!TIP]
> Docker Compose automatically handles PostgreSQL and Redis health checks (`pg_isready` & `redis-cli ping`) before booting the application container.

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
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI automated build & test pipeline
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
├── LICENSE                      # MIT Open Source License
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

> [!TIP]
> See [`SOLUTION.md`](SOLUTION.md) for full architectural design specs, micro-batching configurations, and Redis write-behind caching strategies.

---

## 📄 SOLUTION.md Link

For full details on defect analysis, deduplication rationale, and evaluation answers, check out **[`SOLUTION.md`](SOLUTION.md)**.
