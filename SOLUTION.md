# Solution Architecture & Technical Writeup: Webhook Ingestion Service

## 1. Executive Summary & What Was Broken

The initial implementation of the webhook ingestion service suffered from several critical race conditions, lack of data integrity guarantees, and unhandled duplicate payload scenarios under provider **at-least-once delivery**:

1. **Non-Atomic Operations & Race Conditions**: The ingestion workflow executed separate database queries (`InsertEvent`, `UpsertCall`, `IncrementAccountStats`) sequentially outside of a database transaction. Under concurrent requests with the same `event_id`, multiple requests checked `EventExists`, received `false`, and proceeded to increment account call counts and totals multiple times.
2. **Missing Unique Constraints**: The `events` table contained a non-unique index (`idx_events_event_id`), allowing duplicate `event_id` rows to be persisted into PostgreSQL.
3. **Drifting Account Statistics**: Retries and redeliveries caused `account_stats.call_count` and `account_stats.total_duration_sec` to double/triple count calls.
4. **Cache & Worker Lifecycle Vulnerabilities**:
   - The in-memory stats cache was initialized empty on server boot without PostgreSQL hydration, causing cache misses or inaccurate figures after process restarts.
   - Asynchronous background tasks (like `processRecording`) lacked graceful shutdown wait-group tracking, leading to interrupted jobs during server shutdown.

---

## 2. Why Duplicates Happened

Upstream webhook providers operate on **at-least-once delivery** semantics to guarantee payload delivery despite temporary network glitches or timeouts. Consequently, identical webhooks carrying the same `event_id` may arrive:
- **Sequentially**: Due to provider retry policies after transient 5xx HTTP errors or network timeouts (even after a previous 200 OK reached the network layer).
- **Concurrently**: Due to parallel retry execution or multiple webhook worker dispatchers on the provider side.

Without an atomic constraint and transaction at the storage layer, application-level checks like `if s.store.EventExists(...)` suffer from a classic **Time-of-Check to Time-of-Use (TOCTOU)** race condition.

---

## 3. Why Database Uniqueness is Required

Application-level deduplication (such as `sync.Mutex` or `map[string]bool` in Go) is insufficient because:
- It only works within a single OS process/goroutine scope.
- It fails completely across process restarts, deployments, or horizontally scaled server replicas behind a load balancer.

A PostgreSQL **UNIQUE index** (`idx_events_event_id` on `events(event_id)`) acts as the single, centralized, durable source of truth. It enforces uniqueness across all application instances and persists across restarts.

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique ON events (event_id);
```

---

## 4. Why a PostgreSQL Transaction (`IngestEventTx`) is Required

Enforcing uniqueness on `events(event_id)` alone is not enough; the creation of the `calls` record and update of `account_stats` must occur **atomically** with the event insertion.

We introduced `IngestEventTx(ctx context.Context, e Event) (bool, error)`:

```
BEGIN TRANSACTION;

INSERT INTO events (event_id, call_id, account_id, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (event_id) DO NOTHING;

-- If rows_affected == 0 (Duplicate event_id detected):
--   ROLLBACK TRANSACTION & RETURN inserted = false

INSERT INTO calls (...) VALUES (...) ON CONFLICT (call_id) DO UPDATE ...;
INSERT INTO account_stats (...) VALUES (...) ON CONFLICT (account_id) DO UPDATE ...;

COMMIT TRANSACTION;
```

### Key Guarantees:
- **Atomicity (ACID)**: If any step fails, the entire transaction is rolled back. Partial statistics updates are impossible.
- **Idempotency**: If `event_id` already exists, `INSERT INTO events ... ON CONFLICT DO NOTHING` returns 0 affected rows. The service immediately rolls back and exits without modifying `calls` or incrementing `account_stats`.
- **Exact-Once Statistics Integrity**: `account_stats.call_count` is incremented exactly once per unique `event_id`.

---

## 5. Why Alternative Approaches Were Rejected

| Alternative | Why Rejected |
| :--- | :--- |
| **In-Memory Go Mutex / Map** | Fails across process restarts, deployments, and horizontal multi-replica scaling. |
| **Redis `SETNX` Lock Only** | Introduces network overhead, potential key eviction issues, split-brain scenarios, and lacks transactional ACID binding with PostgreSQL writes. |
| **Application SELECT-then-INSERT** | Prone to TOCTOU race conditions under concurrent requests. |
| **Deduplicating at Statistics Query Time** | Computationally expensive SQL queries (`COUNT(DISTINCT call_id)`) over millions of rows on every API request. |

---

## 6. How to Scale to 10,000 Webhooks/Second

To handle a high-throughput ingestion target of 10,000 webhooks/sec, the direct HTTP-to-Database synchronous write pattern must evolve to an asynchronous, stream-oriented architecture:

```
[ Webhook Provider ]
        │
        ▼ (HTTP POST /webhooks/calls)
[ API Gateway / Ingest Nodes ] ──(Validation + Token Bucket Rate Limiting)
        │
        ▼ Pushes to Message Bus
[ Apache Kafka / AWS Kinesis / Redis Stream ] (Partitioned by account_id)
        │
        ▼ Consumer Worker Pool
[ Ingest Workers ] ──(Micro-batch DB Inserts via pgx.Batch / COPY)
        │
        ▼
[ PostgreSQL Cluster / Distributed DB ] (Primary-Replica / Citus / CockroachDB)
```

### Architectural Scalability Plan:
1. **Asynchronous Edge Ingestion**:
   - The HTTP endpoint validates the payload structure and signature, publishes the raw event to a high-throughput stream (e.g., Apache Kafka or Redis Streams), and immediately returns `202 Accepted` (< 5ms response time).
2. **Message Partitioning**:
   - Partition stream topics by `account_id` or `call_id` to guarantee ordered processing per account while enabling massive horizontal consumer parallelization.
3. **Micro-Batching Database Operations**:
   - Worker processes use `pgx.Batch` or PostgreSQL `COPY` to insert events and upsert aggregates in micro-batches (e.g., 500 events per transaction). Batching reduces disk write overhead, WAL contention, and round-trip network latency by orders of magnitude.
4. **Caching & Read-Side Optimization**:
   - Serve `/accounts/{id}/stats` directly from Redis/Memcached with write-behind or pub-sub cache invalidation.
5. **Database Connection Pooling & PgBouncer**:
   - Deploy PgBouncer in transaction-pooling mode in front of PostgreSQL to handle thousands of concurrent worker connections cleanly.
