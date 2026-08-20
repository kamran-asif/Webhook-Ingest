# Solution Architecture & Technical Writeup: Webhook Ingestion Service

## 1. Executive Summary & Defect Analysis

The original implementation of the webhook ingestion service exhibited several critical failure modes due to lack of **ACID Transaction Boundaries**, missing **Storage-Tier Unique Constraints**, and unhandled **At-Least-Once Delivery Semantics**:

1. **Time-of-Check to Time-of-Use (TOCTOU) Race Conditions**:
   - The ingestion handler executed non-atomic, sequential database queries (`InsertEvent`, `UpsertCall`, `IncrementAccountStats`). Under concurrent HTTP deliveries carrying identical `event_id` keys, multiple parallel goroutines performed read checks (`EventExists`), evaluated `false`, and proceeded to execute duplicate inserts and aggregate increments.
2. **Missing Storage-Tier Uniqueness**:
   - The `events` table lacked a `UNIQUE` index constraint (only possessing a standard non-unique index `idx_events_event_id`), allowing duplicate `event_id` records to be committed to PostgreSQL disk blocks.
3. **Unbounded Redelivery Side-Effects & Statistics Drift**:
   - Provider retries and redeliveries triggered repeated execution of `account_stats` increments, causing `call_count` and `total_duration_sec` metrics to double/triple count.
4. **Volatile Memory Cold-Start State**:
   - The in-memory statistics cache was initialized empty on server boot without **Database Hydration / Cache Warming**, causing cache misses or inaccurate stats upon process restarts.
5. **Abrupt Process Lifecycle Termination**:
   - Asynchronous worker goroutines (such as `processRecording`) lacked **Goroutine Lifecycle Tracking** (`sync.WaitGroup`), causing in-flight transcoding jobs to drop silently during deployment container restarts.

---

## 2. Distributed Messaging Semantics & Duplication Root Cause

Upstream webhook providers operate on **At-Least-Once Delivery Semantics** to guarantee payload delivery despite transient network failures, HTTP 5xx responses, or connection dropouts.

Consequently, identical payloads carrying the same **Idempotency Key (`event_id`)** may be delivered:
- **Sequentially**: Provider retries following transient timeouts or network delays (even after a prior 200 OK reached the transport layer).
- **Concurrently**: Multi-threaded worker dispatch on the provider side triggering parallel HTTP POST streams.

Without **Database Engine-Level Enforcements**, application-tier pre-checks (`if s.store.EventExists(...)`) are inherently vulnerable to concurrency race conditions.

---

## 3. Storage-Tier Uniqueness vs Application-Tier Deduplication

Application-level deduplication (e.g., `sync.Mutex`, `sync.Map`, or `map[string]bool` in Go) is fundamentally inadequate for production systems because:
- State is confined to a single OS process heap and cannot be shared across horizontally scaled application replicas behind a Load Balancer.
- Memory state is volatile and wiped during container restarts, deployment rollouts, or node failures.

A PostgreSQL **B-Tree Unique Index Constraint** (`idx_events_event_id_unique` on `events(event_id)`) serves as the single, centralized, durable source of truth. It enforces uniqueness across all application nodes and survives process restarts.

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique ON events (event_id);
```

---

## 4. Atomic PostgreSQL Transaction Architecture (`IngestEventTx`)

Enforcing uniqueness on `events(event_id)` alone is insufficient; entity creation (`calls`) and aggregate computation (`account_stats`) must execute **atomically** with event insertion.

We engineered `IngestEventTx(ctx context.Context, e Event) (bool, error)` utilizing **Optimistic Concurrency Control (OCC)**:

```sql
BEGIN TRANSACTION;

-- Step 1: Idempotency Anchor & Insert Execution
INSERT INTO events (event_id, call_id, account_id, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (event_id) DO NOTHING;

-- If rows_affected == 0 (Duplicate event_id detected):
--   Immediate ROLLBACK TRANSACTION & RETURN inserted = false

-- Step 2: Entity State Synchronization
INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (call_id) DO UPDATE SET
    status        = EXCLUDED.status,
    duration_sec  = EXCLUDED.duration_sec,
    recording_url = EXCLUDED.recording_url,
    updated_at    = now();

-- Step 3: Atomic Per-Account Aggregate Mutation
INSERT INTO account_stats (account_id, call_count, total_duration_sec)
VALUES ($1, 1, $2)
ON CONFLICT (account_id) DO UPDATE SET
    call_count         = account_stats.call_count + 1,
    total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec;

COMMIT TRANSACTION;
```

### Architectural Guarantees:
- **ACID Atomicity & Consistency**: If any query step fails, PostgreSQL issues a full rollback. Partial state mutations are mathematically impossible.
- **Exact-Once Processing Semantics (EOPS)**: When `event_id` already exists, `INSERT ... ON CONFLICT DO NOTHING` returns 0 affected rows. The transaction immediately rolls back and exits without modifying call entities or incrementing account metrics.

---

## 5. Architectural Evaluation & Rejection Matrix

| Pattern / Alternative | Technical Reason for Rejection |
| :--- | :--- |
| **In-Memory Go Mutex / Map** | Non-durable across process restarts; fails under horizontal multi-node scaling. |
| **Redis `SETNX` Lock Only** | Network round-trip latency overhead; risk of key eviction, split-brain, and lacks ACID binding with PostgreSQL database writes. |
| **Application SELECT-then-INSERT** | Inherently vulnerable to Time-of-Check to Time-of-Use (TOCTOU) race conditions under concurrent deliveries. |
| **Read-Time Aggregation (`COUNT(DISTINCT)`)** | O(N) database query scan over millions of rows on every API request, causing heavy CPU and Disk I/O degradation. |

---

## 6. High-Throughput Scaling Blueprint (10,000 Webhooks/Sec)

To scale from single-node execution to **10,000 webhooks/second**, the architecture must transition from synchronous HTTP-to-DB writes to a decoupled, **Event-Driven Streaming Architecture (EDA)**:

```
[ Webhook Provider ]
        │
        ▼ (HTTP POST /webhooks/calls)
[ Edge Ingestion Gateway ] ──────► (Token Bucket Rate Limiting + Contract Validation)
        │
        ▼ Publishes Event Payload (<5ms Ack)
[ Apache Kafka / AWS Kinesis / Redis Stream ] (Partition Key: account_id)
        │
        ▼ Consumer Worker Pool (Parallel Scaling)
[ Ingestion Worker Cluster ] ───► (Micro-Batching via pgx.Batch / COPY)
        │
        ▼
[ PgBouncer Connection Pooler ] (Transaction-Level Pooling Mode)
        │
        ▼
[ PostgreSQL Cluster / Distributed DB ] (Citus Sharding / Read-Replicas)
```

### Key Scaling Pillars:
1. **Asynchronous Edge Decoupling**:
   - Edge ingestion API nodes perform contract validation, publish raw payloads to a distributed stream (Kafka/Redis Stream), and immediately issue `202 Accepted` (<5ms response time).
2. **Partitioning & Sharding Strategy**:
   - Stream topics are partitioned by `account_id` to maintain ordered processing per tenant while enabling massive horizontal consumer scaling.
3. **Micro-Batching & WAL Optimization**:
   - Ingestion consumers process events using `pgx.Batch` or PostgreSQL `COPY` protocol in micro-batches (e.g., 500 events per transaction), drastically reducing Write-Ahead Logging (WAL) I/O and network overhead.
4. **CQRS & Read-Side Caching**:
   - Reads (`GET /accounts/{id}/stats`) are served entirely from Redis/Memcached with write-behind or pub-sub invalidation, separating read traffic from storage writes.
5. **Connection Pooling via PgBouncer**:
   - Deploy PgBouncer in **Transaction-Level Pooling** mode to multiplex thousands of worker connections down to a bounded set of PostgreSQL engine connections.
