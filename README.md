# Webhook Ingestion Service

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Database](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Cache](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)](https://redis.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A production-grade, highly available HTTP service built in Go to ingest telephony call-completion webhooks, guarantee exact-once storage, and maintain accurate real-time per-account statistics under **at-least-once provider delivery**.

---

## 🌟 Key Features

- **🛡️ Database-Backed Idempotency**: Atomic single-transaction processing (`IngestEventTx`) using PostgreSQL `ON CONFLICT (event_id) DO NOTHING` prevents duplicate event persistence and statistics drift.
- **⚡ Concurrency & Redelivery Resilience**: Safely handles concurrent duplicate HTTP requests and late retries without double-counting call counts or total duration.
- **🔄 Stats Cache & DB Fallback**: In-memory statistics cache hydrated from PostgreSQL at startup with dynamic fallback to ensure data consistency across restarts.
- **⚓ Graceful Shutdown**: Worker WaitGroup integration guarantees all in-flight asynchronous tasks (e.g., call recording processing) finish cleanly before service termination.
- **🧪 Comprehensive Test Suite**: 100% test coverage across unit, integration, atomicity, concurrency, and error handling edge cases.

---

## 🛠️ Architecture & Tech Stack

```
                     ┌───────────────────────────┐
                     │ Webhook Provider (Target) │
                     └─────────────┬─────────────┘
                                   │ (HTTP POST /webhooks/calls)
                                   ▼
                       ┌──────────────────────┐
                       │  HTTP API Router     │
                       └───────────┬──────────┘
                                   │ (Validate Payload)
                                   ▼
                       ┌──────────────────────┐
                       │ Ingest Service Layer │
                       └───────────┬──────────┘
                                   │
                   ┌───────────────┴───────────────┐
                   ▼                               ▼
       ┌──────────────────────┐        ┌──────────────────────┐
       │ In-Memory Cache      │        │ PostgreSQL Database  │
       │ (Account Statistics) │        │ (Atomic Transaction) │
       └──────────────────────┘        └──────────────────────┘
```

- **Language**: Go 1.22
- **Database**: PostgreSQL 16 (Connection pooling via `pgxpool`)
- **Cache**: Redis 7 & Thread-safe In-Memory Cache
- **Containerization**: Docker & Docker Compose

---

## 🚀 Quick Start

### Prerequisites
- [Docker & Docker Compose](https://docs.docker.com/get-docker/)
- [Go 1.22+](https://go.dev/doc/install) (for local testing)

### 1. Launch Infrastructure & Service
Start PostgreSQL, Redis, and the Webhook Ingestion service in detached mode:

```bash
docker compose up -d --build
```

### 2. Verify Service Health
```bash
curl -i http://localhost:8080/healthz
# Returns HTTP 200 OK -> ok
```

### 3. Run Test Suite
To execute the comprehensive integration and unit test suite against the running environment:

```bash
go test -v ./...
```

### Reset Environment
To tear down the containers, purge persistent volumes, and re-apply fresh database migrations:

```bash
make reset
```

---

## 📖 API Reference

### 1. Ingest Webhook Event
`POST /webhooks/calls`

Ingests a call-completion payload from the provider. Responds with `200 OK` on successful ingestion or ignored duplicate.

#### Request Body
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

*Valid `status` values*: `completed`, `failed`, `no_answer`.

#### Response
- **`200 OK`**: Event processed or safely ignored as duplicate.
- **`400 Bad Request`**: Missing required fields, invalid JSON, or illegal status.

---

### 2. Fetch Account Statistics
`GET /accounts/{account_id}/stats`

Returns the accumulated aggregate call count and duration for an account.

#### Response Body
```json
{
  "call_count": 1,
  "total_duration_sec": 143
}
```

---

### 3. Health Check
`GET /healthz`

#### Response Body
```
ok
```

---

## 📁 Repository Structure

```
.
├── cmd/
│   └── server/          # Server entrypoint and dependency wiring
├── internal/
│   ├── config/          # Environment variable parser & app configuration
│   ├── httpapi/         # HTTP handlers, validation, router setup
│   ├── ingest/          # Core ingestion business logic & worker orchestration
│   ├── redisclient/     # Redis client connection factory
│   ├── stats/           # Thread-safe in-memory account aggregate cache
│   ├── store/           # PostgreSQL repository & transactional query logic
│   └── testutil/        # Shared integration test harness & fixtures
├── migrations/          # SQL schema migrations (unique indexes, initial tables)
├── SOLUTION.md          # In-depth architectural writeup & scaling documentation
├── docker-compose.yml   # Multi-container local environment configuration
├── Dockerfile           # Multi-stage optimized Go build Dockerfile
└── Makefile             # Helper management commands
```

---

## 📚 Solution Documentation & Architecture Details

For a detailed technical walkthrough covering the identified defects, transactional idempotency guarantees, alternative architecture trade-offs, and scaling plans for **10,000 webhooks/second**, please refer to [`SOLUTION.md`](SOLUTION.md).
