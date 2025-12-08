# Distributed Async Job & Workflow Orchestrator

A lightweight, production-focused asynchronous job orchestration service built with **FastAPI**, **Redis**, **Pydantic v2**, and **OpenTelemetry**.

Designed to protect compute-bound simulation engines and multi-step AI workflows from HTTP `429 Too Many Requests` saturation under heavy concurrent load.

---

## Core Architecture & Reliability Patterns

1. **Idempotent Request Deduplication (`SETNX` + Canonical SHA-256 Hashing):**
   - Computes a canonical SHA-256 digest over `(tenant_id, workflow_type, payload)` regardless of JSON key ordering.
   - Acquires an atomic `SETNX` lock with TTL so duplicate client retries return `200 OK` with the existing `job_id` instead of spawning redundant compute jobs.
2. **Reliable Queue & Lease Heartbeats (`RPOP` + Processing List):**
   - Moves jobs atomically from `jobs:pending` to `jobs:processing` under an expiring worker lease (`lease:{job_id}`), preventing permanent slot leaks if a worker pod is evicted or receives `SIGTERM` mid-execution.
3. **Semaphore Backpressure & Claim-Check Pattern:**
   - Enforces strict per-worker concurrency bounds (`asyncio.Semaphore`) aligned with Kubernetes/Istio pod concurrency limits, returning `202 Accepted` immediately while workers persist large outputs to object storage and return a pre-signed URI.
4. **Failure Classification & Dead-Letter Queue (DLQ):**
   - Distinguishes `TransientJobError` (retried with exponential backoff $t = b \cdot 2^{k-1}$) from `PermanentJobError` (routed immediately to `jobs:dlq`).
5. **Distributed Tracing (`@telemetry` Decorator):**
   - Custom `functools.wraps` decorator factory wrapping both synchronous and asynchronous functions in OpenTelemetry spans with latency and exception metadata.

---

## Request Lifecycle

```
Client ──► POST /v1/jobs ──► SHA-256 Canonical Hash ──► Redis SETNX idem:{hash}
                                                              │
             ◄── 202 Accepted {job_id, status: QUEUED} ◄──────┘
                                                              ▼
Worker Pool ──► RPOP/LPUSH (jobs:pending -> jobs:processing) ──► Acquire Lease
                                                              │
            ├──► Success: Write Blob URI ──► Mark SUCCEEDED ──► Release Lease
            ├──► Transient Error: Exponential Backoff ──► Requeue (up to max_attempts)
            └──► Permanent Error / Exhausted: Route to Dead-Letter Queue (jobs:dlq)
```

---

## Running Locally & Testing

```bash
pip install -r requirements.txt
pytest -v
uvicorn orchestrator.api:app --reload
```
