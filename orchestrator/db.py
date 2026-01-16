from typing import Any, Dict, Optional
import json
import asyncpg
from orchestrator.models import JobRecord, JobStatus
from orchestrator.telemetry import telemetry

JOBS_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS orchestration_jobs (
    job_id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(64) UNIQUE NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    workflow_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'QUEUED',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    version INTEGER NOT NULL DEFAULT 1,
    result_uri TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_status ON orchestration_jobs (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON orchestration_jobs (created_at DESC);
"""


class PostgresJobRepository:
    """
    Durable PostgreSQL state store enforcing ACID transitions, composite B-Tree indexing,
    and optimistic concurrency control (`version = version + 1`).
    """

    def __init__(self, dsn: str = "postgresql://postgres:postgres@localhost:5432/orchestrator") -> None:
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def init_pool(self) -> None:
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=2, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute(JOBS_SCHEMA_DDL)

    @telemetry(span_name="postgres.insert_job", component="db")
    async def insert_job(self, job: JobRecord) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orchestration_jobs
                (job_id, idempotency_key, tenant_id, workflow_type, payload, status, attempts, max_attempts)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                job.job_id,
                job.idempotency_key,
                job.tenant_id,
                job.workflow_type,
                json.dumps(job.payload),
                job.status.value,
                job.attempts,
                job.max_attempts,
            )

    @telemetry(span_name="postgres.transition_status_optimistic", component="db")
    async def transition_status(
        self,
        job_id: str,
        expected_version: int,
        new_status: JobStatus,
        result_uri: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        if not self.pool:
            return True
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE orchestration_jobs
                SET status = $1,
                    result_uri = COALESCE($2, result_uri),
                    error_message = $3,
                    attempts = attempts + 1,
                    version = version + 1,
                    updated_at = NOW()
                WHERE job_id = $4 AND version = $5
                """,
                new_status.value,
                result_uri,
                error_message,
                job_id,
                expected_version,
            )
            return result == "UPDATE 1"
