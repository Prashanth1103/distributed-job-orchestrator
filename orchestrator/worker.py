import asyncio
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, Optional
import uuid
from orchestrator.config import OrchestratorSettings
from orchestrator.lease import InMemoryRedisAdapter, LeaseManager
from orchestrator.models import JobRecord, JobStatus, JobSubmissionRequest
from orchestrator.telemetry import telemetry


class PermanentJobError(Exception):
    """Raised when a job fails due to non-retryable validation or logic errors."""


class TransientJobError(Exception):
    """Raised when a job encounters temporary downstream timeout or network errors."""


class JobOrchestrator:
    """Coordinates idempotent job intake, reliable queuing, backpressure semaphores, and retries."""

    PENDING_QUEUE = "jobs:pending"
    PROCESSING_QUEUE = "jobs:processing"
    DLQ_QUEUE = "jobs:dlq"

    def __init__(
        self,
        settings: Optional[OrchestratorSettings] = None,
        store: Optional[InMemoryRedisAdapter] = None,
    ) -> None:
        self.settings = settings or OrchestratorSettings()
        self.store = store or InMemoryRedisAdapter()
        self.leases = LeaseManager(
            self.store,
            idem_ttl=self.settings.idempotency_ttl_seconds,
            lease_ttl=self.settings.lease_ttl_seconds,
        )
        self.jobs: Dict[str, JobRecord] = {}
        self._semaphore = asyncio.Semaphore(self.settings.max_concurrent_slots)

    @telemetry(span_name="orchestrator.submit_job", component="api")
    def submit_job(self, req: JobSubmissionRequest) -> Tuple[JobRecord, bool]:
        idem_hash = req.compute_idempotency_hash()
        candidate_id = f"job_{uuid.uuid4().hex[:12]}"
        is_new, job_id = self.leases.acquire_idempotency_lock(idem_hash, candidate_id)

        if not is_new and job_id in self.jobs:
            return self.jobs[job_id], False

        record = JobRecord(
            job_id=job_id,
            idempotency_key=idem_hash,
            tenant_id=req.tenant_id,
            workflow_type=req.workflow_type,
            payload=req.payload,
            status=JobStatus.QUEUED,
            max_attempts=self.settings.max_retry_attempts,
        )
        self.jobs[job_id] = record
        self.store.lpush(self.PENDING_QUEUE, job_id)
        return record, True

    @telemetry(span_name="orchestrator.process_next", component="worker")
    async def process_next(
        self,
        handler: Callable[[JobRecord], Awaitable[str]],
        worker_id: str = "worker-0",
    ) -> Optional[JobRecord]:
        async with self._semaphore:
            job_id = self.store.rpop_lpush(self.PENDING_QUEUE, self.PROCESSING_QUEUE)
            if not job_id:
                return None

            record = self.jobs[job_id]
            if not self.leases.acquire_worker_lease(job_id, worker_id):
                return record

            record.status = JobStatus.RUNNING
            record.attempts += 1
            record.updated_at = datetime.now(timezone.utc)

            try:
                result_uri = await handler(record)
                record.status = JobStatus.SUCCEEDED
                record.result_uri = result_uri
                record.error_message = None
                self.store.lrem(self.PROCESSING_QUEUE, job_id)
                self.leases.release_worker_lease(job_id)
                return record

            except PermanentJobError as exc:
                record.status = JobStatus.DEAD_LETTER
                record.error_message = str(exc)
                self.store.lrem(self.PROCESSING_QUEUE, job_id)
                self.store.lpush(self.DLQ_QUEUE, job_id)
                self.leases.release_worker_lease(job_id)
                return record

            except Exception as exc:
                self.store.lrem(self.PROCESSING_QUEUE, job_id)
                self.leases.release_worker_lease(job_id)
                record.error_message = str(exc)
                if record.attempts >= record.max_attempts:
                    record.status = JobStatus.DEAD_LETTER
                    self.store.lpush(self.DLQ_QUEUE, job_id)
                else:
                    record.status = JobStatus.QUEUED
                    backoff = self.settings.base_backoff_seconds * (2 ** (record.attempts - 1))
                    await asyncio.sleep(min(backoff, 0.05))
                    self.store.lpush(self.PENDING_QUEUE, job_id)
                return record
