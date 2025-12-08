import asyncio
import pytest
from fastapi.testclient import TestClient
from orchestrator.api import app, get_orchestrator
from orchestrator.config import OrchestratorSettings
from orchestrator.models import JobRecord, JobStatus, JobSubmissionRequest
from orchestrator.telemetry import span_exporter
from orchestrator.worker import JobOrchestrator, PermanentJobError, TransientJobError


@pytest.fixture
def orchestrator() -> JobOrchestrator:
    span_exporter.clear()
    settings = OrchestratorSettings(max_concurrent_slots=2, max_retry_attempts=3, base_backoff_seconds=0.01)
    return JobOrchestrator(settings=settings)


def test_idempotent_job_submission_deduplicates_identical_payloads(orchestrator: JobOrchestrator):
    req1 = JobSubmissionRequest(
        tenant_id="ab-inbev-in",
        workflow_type="price_elasticity_sim",
        payload={"sku_id": "SKU_104", "discount_pct": 0.10},
    )
    req2 = JobSubmissionRequest(
        tenant_id="ab-inbev-in",
        workflow_type="price_elasticity_sim",
        payload={"discount_pct": 0.10, "sku_id": "SKU_104"},  # Reordered keys
    )

    rec1, created1 = orchestrator.submit_job(req1)
    rec2, created2 = orchestrator.submit_job(req2)

    assert created1 is True
    assert created2 is False
    assert rec1.job_id == rec2.job_id
    assert orchestrator.store.llen(orchestrator.PENDING_QUEUE) == 1


@pytest.mark.asyncio
async def test_worker_successful_execution_and_claim_check_uri(orchestrator: JobOrchestrator):
    req = JobSubmissionRequest(tenant_id="t1", workflow_type="sim", payload={"run": 1})
    rec, _ = orchestrator.submit_job(req)

    async def handler(job: JobRecord) -> str:
        await asyncio.sleep(0.01)
        return f"blob://results/{job.job_id}.parquet?sv=2026-01-01"

    updated = await orchestrator.process_next(handler)
    assert updated is not None
    assert updated.status == JobStatus.SUCCEEDED
    assert updated.result_uri == f"blob://results/{rec.job_id}.parquet?sv=2026-01-01"
    assert orchestrator.store.llen(orchestrator.PROCESSING_QUEUE) == 0


@pytest.mark.asyncio
async def test_transient_failures_retry_and_move_to_dlq_after_max_attempts(orchestrator: JobOrchestrator):
    req = JobSubmissionRequest(tenant_id="t1", workflow_type="sim", payload={"flaky": True})
    rec, _ = orchestrator.submit_job(req)

    async def failing_handler(job: JobRecord) -> str:
        raise TransientJobError("Database connection reset")

    # Attempt 1 -> requeued
    await orchestrator.process_next(failing_handler)
    assert rec.status == JobStatus.QUEUED
    assert rec.attempts == 1

    # Attempt 2 -> requeued
    await orchestrator.process_next(failing_handler)
    assert rec.status == JobStatus.QUEUED
    assert rec.attempts == 2

    # Attempt 3 -> exhausted -> DEAD_LETTER
    await orchestrator.process_next(failing_handler)
    assert rec.status == JobStatus.DEAD_LETTER
    assert rec.attempts == 3
    assert orchestrator.store.llen(orchestrator.DLQ_QUEUE) == 1


@pytest.mark.asyncio
async def test_permanent_error_bypasses_retries_directly_to_dlq(orchestrator: JobOrchestrator):
    req = JobSubmissionRequest(tenant_id="t1", workflow_type="sim", payload={"bad_schema": True})
    rec, _ = orchestrator.submit_job(req)

    async def bad_handler(job: JobRecord) -> str:
        raise PermanentJobError("Invalid negative SKU price")

    await orchestrator.process_next(bad_handler)
    assert rec.status == JobStatus.DEAD_LETTER
    assert rec.attempts == 1
    assert orchestrator.store.llen(orchestrator.DLQ_QUEUE) == 1


def test_fastapi_endpoints_and_telemetry_spans(orchestrator: JobOrchestrator):
    app.dependency_overrides[get_orchestrator] = lambda: orchestrator
    client = TestClient(app)

    payload = {
        "tenant_id": "enterprise-01",
        "workflow_type": "forecast_batch",
        "payload": {"region": "APAC", "horizon_months": 12},
    }
    res1 = client.post("/v1/jobs", json=payload)
    assert res1.status_code == 202
    data1 = res1.json()
    assert data1["deduplicated"] is False

    res2 = client.post("/v1/jobs", json=payload)
    assert res2.status_code == 200
    assert res2.json()["deduplicated"] is True

    status_res = client.get(f"/v1/jobs/{data1['job_id']}")
    assert status_res.status_code == 200
    assert status_res.json()["status"] == "QUEUED"

    spans = span_exporter.get_finished_spans()
    assert any(s.name == "orchestrator.submit_job" for s in spans)
