from typing import Any, Dict
from fastapi import Depends, FastAPI, HTTPException, Response, status
from orchestrator.models import JobRecord, JobSubmissionRequest
from orchestrator.worker import JobOrchestrator

app = FastAPI(
    title="Distributed Async Job Orchestrator",
    version="0.1.0",
    description="Backpressure-managed job orchestration API with SETNX idempotency and OpenTelemetry tracing.",
)

_orchestrator_instance = JobOrchestrator()


def get_orchestrator() -> JobOrchestrator:
    return _orchestrator_instance


@app.post("/v1/jobs", status_code=status.HTTP_202_ACCEPTED)
async def submit_job_endpoint(
    request: JobSubmissionRequest,
    response: Response,
    orch: JobOrchestrator = Depends(get_orchestrator),
) -> Dict[str, Any]:
    record, created = orch.submit_job(request)
    if not created:
        response.status_code = status.HTTP_200_OK
    return {
        "job_id": record.job_id,
        "status": record.status,
        "idempotency_key": record.idempotency_key,
        "deduplicated": not created,
    }


@app.get("/v1/jobs/{job_id}", response_model=JobRecord)
async def get_job_status_endpoint(
    job_id: str,
    orch: JobOrchestrator = Depends(get_orchestrator),
) -> JobRecord:
    record = orch.jobs.get(job_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return record


@app.get("/healthz")
async def healthcheck(orch: JobOrchestrator = Depends(get_orchestrator)) -> Dict[str, Any]:
    return {
        "status": "healthy",
        "pending_jobs": orch.store.llen(orch.PENDING_QUEUE),
        "processing_jobs": orch.store.llen(orch.PROCESSING_QUEUE),
        "dlq_jobs": orch.store.llen(orch.DLQ_QUEUE),
    }
