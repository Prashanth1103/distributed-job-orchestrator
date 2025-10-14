from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
import hashlib
import json
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class JobSubmissionRequest(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=64)
    workflow_type: str = Field(..., min_length=1, max_length=64)
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10)

    def compute_idempotency_hash(self) -> str:
        canonical = json.dumps(
            {
                "tenant_id": self.tenant_id,
                "workflow_type": self.workflow_type,
                "payload": self.payload,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class JobRecord(BaseModel):
    job_id: str
    idempotency_key: str
    tenant_id: str
    workflow_type: str
    payload: Dict[str, Any]
    status: JobStatus = JobStatus.QUEUED
    attempts: int = 0
    max_attempts: int = 3
    result_uri: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
