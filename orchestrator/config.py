from pydantic_settings import BaseSettings


class OrchestratorSettings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    max_concurrent_slots: int = 6
    idempotency_ttl_seconds: int = 900
    lease_ttl_seconds: int = 300
    max_retry_attempts: int = 3
    base_backoff_seconds: float = 1.5
    service_name: str = "distributed-job-orchestrator"

    class Config:
        env_prefix = "ORCH_"
