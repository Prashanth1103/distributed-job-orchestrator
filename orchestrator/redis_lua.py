from typing import Optional, Tuple
import redis.asyncio as aioredis
from orchestrator.telemetry import telemetry

# Atomic Lua script: Renew lease only if the caller still owns the lock
RENEW_LEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
else
    return 0
end
"""

# Atomic Lua script: Release lease only if the caller owns the lock
RELEASE_LEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


class AsyncRedisBroker:
    """
    Production async Redis broker implementing atomic SETNX idempotency locks,
    LMOVE reliable queue transitions, and Lua-verified worker lease heartbeats.
    """

    PENDING_QUEUE = "jobs:pending"
    PROCESSING_QUEUE = "jobs:processing"
    DLQ_QUEUE = "jobs:dlq"

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        self.client = aioredis.from_url(redis_url, decode_responses=True)

    @telemetry(span_name="redis.claim_idempotency", component="redis")
    async def claim_idempotency_key(self, idem_hash: str, job_id: str, ttl_seconds: int = 900) -> Tuple[bool, str]:
        key = f"idem:{idem_hash}"
        created = await self.client.set(key, job_id, nx=True, ex=ttl_seconds)
        if created:
            return True, job_id
        existing = await self.client.get(key)
        return False, existing or job_id

    @telemetry(span_name="redis.enqueue_job", component="redis")
    async def enqueue(self, job_id: str) -> None:
        await self.client.lpush(self.PENDING_QUEUE, job_id)

    @telemetry(span_name="redis.claim_next_job", component="redis")
    async def claim_next_job(self, worker_id: str, lease_ttl: int = 300) -> Optional[str]:
        job_id = await self.client.lmove(self.PENDING_QUEUE, self.PROCESSING_QUEUE, "RIGHT", "LEFT")
        if not job_id:
            return None
        await self.client.set(f"lease:{job_id}", worker_id, nx=True, ex=lease_ttl)
        return job_id

    @telemetry(span_name="redis.renew_lease", component="redis")
    async def renew_lease(self, job_id: str, worker_id: str, lease_ttl: int = 300) -> bool:
        res = await self.client.eval(RENEW_LEASE_LUA, 1, f"lease:{job_id}", worker_id, str(lease_ttl))
        return bool(res)

    @telemetry(span_name="redis.ack_job", component="redis")
    async def ack_job(self, job_id: str, worker_id: str) -> None:
        await self.client.lrem(self.PROCESSING_QUEUE, 1, job_id)
        await self.client.eval(RELEASE_LEASE_LUA, 1, f"lease:{job_id}", worker_id)
