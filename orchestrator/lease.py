import time
from typing import Dict, Optional, Tuple


class InMemoryRedisAdapter:
    """Thread/coroutine-safe Redis-compatible store supporting atomic SETNX and TTL leases."""

    def __init__(self) -> None:
        self._kv: Dict[str, Tuple[str, Optional[float]]] = {}
        self._lists: Dict[str, list[str]] = {}

    def _purge_expired(self, key: str) -> None:
        if key in self._kv:
            _, exp = self._kv[key]
            if exp is not None and time.monotonic() >= exp:
                del self._kv[key]

    def set_nx_ex(self, key: str, value: str, ttl_seconds: int) -> bool:
        self._purge_expired(key)
        if key in self._kv:
            return False
        self._kv[key] = (value, time.monotonic() + ttl_seconds)
        return True

    def get(self, key: str) -> Optional[str]:
        self._purge_expired(key)
        entry = self._kv.get(key)
        return entry[0] if entry else None

    def set_ex(self, key: str, value: str, ttl_seconds: int) -> None:
        self._kv[key] = (value, time.monotonic() + ttl_seconds)

    def delete(self, key: str) -> None:
        self._kv.pop(key, None)

    def lpush(self, queue: str, item: str) -> int:
        lst = self._lists.setdefault(queue, [])
        lst.insert(0, item)
        return len(lst)

    def rpop_lpush(self, source: str, destination: str) -> Optional[str]:
        src = self._lists.get(source, [])
        if not src:
            return None
        item = src.pop()
        dst = self._lists.setdefault(destination, [])
        dst.insert(0, item)
        return item

    def lrem(self, queue: str, item: str) -> bool:
        lst = self._lists.get(queue, [])
        if item in lst:
            lst.remove(item)
            return True
        return False

    def llen(self, queue: str) -> int:
        return len(self._lists.get(queue, []))


class LeaseManager:
    """Manages atomic request deduplication (idempotency keys) and worker slot leases."""

    def __init__(self, store: InMemoryRedisAdapter, idem_ttl: int = 900, lease_ttl: int = 300) -> None:
        self.store = store
        self.idem_ttl = idem_ttl
        self.lease_ttl = lease_ttl

    def acquire_idempotency_lock(self, idem_hash: str, job_id: str) -> Tuple[bool, str]:
        key = f"idem:{idem_hash}"
        acquired = self.store.set_nx_ex(key, job_id, self.idem_ttl)
        if acquired:
            return True, job_id
        existing_job_id = self.store.get(key) or job_id
        return False, existing_job_id

    def acquire_worker_lease(self, job_id: str, worker_id: str) -> bool:
        key = f"lease:{job_id}"
        return self.store.set_nx_ex(key, worker_id, self.lease_ttl)

    def renew_worker_lease(self, job_id: str, worker_id: str) -> bool:
        key = f"lease:{job_id}"
        current = self.store.get(key)
        if current != worker_id:
            return False
        self.store.set_ex(key, worker_id, self.lease_ttl)
        return True

    def release_worker_lease(self, job_id: str) -> None:
        self.store.delete(f"lease:{job_id}")
