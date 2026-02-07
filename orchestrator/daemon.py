import asyncio
import contextlib
import signal
from typing import Awaitable, Callable
from orchestrator.models import JobRecord
from orchestrator.telemetry import telemetry
from orchestrator.worker import JobOrchestrator


class LeaseHeartbeatDaemon:
    """
    Runs a background heartbeat loop renewing the worker's Redis lease every `interval_sec`
    while a long-running (2-10 min) simulation or agent workflow executes.
    Handles graceful SIGTERM drain during Kubernetes rolling updates.
    """

    def __init__(
        self,
        orchestrator: JobOrchestrator,
        worker_id: str = "worker-pod-0",
        heartbeat_interval_sec: float = 30.0,
    ) -> None:
        self.orchestrator = orchestrator
        self.worker_id = worker_id
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self._stop_event = asyncio.Event()

    def request_shutdown(self, *_: object) -> None:
        self._stop_event.set()

    async def _heartbeat_loop(self, job_id: str) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(self.heartbeat_interval_sec)
            self.orchestrator.leases.renew_worker_lease(job_id, self.worker_id)

    @telemetry(span_name="daemon.execute_with_heartbeat", component="daemon")
    async def run_once_with_heartbeat(
        self,
        handler: Callable[[JobRecord], Awaitable[str]],
    ) -> JobRecord | None:
        async def wrapped_handler(job: JobRecord) -> str:
            hb_task = asyncio.create_task(self._heartbeat_loop(job.job_id))
            try:
                return await handler(job)
            finally:
                hb_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await hb_task

        return await self.orchestrator.process_next(wrapped_handler, worker_id=self.worker_id)
