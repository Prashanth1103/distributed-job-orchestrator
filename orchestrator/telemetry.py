import functools
import inspect
import time
from typing import Any, Callable, Dict, List
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, InMemorySpanExporter

_provider = TracerProvider()
span_exporter = InMemorySpanExporter()
_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
trace.set_tracer_provider(_provider)
tracer = trace.get_tracer("distributed.job.orchestrator")


def telemetry(span_name: str, component: str = "worker") -> Callable:
    """Decorator factory instrumenting sync and async functions with OpenTelemetry spans."""

    def decorator(fn: Callable) -> Callable:
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                start_ns = time.perf_counter_ns()
                with tracer.start_as_current_span(span_name) as span:
                    span.set_attribute("component", component)
                    span.set_attribute("function.name", fn.__name__)
                    try:
                        result = await fn(*args, **kwargs)
                        span.set_attribute("status", "ok")
                        return result
                    except Exception as exc:
                        span.set_attribute("status", "error")
                        span.set_attribute("error.type", type(exc).__name__)
                        span.record_exception(exc)
                        raise
                    finally:
                        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1e6
                        span.set_attribute("duration_ms", round(elapsed_ms, 3))

            return async_wrapper
        else:
            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                start_ns = time.perf_counter_ns()
                with tracer.start_as_current_span(span_name) as span:
                    span.set_attribute("component", component)
                    span.set_attribute("function.name", fn.__name__)
                    try:
                        result = fn(*args, **kwargs)
                        span.set_attribute("status", "ok")
                        return result
                    except Exception as exc:
                        span.set_attribute("status", "error")
                        span.set_attribute("error.type", type(exc).__name__)
                        span.record_exception(exc)
                        raise
                    finally:
                        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1e6
                        span.set_attribute("duration_ms", round(elapsed_ms, 3))

            return sync_wrapper

    return decorator
