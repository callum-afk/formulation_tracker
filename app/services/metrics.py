from __future__ import annotations

from contextvars import ContextVar


# Store the request-scoped identifier so BigQuery logs can be correlated with API timing logs.
request_id_var: ContextVar[str] = ContextVar("request_id", default="-")

# Accumulate total BigQuery wall-clock time per request to isolate DB time from API and rendering overhead.
bq_time_ms_var: ContextVar[float] = ContextVar("bq_time_ms", default=0.0)

# Count BigQuery queries per request so hotspots with excessive query counts are easy to identify.
bq_query_count_var: ContextVar[int] = ContextVar("bq_query_count", default=0)


def reset_request_metrics() -> None:
    # Reset all request-scoped accumulators at middleware entry so each request starts with a clean slate.
    bq_time_ms_var.set(0.0)
    bq_query_count_var.set(0)


def add_bigquery_timing(duration_ms: float) -> None:
    # Add one finished query duration to the request total for end-of-request reporting.
    bq_time_ms_var.set(bq_time_ms_var.get() + max(duration_ms, 0.0))
    # Increment query count in lockstep with timing accumulation for accurate per-request diagnostics.
    bq_query_count_var.set(bq_query_count_var.get() + 1)
