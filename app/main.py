from __future__ import annotations

import logging
import time
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.auth import get_auth_context
from app.dependencies import init_services, init_settings
from app.api.ingredients_api import router as ingredients_router
from app.api.batches_api import router as batches_router
from app.api.sets_api import router as sets_router
from app.api.weights_api import router as weights_router
from app.api.batch_variants_api import router as batch_variants_router
from app.api.formulations_api import router as formulations_router
from app.api.location_codes_api import router as location_codes_router
from app.api.compounding_how_api import router as compounding_how_router
from app.api.pellet_bags_api import router as pellet_bags_router
from app.api.pellet_bag_status_api import router as pellet_bag_status_router
from app.api.conversion1_products_api import router as conversion1_products_router
from app.web.routes import router as web_router
from app.services.metrics import bq_query_count_var, bq_time_ms_var, request_id_var, reset_request_metrics
from app.services.permission_service import ResolvedUserAccess, build_sidebar_groups, resolve_permissions_for_role

LOGGER = logging.getLogger(__name__)


app = FastAPI(title="Formulation Tracker")
templates = Jinja2Templates(directory="app/web/templates")
# Keep one in-memory role cache to reduce repeated per-request BigQuery lookups for active sessions.
_ROLE_CACHE_TTL_SECONDS = 30.0
# Store cached role rows keyed by lowercase email with expiration timestamps.
_role_cache: dict[str, tuple[float, Optional[dict]]] = {}
# Cache active-role count briefly to avoid repeated full-count queries on every request.
_active_role_count_cache: tuple[float, int] = (0.0, 0)


@app.on_event("startup")
def startup() -> None:
    settings = init_settings()
    bigquery, storage = init_services(settings)
    # Log startup BigQuery context so region misconfiguration can be diagnosed from Cloud Run logs quickly.
    LOGGER.info(
        "Starting app with BigQuery project=%s dataset=%s location=%s",
        settings.project_id,
        settings.dataset_id,
        settings.bq_location,
    )
    # Run idempotent startup migrations so required tables/views/counters exist before serving requests.
    bigquery.ensure_tables()
    app.state.settings = settings
    app.state.bigquery = bigquery
    app.state.storage = storage


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    # Stamp each request with a correlation id used across API and BigQuery timing logs.
    request_id = uuid4().hex[:12]
    request.state.request_id = request_id
    request_id_var.set(request_id)
    # Reset per-request timing counters before any endpoint or auth query executes.
    reset_request_metrics()
    # Capture high-level request start time for full API duration reporting.
    request_started_at = time.perf_counter()
    if request.url.path in {"/health", "/static/app.js", "/static/styles.css"}:
        # Skip auth and metric-rich logging for static assets and health probes.
        response = await call_next(request)
        return response

    try:
        # Parse auth context once in middleware so downstream routes and templates can reuse user metadata.
        auth_context = get_auth_context(request)
        # Store actor and email on request state for template rendering and audit logging consistency.
        request.state.actor = auth_context.email
        request.state.user_email = auth_context.email
        # Load the persisted user-role row so every downstream page and API can enforce the same permissions.
        role_record = _get_cached_role(request, auth_context.email)
        # Detect bootstrap mode so the first signed-in user can access the admin page and seed roles.
        is_bootstrap_admin = role_record is None and _get_cached_active_role_count(request) == 0
        # Promote only the bootstrap user to admin; otherwise preserve the explicit persisted role-group.
        role_group = "admin" if is_bootstrap_admin else (role_record or {}).get("role_group", "")
        # Resolve the role-group into the concrete named permission list used by route guards and filtering.
        permissions = frozenset(resolve_permissions_for_role(role_group))
        request.state.user_access = ResolvedUserAccess(
            role_record=role_record,
            role_group=role_group or "unassigned",
            permissions=permissions,
            is_admin=role_group == "admin",
            is_bootstrap_admin=is_bootstrap_admin,
        )
        # Precompute the sidebar model once so templates can render only the destinations this user may access.
        request.state.sidebar_groups = build_sidebar_groups(permissions, role_group == "admin")
    except ValueError as exc:
        # Ensure templates that render user boxes never crash when auth parsing fails.
        request.state.actor = None
        request.state.user_email = None
        request.state.user_access = ResolvedUserAccess(
            role_record=None,
            role_group="unassigned",
            permissions=frozenset(),
            is_admin=False,
            is_bootstrap_admin=False,
        )
        request.state.sidebar_groups = []
        return JSONResponse(status_code=401, content={"ok": False, "error": str(exc)})
    # Execute downstream request handling only after auth context has been resolved.
    response = await call_next(request)
    # Compute total end-to-end API time to compare against aggregated BigQuery query time.
    total_api_ms = (time.perf_counter() - request_started_at) * 1000.0
    # Read request payload size from Content-Length header when provided by clients.
    request_bytes = int(request.headers.get("content-length", "0") or 0)
    # Read response payload size from Content-Length header when present on response objects.
    response_bytes = int(response.headers.get("content-length", "0") or 0)
    # Retrieve accumulated BigQuery timing captured by instrumented query jobs.
    total_bq_ms = bq_time_ms_var.get()
    query_count = bq_query_count_var.get()
    # Emit one structured request log line for rapid bottleneck attribution by layer.
    LOGGER.info(
        "api.request request_id=%s method=%s path=%s status=%s total_ms=%.2f bq_ms=%.2f bq_query_count=%s request_bytes=%s response_bytes=%s",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        total_api_ms,
        total_bq_ms,
        query_count,
        request_bytes,
        response_bytes,
    )
    # Attach timing headers so browser devtools can isolate API and BigQuery contributions per request.
    response.headers["X-Request-Id"] = request_id
    response.headers["X-Api-Time-Ms"] = f"{total_api_ms:.2f}"
    response.headers["X-Bq-Time-Ms"] = f"{total_bq_ms:.2f}"
    response.headers["X-Bq-Query-Count"] = str(query_count)
    return response


def _get_cached_role(request: Request, email: str) -> Optional[dict]:
    # Reuse recent role lookups to remove one BigQuery query from most authenticated requests.
    cache_key = (email or "").strip().lower()
    now = time.monotonic()
    cached = _role_cache.get(cache_key)
    # Return cached role rows while the short TTL remains valid.
    if cached and cached[0] > now:
        return cached[1]
    # Refresh role data from BigQuery when cache is missing or expired.
    role_record = request.app.state.bigquery.get_user_role(email)
    _role_cache[cache_key] = (now + _ROLE_CACHE_TTL_SECONDS, role_record)
    return role_record


def _get_cached_active_role_count(request: Request) -> int:
    # Cache active-role totals briefly because bootstrap checks run on every authenticated request.
    now = time.monotonic()
    global _active_role_count_cache
    expires_at, cached_count = _active_role_count_cache
    # Return cached count while still fresh to avoid repeated COUNT queries.
    if expires_at > now:
        return cached_count
    # Refresh the active role count from BigQuery once cache expires.
    latest_count = request.app.state.bigquery.count_active_user_roles()
    _active_role_count_cache = (now + _ROLE_CACHE_TTL_SECONDS, latest_count)
    return latest_count


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # Render a dedicated forbidden page for browser requests blocked by permission guards.
    if exc.status_code == 403 and "text/html" in (request.headers.get("accept") or "").lower():
        return templates.TemplateResponse(
            "forbidden.html",
            {
                "request": request,
                "title": "Forbidden",
            },
            status_code=403,
        )
    # Preserve JSON-style HTTPException responses for API calls and non-browser clients.
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

app.include_router(ingredients_router)
app.include_router(batches_router)
app.include_router(sets_router)
app.include_router(weights_router)
app.include_router(batch_variants_router)
app.include_router(formulations_router)
app.include_router(location_codes_router)
app.include_router(compounding_how_router)
app.include_router(pellet_bags_router)
app.include_router(pellet_bag_status_router)
app.include_router(conversion1_products_router)
app.include_router(web_router)

app.mount("/static", StaticFiles(directory="app/web/static"), name="static")
