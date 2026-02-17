from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

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
from app.web.routes import router as web_router

LOGGER = logging.getLogger(__name__)


app = FastAPI(title="Formulation Tracker")


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
    if request.url.path in {"/health", "/static/app.js", "/static/styles.css"}:
        return await call_next(request)

    try:
        get_auth_context(request)
    except ValueError as exc:
        return JSONResponse(status_code=401, content={"ok": False, "error": str(exc)})

    return await call_next(request)


@app.get("/health")
async def health() -> dict:
    return {"ok": True}

@app.get("/")
def root() -> RedirectResponse:
    # Redirect legacy root requests to the ingredients UI route to keep browser entry consistent.
    return RedirectResponse(url="/ingredients", status_code=307)

app.include_router(ingredients_router)
app.include_router(batches_router)
app.include_router(sets_router)
app.include_router(weights_router)
app.include_router(batch_variants_router)
app.include_router(formulations_router)
app.include_router(location_codes_router)
app.include_router(compounding_how_router)
app.include_router(web_router)

app.mount("/static", StaticFiles(directory="app/web/static"), name="static")
