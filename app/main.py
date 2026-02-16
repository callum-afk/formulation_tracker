from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
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
from app.web.routes import router as web_router

app = FastAPI(title="Formulation Tracker")


@app.on_event("startup")
def startup() -> None:
    settings = init_settings()
    bigquery, storage = init_services(settings)
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
def root():
    return {"status": "ok", "service": "formulation-tracker"}


@app.get("/formulations")
def formulations() -> dict:
    return {
        "ok": True,
        "items": [
            {
                "set_code": "AB",
                "weight_code": "AB",
                "batch_variant_code": "AB",
                "notes": "Stub formulation entry",
            }
        ],
    }

app.include_router(ingredients_router)
app.include_router(batches_router)
app.include_router(sets_router)
app.include_router(weights_router)
app.include_router(batch_variants_router)
app.include_router(formulations_router)
app.include_router(location_codes_router)
app.include_router(web_router)

app.mount("/static", StaticFiles(directory="app/web/static"), name="static")
