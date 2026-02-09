from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.dependencies import get_bigquery
from app.services.bigquery_service import BigQueryService

router = APIRouter()

templates = Jinja2Templates(directory="app/web/templates")


@router.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("base.html", {"request": request, "title": "Formulation Tracker"})


@router.get("/ingredients", response_class=HTMLResponse)
async def ingredients(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    items = bigquery.list_ingredients({})
    return templates.TemplateResponse(
        "ingredients.html",
        {"request": request, "title": "Ingredients", "items": items},
    )


@router.get("/ingredient_import", response_class=HTMLResponse)
async def ingredient_import(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "ingredient_import.html",
        {"request": request, "title": "Ingredient Import"},
    )


@router.get("/batches", response_class=HTMLResponse)
async def batches(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    items = bigquery.list_ingredients({})
    return templates.TemplateResponse(
        "batches.html",
        {"request": request, "title": "Batches", "items": items},
    )


@router.get("/sets", response_class=HTMLResponse)
async def sets(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    sets_data = bigquery.list_sets()
    items = bigquery.list_ingredients({})
    return templates.TemplateResponse(
        "sets.html",
        {"request": request, "title": "Ingredient Sets", "sets": sets_data, "items": items},
    )


@router.get("/dry_weights", response_class=HTMLResponse)
async def dry_weights(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "dry_weights.html",
        {"request": request, "title": "Dry Weight Variants"},
    )


@router.get("/batch_selection", response_class=HTMLResponse)
async def batch_selection(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "batch_selection.html",
        {"request": request, "title": "Batch Selection"},
    )


@router.get("/formulations/ui", response_class=HTMLResponse)
async def formulations(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "formulations.html",
        {"request": request, "title": "Formulations"},
    )
