from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.dependencies import get_bigquery, get_settings, get_storage
from app.services.bigquery_service import BigQueryService
from app.services.storage_service import StorageService

router = APIRouter()

templates = Jinja2Templates(directory="app/web/templates")


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Build dashboard sections for each meaningful pellet bag status stream.
    status_sections = [
        {"title": "Long Moisture", "column": "long_moisture_status", "items": bigquery.list_pellet_bags_with_meaningful_status("long_moisture_status")},
        {"title": "Density", "column": "density_status", "items": bigquery.list_pellet_bags_with_meaningful_status("density_status")},
        {"title": "Injection Moulding Status", "column": "injection_moulding_status", "items": bigquery.list_pellet_bags_with_meaningful_status("injection_moulding_status")},
        {"title": "Film Forming Status", "column": "film_forming_status", "items": bigquery.list_pellet_bags_with_meaningful_status("film_forming_status")},
        {"title": "QC Team View", "column": "qc_status", "items": bigquery.list_pellet_bags_with_meaningful_status("qc_status")},
    ]
    # Render the dashboard at root so Cloud Run domain root lands on operational status panels.
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "title": "Dashboard", "status_sections": status_sections},
    )


@router.get("/about", response_class=HTMLResponse)
async def about(request: Request, q: str | None = None, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Preserve previous landing page behaviour under a dedicated informational route.
    filters = {"q": q} if q else {}
    items = bigquery.list_ingredients(filters)
    return templates.TemplateResponse(
        "ingredients.html",
        {"request": request, "title": "Ingredient SKUs", "items": items, "q": q or ""},
    )


@router.get("/ingredients", response_class=HTMLResponse)
async def ingredients(request: Request, q: str | None = None, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    filters = {"q": q} if q else {}
    items = bigquery.list_ingredients(filters)
    return templates.TemplateResponse(
        "ingredients.html",
        {"request": request, "title": "Ingredient SKUs", "items": items, "q": q or ""},
    )


@router.get("/ingredient_import", response_class=HTMLResponse)
async def ingredient_import(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "ingredient_import.html",
        {"request": request, "title": "Ingredient Import"},
    )


@router.get("/utilities", response_class=HTMLResponse)
async def utilities(request: Request) -> HTMLResponse:
    # Serve utility workflows (SKU import and partner-code creation) on a single page.
    return templates.TemplateResponse(
        "utilities.html",
        {"request": request, "title": "Utilities"},
    )


@router.get("/batches", response_class=HTMLResponse)
async def batches(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    items = bigquery.list_ingredients({})
    return templates.TemplateResponse(
        "batches.html",
        {"request": request, "title": "Ingredient Batches", "items": items},
    )


@router.get("/batches/{sku}/{batch_code}", response_class=HTMLResponse)
async def batch_detail(sku: str, batch_code: str, request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Retrieve full batch details for the selected SKU + batch code pair.
    batch = bigquery.get_batch(sku, batch_code)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    return templates.TemplateResponse(
        "batch_detail.html",
        {
            "request": request,
            "title": f"Batch {batch_code}",
            "sku": sku,
            "batch_code": batch_code,
            "batch": batch,
        },
    )


@router.get("/sets", response_class=HTMLResponse)
async def sets(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Load ingredient options for set creation; existing set rows are fetched client-side with pagination.
    items = bigquery.list_ingredients({})
    return templates.TemplateResponse(
        "sets.html",
        {"request": request, "title": "Formulation Sets", "items": items},
    )


@router.get("/dry_weights", response_class=HTMLResponse)
async def dry_weights(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "dry_weights.html",
        {"request": request, "title": "Dry Weights"},
    )


@router.get("/batch_selection", response_class=HTMLResponse)
async def batch_selection(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "batch_selection.html",
        {"request": request, "title": "Batch Selection"},
    )



@router.get("/location_codes", response_class=HTMLResponse)
async def location_codes(request: Request) -> HTMLResponse:
    # Serve the location-ID workflow page for production partner/date code generation and partner management.
    return templates.TemplateResponse(
        "location_codes.html",
        {"request": request, "title": "Machine"},
    )




@router.get("/compounding_how", response_class=HTMLResponse)
async def compounding_how(request: Request) -> HTMLResponse:
    # Serve compounding-how creation and edit workflow page.
    return templates.TemplateResponse(
        "compounding_how.html",
        {"request": request, "title": "How"},
    )

@router.get("/pellet-bags/status/{status_column}", response_class=HTMLResponse)
async def pellet_bag_status_list(status_column: str, request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Render full status list pages used by dashboard view all links.
    items = bigquery.list_pellet_bags_with_meaningful_status(status_column, limit=500)
    return templates.TemplateResponse(
        "pellet_bag_status_list.html",
        {"request": request, "title": "Pellet Bag Status List", "status_column": status_column, "items": items},
    )


@router.get("/pellet_bags", response_class=HTMLResponse)
async def pellet_bags(request: Request) -> HTMLResponse:
    # Serve pellet bag code minting and management workflow page.
    return templates.TemplateResponse(
        "pellet_bags.html",
        {"request": request, "title": "Pellet Bags"},
    )


@router.get("/ingredients/{sku}/edit", response_class=HTMLResponse)
async def ingredient_edit(sku: str, request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    ingredient = bigquery.get_ingredient(sku)
    if not ingredient:
        raise HTTPException(status_code=404, detail="Ingredient not found")
    return templates.TemplateResponse(
        "ingredient_edit.html",
        {"request": request, "title": f"Edit {sku}", "ingredient": ingredient},
    )


@router.get("/ingredients/{sku}", response_class=HTMLResponse)
async def sku_detail(sku: str, request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Render a SKU summary page with linked formulation and pellet bag context.
    summary = bigquery.get_sku_summary(sku)
    if not summary.get("ingredient"):
        raise HTTPException(status_code=404, detail="Ingredient not found")
    return templates.TemplateResponse(
        "sku_detail.html",
        {"request": request, "title": f"SKU {sku}", "sku": sku, "summary": summary},
    )


@router.get("/pellet-bags/{pellet_bag_code}", response_class=HTMLResponse)
async def pellet_bag_detail(pellet_bag_code: str, request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Render one pellet bag detail page with all known fields and compounding context.
    detail = bigquery.get_pellet_bag_detail(pellet_bag_code)
    if not detail:
        raise HTTPException(status_code=404, detail="Pellet bag not found")
    return templates.TemplateResponse(
        "pellet_bag_detail.html",
        {"request": request, "title": f"Pellet bag {pellet_bag_code}", "detail": detail},
    )


@router.get("/ingredients/{sku}/msds")
async def ingredient_msds_download(
    sku: str,
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
):
    ingredient = bigquery.get_ingredient(sku)
    if not ingredient or not ingredient.get("msds_object_path"):
        raise HTTPException(status_code=404, detail="MSDS not found")
    url = storage.generate_download_url(settings.bucket_msds, ingredient["msds_object_path"], ttl_minutes=10)
    return RedirectResponse(url=url, status_code=302)
