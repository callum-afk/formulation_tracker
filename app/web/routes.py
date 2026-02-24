from __future__ import annotations

from datetime import date, datetime
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.dependencies import get_bigquery, get_settings, get_storage
from app.constants import DEFAULT_PAGE_SIZE, FAILURE_MODES, MAX_PAGE_SIZE
from app.services.bigquery_service import BigQueryService
from app.services.storage_service import StorageService

router = APIRouter()

templates = Jinja2Templates(directory="app/web/templates")


def _to_json_safe(value):
    # Recursively convert nested values (including datetimes/dates/Decimals) into JSON-safe primitives for Jinja tojson.
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    # BigQuery NUMERIC/BIGNUMERIC often comes back as Decimal
    try:
        from decimal import Decimal
        if isinstance(value, Decimal):
            # Use float for easy JS use; switch to str(value) if you need exact precision.
            return float(value)
    except Exception:
        pass
    # Convert bytes (rare) to text
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(key): _to_json_safe(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_safe(item) for item in value]
    return value


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Build dashboard sections grouped into quality control and processing workstreams.
    status_sections = [
        {
            "title": "Quality Control",
            "items": [
                {"title": "Long Moisture Status", "column": "long_moisture_status", "items": bigquery.list_pellet_bags_with_meaningful_status("long_moisture_status")},
                {"title": "Density Status", "column": "density_status", "items": bigquery.list_pellet_bags_with_meaningful_status("density_status")},
            ],
        },
        {
            "title": "Processing",
            "items": [
                {"title": "Injection Moulding Status", "column": "injection_moulding_status", "items": bigquery.list_pellet_bags_with_meaningful_status("injection_moulding_status")},
                {"title": "Film Forming Status", "column": "film_forming_status", "items": bigquery.list_pellet_bags_with_meaningful_status("film_forming_status")},
            ],
        },
    ]
    # Render the dashboard at root so Cloud Run domain root lands on operational status panels.
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "title": "Dashboard", "status_sections": status_sections, "dashboard_stats": bigquery.get_dashboard_stats()},
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

@router.get("/conversion1/context", response_class=HTMLResponse)
async def conversion1_context_page(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Render Conversion 1 Context form and machine/partner options sourced from mixing partner records.
    return templates.TemplateResponse(
        "conversion1_context.html",
        {
            "request": request,
            "title": "Conversion 1 Context",
            "options": bigquery.get_mixing_partner_machine_options(),
            "errors": [],
            "result": None,
            "form_data": {},
        },
    )


@router.post("/conversion1/context", response_class=HTMLResponse)
async def conversion1_context_submit(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Parse and normalize incoming form values before validating required conversion context fields.
    form = await request.form()
    pellet_code = " ".join(str(form.get("pellet_code", "")).strip().split())
    partner_code = str(form.get("partner_code", "")).strip().upper()
    machine_code = str(form.get("machine_code", "")).strip()
    date_yymmdd = str(form.get("date_yymmdd", "")).strip()
    errors: list[str] = []
    # Validate pellet code shape by requiring at least six space-separated tokens.
    if len(pellet_code.split()) < 6:
        errors.append("Pellet Code must contain multiple tokens (e.g. AB AC AC AD 240910 AF PR 0015).")
    # Enforce partner code as two letters to match existing code vocabulary.
    if len(partner_code) != 2 or not partner_code.isalpha():
        errors.append("Partner Code must be exactly two letters.")
    # Require machine code selection so context metadata remains complete.
    if not machine_code:
        errors.append("Machine is required.")
    # Validate YYMMDD date token format before context code generation.
    if len(date_yymmdd) != 6 or not date_yymmdd.isdigit():
        errors.append("Date must be exactly 6 digits in YYMMDD format.")
    result = None
    if not errors:
        # Persist deterministic context code row and surface generated context code for copy actions.
        result = bigquery.create_or_get_conversion1_context(
            pellet_code=pellet_code,
            partner_code=partner_code,
            machine_code=machine_code,
            date_yymmdd=date_yymmdd,
            user_email=request.state.user_email,
        )
    return templates.TemplateResponse(
        "conversion1_context.html",
        {
            "request": request,
            "title": "Conversion 1 Context",
            "options": bigquery.get_mixing_partner_machine_options(),
            "errors": errors,
            "result": _to_json_safe(result) if result else None,
            "form_data": {
                "pellet_code": pellet_code,
                "partner_code": partner_code,
                "machine_code": machine_code,
                "date_yymmdd": date_yymmdd,
            },
        },
        status_code=400 if errors else 200,
    )


@router.get("/conversion1/how", response_class=HTMLResponse)
async def conversion1_how_page(
    request: Request,
    context_code: str | None = None,
    process_id: str | None = None,
    failure_mode: str | None = None,
    search: str | None = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> HTMLResponse:
    # Clamp pagination values to keep list queries within global safety bounds.
    safe_page_size = max(1, min(page_size, MAX_PAGE_SIZE))
    rows, total = bigquery.list_conversion1_how(
        context_code=(context_code or "").strip() or None,
        process_id=(process_id or "").strip() or None,
        failure_mode=(failure_mode or "").strip() or None,
        search=(search or "").strip() or None,
        page=max(1, page),
        page_size=safe_page_size,
    )
    return templates.TemplateResponse(
        "conversion1_how.html",
        {
            "request": request,
            "title": "Conversion 1 How",
            "errors": [],
            "result": None,
            "rows": _to_json_safe(rows),
            "failure_modes": FAILURE_MODES,
            "filters": {
                "context_code": context_code or "",
                "process_id": process_id or "",
                "failure_mode": failure_mode or "",
                "search": search or "",
            },
            "pagination": {
                "page": max(1, page),
                "page_size": safe_page_size,
                "total": total,
                "has_prev": page > 1,
                "has_next": page * safe_page_size < total,
            },
            "form_data": {},
        },
    )


@router.post("/conversion1/how", response_class=HTMLResponse)
async def conversion1_how_submit(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Parse and normalize conversion-how form fields for validation and persistence.
    form = await request.form()
    context_code = " ".join(str(form.get("context_code", "")).strip().split())
    notes = str(form.get("notes", "")).strip() or None
    failure_mode = str(form.get("failure_mode", "")).strip() or None
    setup_link = str(form.get("setup_link", "")).strip() or None
    processed_data_link = str(form.get("processed_data_link", "")).strip() or None
    errors: list[str] = []
    # Require context code input to generate deterministic conversion-how identifiers.
    if not context_code:
        errors.append("Context Code is required.")
    # Require that provided context code exists before minting a process code.
    if context_code and not bigquery.get_conversion1_context(context_code):
        errors.append("Context Code was not found. Please generate it on Conversion 1 Context first.")
    # Validate selected failure mode against approved controlled vocabulary.
    if failure_mode and failure_mode not in FAILURE_MODES:
        errors.append("Invalid failure mode.")
    result = None
    if not errors:
        # Persist conversion-how row and return generated How code + process metadata.
        result = bigquery.create_or_update_conversion1_how(
            context_code=context_code,
            notes=notes,
            failure_mode=failure_mode,
            setup_link=setup_link,
            processed_data_link=processed_data_link,
            user_email=request.state.user_email,
        )
    rows, total = bigquery.list_conversion1_how(
        context_code=None,
        process_id=None,
        failure_mode=None,
        search=None,
        page=1,
        page_size=DEFAULT_PAGE_SIZE,
    )
    return templates.TemplateResponse(
        "conversion1_how.html",
        {
            "request": request,
            "title": "Conversion 1 How",
            "errors": errors,
            "result": _to_json_safe(result) if result else None,
            "rows": _to_json_safe(rows),
            "failure_modes": FAILURE_MODES,
            "filters": {"context_code": "", "process_id": "", "failure_mode": "", "search": ""},
            "pagination": {
                "page": 1,
                "page_size": DEFAULT_PAGE_SIZE,
                "total": total,
                "has_prev": False,
                "has_next": DEFAULT_PAGE_SIZE < total,
            },
            "form_data": {
                "context_code": context_code,
                "notes": notes or "",
                "failure_mode": failure_mode or "",
                "setup_link": setup_link or "",
                "processed_data_link": processed_data_link or "",
            },
        },
        status_code=400 if errors else 200,
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
async def pellet_bags(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Serve pellet bag code minting and management workflow page.
    return templates.TemplateResponse(
        "pellet_bags.html",
        {"request": request, "title": "Pellet Bags", "compounding_how_codes": bigquery.list_compounding_how_codes()},
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
        {"request": request, "title": f"Pellet bag {pellet_bag_code}", "detail": _to_json_safe(detail)},
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
