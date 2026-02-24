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
    q: str | None = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> HTMLResponse:
    # Clamp pagination values to keep list queries within global safety bounds.
    safe_page_size = max(1, min(page_size, MAX_PAGE_SIZE))
    safe_page = max(1, page)
    # Load conversion-code rows using optional text filtering on the generated conversion code.
    rows, total = bigquery.list_conversion1_codes_paginated(
        search=(q or "").strip() or None,
        page=safe_page,
        page_size=safe_page_size,
    )
    # Fetch active pellet bag IDs for dropdown + manual validation.
    pellet_codes = bigquery.list_pellet_bag_codes()
    # Fetch partner-machine options from the same source used by Mixing Machine page.
    raw_options = bigquery.get_mixing_partner_machine_options()
    # Build normalized option payload where each select value maps to one partner + machine combination.
    partner_machine_options = [
        {
            "key": f"{(option.get('partner_code') or '').strip()}||{(option.get('machine_code') or '').strip()}",
            "partner_code": (option.get("partner_code") or "").strip(),
            "machine_code": (option.get("machine_code") or "").strip(),
            "label": f"{(option.get('partner_name') or (option.get('partner_code') or '')).strip()} - {(option.get('machine_code') or '').strip()}",
        }
        for option in raw_options
    ]
    # Keep only complete options to avoid presenting invalid rows in the selector.
    partner_machine_options = [row for row in partner_machine_options if row["partner_code"] and row["machine_code"]]
    return templates.TemplateResponse(
        "conversion1_how.html",
        {
            "request": request,
            "title": "Conversion 1",
            "errors": [],
            "result": None,
            "rows": _to_json_safe(rows),
            "filters": {"q": q or ""},
            "pagination": {
                "page": safe_page,
                "page_size": safe_page_size,
                "total": total,
                "has_prev": safe_page > 1,
                "has_next": safe_page * safe_page_size < total,
            },
            "form_data": {},
            "pellet_codes": pellet_codes,
            "partner_machine_options": partner_machine_options,
            "partner_machine_labels": sorted({row["label"] for row in partner_machine_options}),
        },
    )


@router.post("/conversion1/how", response_class=HTMLResponse)
async def conversion1_how_submit(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Parse and normalize Conversion 1 create-form inputs.
    form = await request.form()
    pellet_code_select = " ".join(str(form.get("pellet_code_select", "")).strip().split())
    pellet_code_manual = " ".join(str(form.get("pellet_code_manual", "")).strip().split())
    conversion_partner_key = str(form.get("conversion_partner_key", "")).strip()
    conversion_partner_manual = " ".join(str(form.get("conversion_partner_manual", "")).strip().split())
    production_date = str(form.get("production_date", "")).strip()
    # Prefer manual pellet entry when present; otherwise use dropdown selection.
    pellet_code = pellet_code_manual or pellet_code_select
    errors: list[str] = []
    # Load valid option sets once so dropdown and manual entries both validate against persisted data.
    pellet_codes = bigquery.list_pellet_bag_codes()
    raw_options = bigquery.get_mixing_partner_machine_options()
    partner_machine_options = [
        {
            "key": f"{(option.get('partner_code') or '').strip()}||{(option.get('machine_code') or '').strip()}",
            "partner_code": (option.get("partner_code") or "").strip(),
            "machine_code": (option.get("machine_code") or "").strip(),
            "label": f"{(option.get('partner_name') or (option.get('partner_code') or '')).strip()} - {(option.get('machine_code') or '').strip()}",
        }
        for option in raw_options
    ]
    # Keep only complete options so validation and dropdown use the same clean source.
    partner_machine_options = [row for row in partner_machine_options if row["partner_code"] and row["machine_code"]]
    option_by_key = {row["key"]: row for row in partner_machine_options}
    option_by_label = {row["label"]: row for row in partner_machine_options}
    # Enforce pellet code presence and existence in the active pellet bag list.
    if not pellet_code:
        errors.append("Pellet ID is required.")
    elif pellet_code not in set(pellet_codes):
        errors.append("Pellet ID was not found. Please use a valid existing pellet ID.")
    # Resolve partner-machine either from dropdown key or manual BF-style text and ensure it exists.
    selected_option = option_by_key.get(conversion_partner_key)
    if not selected_option and conversion_partner_manual:
        selected_option = option_by_label.get(conversion_partner_manual)
    if not selected_option:
        errors.append("Conversion Partner must match an existing partner-machine option.")
    # Convert calendar date input into YYMMDD token required by persisted code format.
    date_yymmdd = ""
    if not production_date:
        errors.append("Date of production is required.")
    else:
        try:
            date_yymmdd = datetime.strptime(production_date, "%Y-%m-%d").strftime("%y%m%d")
        except ValueError:
            errors.append("Date of production must be a valid calendar date.")
    result = None
    if not errors and selected_option:
        # Ensure deterministic context exists before minting the next conversion code suffix.
        context = bigquery.create_or_get_conversion1_context(
            pellet_code=pellet_code,
            partner_code=selected_option["partner_code"],
            machine_code=selected_option["machine_code"],
            date_yymmdd=date_yymmdd,
            user_email=request.state.user_email,
        )
        # Persist generated conversion code row using the same code generator as existing Conversion 1 flow.
        result = bigquery.create_or_update_conversion1_how(
            context_code=str(context.get("context_code") or ""),
            notes=None,
            failure_mode=None,
            setup_link=None,
            processed_data_link=None,
            user_email=request.state.user_email,
        )
    # Reload first table page after create attempt so users see newest rows and validation feedback together.
    rows, total = bigquery.list_conversion1_codes_paginated(
        search=None,
        page=1,
        page_size=DEFAULT_PAGE_SIZE,
    )
    return templates.TemplateResponse(
        "conversion1_how.html",
        {
            "request": request,
            "title": "Conversion 1",
            "errors": errors,
            "result": _to_json_safe(result) if result else None,
            "rows": _to_json_safe(rows),
            "filters": {"q": ""},
            "pagination": {
                "page": 1,
                "page_size": DEFAULT_PAGE_SIZE,
                "total": total,
                "has_prev": False,
                "has_next": DEFAULT_PAGE_SIZE < total,
            },
            "form_data": {
                "pellet_code_select": pellet_code_select,
                "pellet_code_manual": pellet_code_manual,
                "conversion_partner_key": conversion_partner_key,
                "conversion_partner_manual": conversion_partner_manual,
                "production_date": production_date,
            },
            "pellet_codes": pellet_codes,
            "partner_machine_options": partner_machine_options,
            "partner_machine_labels": sorted(option_by_label.keys()),
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
