from __future__ import annotations

from datetime import date, datetime
from urllib.parse import urlparse
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.dependencies import get_bigquery, get_settings, get_storage
from app.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.services.bigquery_service import BigQueryService
from app.services.storage_service import StorageService

router = APIRouter()

templates = Jinja2Templates(directory="app/web/templates")




def _is_google_drive_url(url: str) -> bool:
    # Restrict external links to common Google Drive/Docs hostnames expected for machine/process files.
    if not url:
        return True
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    allowed_hosts = {"drive.google.com", "docs.google.com", "sheets.google.com"}
    return host in allowed_hosts or host.endswith(".google.com")

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
async def conversion1_context_page(
    request: Request,
    q: str | None = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> HTMLResponse:
    # Clamp pagination arguments so table browsing remains within safe global bounds.
    safe_page_size = max(1, min(page_size, MAX_PAGE_SIZE))
    safe_page = max(1, page)
    # Load conversion-code rows for the Context page table and apply optional text filtering.
    rows, total = bigquery.list_conversion1_codes_paginated(
        search=(q or "").strip() or None,
        page=safe_page,
        page_size=safe_page_size,
    )
    # Fetch active pellet codes for the dropdown and partner-machine options for conversion partner selection.
    pellet_codes = bigquery.list_pellet_bag_codes()
    raw_options = bigquery.get_mixing_partner_machine_options()
    # Normalize partner-machine rows so one select option resolves to one deterministic partner + machine pair.
    partner_machine_options = [
        {
            "key": f"{(option.get('partner_code') or '').strip()}||{(option.get('machine_code') or '').strip()}",
            "partner_code": (option.get("partner_code") or "").strip(),
            "machine_code": (option.get("machine_code") or "").strip(),
            "label": f"{(option.get('partner_name') or (option.get('partner_code') or '')).strip()} - {(option.get('machine_code') or '').strip()}",
        }
        for option in raw_options
    ]
    # Keep only valid complete options so the UI never renders partially configured rows.
    partner_machine_options = [row for row in partner_machine_options if row["partner_code"] and row["machine_code"]]
    return templates.TemplateResponse(
        "conversion1_context.html",
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
        },
    )


@router.post("/conversion1/context", response_class=HTMLResponse)
async def conversion1_context_submit(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Parse and normalize Context form fields while preserving both dropdown and manual pellet entry.
    form = await request.form()
    pellet_code_select = " ".join(str(form.get("pellet_code_select", "")).strip().split())
    pellet_code_manual = " ".join(str(form.get("pellet_code_manual", "")).strip().split())
    conversion_partner_key = str(form.get("conversion_partner_key", "")).strip()
    production_date = str(form.get("production_date", "")).strip()
    pellet_code = pellet_code_manual or pellet_code_select
    errors: list[str] = []
    # Fetch reference data used by both validation and dropdown rendering.
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
    # Keep only valid options and build a key map for fast lookup.
    partner_machine_options = [row for row in partner_machine_options if row["partner_code"] and row["machine_code"]]
    option_by_key = {row["key"]: row for row in partner_machine_options}
    # Validate pellet code presence and existence.
    if not pellet_code:
        errors.append("Pellet Code is required.")
    elif pellet_code not in set(pellet_codes):
        errors.append("Pellet Code was not found. Please use a valid existing pellet code.")
    # Validate selected partner-machine option.
    selected_option = option_by_key.get(conversion_partner_key)
    if not selected_option:
        errors.append("Conversion partner must match an existing option.")
    # Validate date and derive YYMMDD token used by the generated code.
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
        # Ensure deterministic context exists before minting the final Conversion ID row.
        context = bigquery.create_or_get_conversion1_context(
            pellet_code=pellet_code,
            partner_code=selected_option["partner_code"],
            machine_code=selected_option["machine_code"],
            date_yymmdd=date_yymmdd,
            user_email=request.state.user_email,
        )
        # Return the generated Context code directly because Conversion 1 How is intentionally removed.
        result = {"conversion_code": str(context.get("context_code") or "")}
    # Reload first page after submission so newest entry appears immediately with any validation feedback.
    rows, total = bigquery.list_conversion1_codes_paginated(
        search=None,
        page=1,
        page_size=DEFAULT_PAGE_SIZE,
    )
    return templates.TemplateResponse(
        "conversion1_context.html",
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
                "production_date": production_date,
            },
            "pellet_codes": pellet_codes,
            "partner_machine_options": partner_machine_options,
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
    # Clamp pagination arguments so browsing remains bounded and consistent with global limits.
    safe_page_size = max(1, min(page_size, MAX_PAGE_SIZE))
    safe_page = max(1, page)
    # Load newest Conversion 1 How entries and optional filter state for table rendering.
    rows, total = bigquery.list_conversion1_how_entries(search=(q or "").strip() or None, page=safe_page, page_size=safe_page_size)
    # Load active context codes so users can either select an existing code or paste one manually.
    context_rows, _ = bigquery.list_conversion1_codes_paginated(search=None, page=1, page_size=MAX_PAGE_SIZE)
    context_codes = [str(row.get("conversion_code") or "").strip() for row in context_rows if str(row.get("conversion_code") or "").strip()]
    return templates.TemplateResponse(
        "conversion1_how.html",
        {
            "request": request,
            "title": "Conversion 1",
            "section_header": "How",
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
            "context_codes": context_codes,
            "failure_modes": bigquery.get_failure_modes(),
        },
    )


@router.post("/conversion1/how", response_class=HTMLResponse)
async def conversion1_how_submit(request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> HTMLResponse:
    # Parse and normalize form values including both context-code input variants.
    form = await request.form()
    context_code_select = " ".join(str(form.get("context_code_select", "")).strip().split())
    context_code_manual = " ".join(str(form.get("context_code_manual", "")).strip().split())
    process_code = str(form.get("process_code", "")).strip().upper()
    failure_mode = str(form.get("failure_mode", "")).strip()
    machine_setup_url = str(form.get("machine_setup_url", "")).strip()
    processed_data_url = str(form.get("processed_data_url", "")).strip()
    submit_action = str(form.get("submit_action", "generate")).strip().lower()

    # Resolve one canonical context code from manual text first, then dropdown fallback.
    context_code = context_code_manual or context_code_select
    errors: list[str] = []
    # Re-hydrate dropdown options and table state for full-page re-render in both success and error flows.
    context_rows, _ = bigquery.list_conversion1_codes_paginated(search=None, page=1, page_size=MAX_PAGE_SIZE)
    context_codes = [str(row.get("conversion_code") or "").strip() for row in context_rows if str(row.get("conversion_code") or "").strip()]
    failure_modes = bigquery.get_failure_modes()

    # Validate required context code and ensure the full code already exists in Conversion 1 Context storage.
    if not context_code:
        errors.append("Context code is required (dropdown or text entry).")
    elif not bigquery.conversion1_context_exists(context_code):
        errors.append("Context code was not found. Please use an existing full context code.")

    # Validate process code format when provided manually.
    if process_code and (len(process_code) != 2 or not process_code.isalpha()):
        errors.append("Process code must be a two-letter code like AB.")

    # Validate failure mode selection against the same shared canonical source as pellet-bag workflows.
    if failure_mode and failure_mode not in failure_modes:
        errors.append("Failure mode must be selected from the allowed list.")

    # Validate external links as Google Drive/Docs URLs only.
    if machine_setup_url and not _is_google_drive_url(machine_setup_url):
        errors.append("Machine Setup File URL must be a valid Google Drive/Docs link.")
    if processed_data_url and not _is_google_drive_url(processed_data_url):
        errors.append("Processed Data File URL must be a valid Google Drive/Docs link.")

    # Determine generated process code from persisted rows only so repeated Generate clicks stay stable until save.
    generated_process_code = process_code or bigquery.get_next_conversion1_how_process_code(start_code="AB")
    # Build processing/how code exactly as context code + one space + process code.
    generated_how_code = f"{context_code} {generated_process_code}".strip() if context_code else ""

    # Save action requires failure mode and generated code, then upserts to BigQuery.
    if submit_action == "save" and not errors:
        if not failure_mode:
            errors.append("Failure mode is required to save Conversion How.")
        if not generated_how_code:
            errors.append("Conversion 1 How code could not be generated.")

    if submit_action == "save" and not errors:
        # Persist Conversion 1 How row with deterministic merge semantics and signed-in owner metadata.
        bigquery.create_or_update_conversion1_how(
            {
                "conversion1_how_code": generated_how_code,
                "context_code": context_code,
                "process_code": generated_process_code,
                "processing_code": generated_how_code,
                "failure_mode": failure_mode,
                "machine_setup_url": machine_setup_url or None,
                "processed_data_url": processed_data_url or None,
                "created_by": request.state.user_email,
            }
        )

    # Reload first page after submit so newest save appears immediately and pagination resets predictably.
    rows, total = bigquery.list_conversion1_how_entries(search=None, page=1, page_size=DEFAULT_PAGE_SIZE)
    status_code = 400 if errors else 200
    return templates.TemplateResponse(
        "conversion1_how.html",
        {
            "request": request,
            "title": "Conversion 1",
            "section_header": "How",
            "errors": errors,
            "result": {
                "conversion1_how_code": generated_how_code,
                "process_code": generated_process_code,
            } if generated_how_code else None,
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
                "context_code_select": context_code_select,
                "context_code_manual": context_code_manual,
                "process_code": generated_process_code,
                "failure_mode": failure_mode,
                "machine_setup_url": machine_setup_url,
                "processed_data_url": processed_data_url,
            },
            "context_codes": context_codes,
            "failure_modes": failure_modes,
        },
        status_code=status_code,
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
