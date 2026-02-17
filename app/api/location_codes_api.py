from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from google.api_core.exceptions import NotFound

from app.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.dependencies import get_actor, get_bigquery
from app.models import ApiResponse, LocationCodeCreate, LocationPartnerCreate
from app.services.bigquery_service import BigQueryService
from app.services.codegen_service import int_to_code

router = APIRouter(prefix="/api/location_codes", tags=["location_codes"])

# Seeded partner data shown in the requested dropdown order so users can select by partner name.
DEFAULT_LOCATION_PARTNERS = [
    {"partner_name": "Unknown", "partner_code": "AA", "machine_specification": ""},
    {"partner_name": "Notpla - Machine Unknown", "partner_code": "AB", "machine_specification": ""},
    {"partner_name": "Broadway", "partner_code": "AC", "machine_specification": ""},
    {"partner_name": "Polytechs", "partner_code": "AD", "machine_specification": ""},
    {"partner_name": "PES", "partner_code": "AE", "machine_specification": ""},
    {"partner_name": "Poloplast", "partner_code": "AF", "machine_specification": ""},
    {"partner_name": "Aimplas", "partner_code": "AG", "machine_specification": ""},
    {"partner_name": "Plastribution", "partner_code": "AH", "machine_specification": ""},
    {"partner_name": "Vegeplast", "partner_code": "AI", "machine_specification": ""},
    {"partner_name": "Viscofan - Cast", "partner_code": "AJ", "machine_specification": ""},
    {"partner_name": "Viscofan - Extruded", "partner_code": "AK", "machine_specification": ""},
    {"partner_name": "Notpla - Threetec", "partner_code": "AL", "machine_specification": ""},
    {"partner_name": "Notpla - Leistritz 27mm", "partner_code": "AM", "machine_specification": ""},
    {"partner_name": "ARCHIVED POST BARREL REPLACEMENT Notpla - Boy 22A", "partner_code": "AN", "machine_specification": ""},
    {"partner_name": "Notpla - Collin", "partner_code": "AO", "machine_specification": ""},
    {"partner_name": "Polytechs - Boy 22A", "partner_code": "AP", "machine_specification": ""},
    {"partner_name": "Polytechs - Maris 51", "partner_code": "AQ", "machine_specification": ""},
    {"partner_name": "DeSter", "partner_code": "AR", "machine_specification": ""},
    {"partner_name": "PES - Sapphire, Spoons", "partner_code": "AS", "machine_specification": ""},
    {"partner_name": "Notpla - Boy 22 A", "partner_code": "AT", "machine_specification": ""},
    {"partner_name": "Warwick e-victory 60 ISO 527 Mould", "partner_code": "AU", "machine_specification": ""},
    {"partner_name": "Mclaren - 150 Spoon", "partner_code": "AV", "machine_specification": ""},
    {"partner_name": "Notpla - Engel Victory 210/50 Spex", "partner_code": "AX", "machine_specification": ""},
    {"partner_name": "QUB - Arburg - Family Tool", "partner_code": "AY", "machine_specification": ""},
    {"partner_name": "QUB - Boy - Clip Tool", "partner_code": "AZ", "machine_specification": ""},
    {"partner_name": "Pontacol - 29 L/D Extruder", "partner_code": "BA", "machine_specification": ""},
    {"partner_name": "DAME - Engel 50T", "partner_code": "BB", "machine_specification": ""},
    {"partner_name": "Ecozema - Ice Cream Spoon", "partner_code": "BC", "machine_specification": ""},
    {"partner_name": "Pontacol - Pilot Scale Extruder", "partner_code": "BD", "machine_specification": ""},
    {"partner_name": "Notpla Masterbatch", "partner_code": "BE", "machine_specification": ""},
]


@router.get("/partners", response_model=ApiResponse)
def list_location_partners(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Merge requested default mappings with user-created partner rows while preserving unique partner codes.
    try:
        custom = bigquery.list_location_partners()
    except NotFound:
        # If the custom partner table is missing in an environment, still return seeded partners for dropdown use.
        custom = []
    by_code = {partner["partner_code"]: partner for partner in DEFAULT_LOCATION_PARTNERS}
    for partner in custom:
        by_code[partner["partner_code"]] = partner
    items = sorted(by_code.values(), key=lambda row: row.get("partner_code", ""))
    return ApiResponse(ok=True, data={"items": items})


@router.post("/partners", response_model=ApiResponse)
def create_location_partner(
    payload: LocationPartnerCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor)
) -> ApiResponse:
    # Allocate the next two-letter code after seed values so custom partners continue from code BF onwards.
    partner_code = None
    for _ in range(20):
        # Always reserve a fresh code, even when partner or machine text matches an existing row.
        next_value = bigquery.allocate_counter("location_partner_code", "", 31)
        candidate = int_to_code(next_value)
        if not bigquery.get_location_partner(candidate):
            partner_code = candidate
            break
    if not partner_code:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Unable to allocate a unique partner code")
    bigquery.insert_location_partner(
        partner_code=partner_code,
        partner_name=payload.partner_name,
        machine_specification=payload.machine_specification,
        created_by=actor.email if actor else None,
    )
    return ApiResponse(ok=True, data={"partner_code": partner_code, "partner_name": payload.partner_name})




@router.get("/formulations", response_model=ApiResponse)
def list_location_formulations(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Supply selectable formulation combinations for location-code creation UX.
    items = bigquery.list_distinct_formulation_codes()
    return ApiResponse(ok=True, data={"items": items})


@router.get("", response_model=ApiResponse)
def list_location_codes(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    q: str | None = Query(default=None),
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    # Return paginated location IDs with optional substring filtering on the full location code string.
    rows, total = bigquery.list_location_codes_paginated(page=page, page_size=page_size, q=(q or "").strip() or None)

    # Merge partner labels for human-readable table rendering without losing canonical stored partner codes.
    partner_map = {partner["partner_code"]: partner for partner in DEFAULT_LOCATION_PARTNERS}
    try:
        for partner in bigquery.list_location_partners():
            partner_map[partner["partner_code"]] = partner
    except NotFound:
        pass

    normalized_rows = []
    for row in rows:
        partner = partner_map.get(row.get("partner_code"), {})
        normalized_rows.append(
            {
                **row,
                "partner_name": partner.get("partner_name") or "Unknown",
                "machine_specification": partner.get("machine_specification") or "",
            }
        )
    return ApiResponse(ok=True, data={"items": normalized_rows, "total": total, "page": page, "page_size": page_size})

@router.post("", response_model=ApiResponse)
def create_location_code(
    payload: LocationCodeCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Validate partner code against the merged default+custom partner registry before issuing a location ID.
    partner_codes = {partner["partner_code"] for partner in DEFAULT_LOCATION_PARTNERS}
    try:
        partner_codes.update(partner["partner_code"] for partner in bigquery.list_location_partners())
    except NotFound:
        # Allow location code generation to proceed with seeded defaults when custom table is unavailable.
        pass
    if payload.partner_code not in partner_codes:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partner code not found")

    # Ensure location codes are only generated for formulations that actually exist in formulation records.
    if not bigquery.formulation_exists(payload.set_code, payload.weight_code, payload.batch_variant_code):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Formulation not found")

    # Build the requested human-readable location ID format: AB AB AB AC 240827.
    location_id = (
        f"{payload.set_code} {payload.weight_code} {payload.batch_variant_code} "
        f"{payload.partner_code} {payload.production_date}"
    )
    bigquery.insert_location_code(
        set_code=payload.set_code,
        weight_code=payload.weight_code,
        batch_variant_code=payload.batch_variant_code,
        partner_code=payload.partner_code,
        production_date=payload.production_date,
        location_id=location_id,
        created_by=actor.email if actor else None,
    )
    return ApiResponse(ok=True, data={"location_id": location_id, "created_at": datetime.utcnow().isoformat()})
