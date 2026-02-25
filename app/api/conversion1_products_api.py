from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_actor, get_bigquery
from app.models import ApiResponse, Conversion1ProductCreate, Conversion1ProductUpdate
from app.services.bigquery_service import BigQueryService

router = APIRouter(prefix="/api/conversion1_products", tags=["conversion1_products"])

# Storage-location options mirror product warehousing states requested by the specification.
STORAGE_LOCATION_OPTIONS = ["Notpla", "Expect Warehouse", "FM Warehouse"]

# Shared status options for non-tensile test streams.
OTHER_STATUS_OPTIONS = ["Not Requested", "Requested", "Planned", "In Progress", "Complete"]

# Tensile status options include additional lifecycle states used by the tensile workflows.
TENSILE_STATUS_OPTIONS = [
    "Awaiting Moulding",
    "Ready",
    "Not Started",
    "Not Requested",
    "Planned",
    "In Progress",
    "Complete",
    "Conditioning",
]


def _normalize_optional_text(value: str | None) -> str | None:
    # Normalize whitespace and map blank values to None for nullable fields.
    normalized = (value or "").strip()
    return normalized or None


def _validate_update_payload(payload: dict, *, update: bool = False) -> dict:
    # Validate constrained dropdown fields and keep optional values nullable when not provided.
    storage_location = _normalize_optional_text(payload.get("storage_location"))
    if storage_location and storage_location not in STORAGE_LOCATION_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid storage_location")

    tensile_rigid_status = _normalize_optional_text(payload.get("tensile_rigid_status"))
    if tensile_rigid_status and tensile_rigid_status not in TENSILE_STATUS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid tensile_rigid_status")

    tensile_films_status = _normalize_optional_text(payload.get("tensile_films_status"))
    if tensile_films_status and tensile_films_status not in TENSILE_STATUS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid tensile_films_status")

    validated = {
        "storage_location": storage_location,
        "notes": _normalize_optional_text(payload.get("notes")),
        "number_units_produced": payload.get("number_units_produced"),
        "numbered_in_order": payload.get("numbered_in_order"),
        "tensile_rigid_status": tensile_rigid_status,
        "tensile_films_status": tensile_films_status,
    }

    # Validate shared status values for all non-tensile status columns.
    for field_name in [
        "seal_strength_status",
        "shelf_stability_status",
        "solubility_status",
        "defect_analysis_status",
        "blocking_status",
        "film_emc_status",
        "friction_status",
    ]:
        status_value = _normalize_optional_text(payload.get(field_name))
        if status_value and status_value not in OTHER_STATUS_OPTIONS:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid {field_name}")
        validated[field_name] = status_value

    # Keep numeric editable fields as optional values validated by Pydantic model types.
    for numeric_field in [
        "width_mm",
        "length_m",
        "avg_film_thickness_um",
        "sd_film_thickness",
        "film_thickness_variation_percent",
    ]:
        validated[numeric_field] = payload.get(numeric_field)

    # Reject no-op updates so each patch call changes at least one editable field.
    if update and all(value is None for value in validated.values()):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="No editable fields provided")

    return validated


@router.get("/meta", response_model=ApiResponse)
def get_conversion1_product_meta() -> ApiResponse:
    # Return dropdown option payload used by create and inline-edit controls.
    return ApiResponse(
        ok=True,
        data={
            "storage_location_options": STORAGE_LOCATION_OPTIONS,
            "other_status_options": OTHER_STATUS_OPTIONS,
            "tensile_status_options": TENSILE_STATUS_OPTIONS,
        },
    )


@router.get("/how_codes", response_model=ApiResponse)
def list_conversion1_how_codes(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Return active Conversion 1 How codes for dropdown selection in the create panel.
    return ApiResponse(ok=True, data={"items": bigquery.list_conversion1_how_codes()})


@router.get("", response_model=ApiResponse)
def list_conversion1_products(
    search: str | None = None,
    page: int = 1,
    page_size: int = 50,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    # Clamp pagination settings to safe bounds before loading table rows.
    safe_page = max(1, page)
    safe_page_size = max(1, min(page_size, 200))
    rows, total = bigquery.list_conversion1_products(
        search=(search or "").strip() or None,
        page=safe_page,
        page_size=safe_page_size,
    )
    return ApiResponse(ok=True, data={"items": rows, "total": total, "page": safe_page, "page_size": safe_page_size})


@router.post("", response_model=ApiResponse)
def create_conversion1_products(
    payload: Conversion1ProductCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Ensure create requests can only reference active existing Conversion 1 How rows.
    if not bigquery.conversion1_how_exists(payload.conversion1_how_code):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid conversion1_how_code")

    created = bigquery.create_conversion1_products(
        how_code=payload.conversion1_how_code,
        n=payload.number_of_records,
        created_by=actor.email if actor else None,
    )
    return ApiResponse(ok=True, data={"items": created})


@router.patch("/{product_code}", response_model=ApiResponse)
def update_conversion1_product(
    product_code: str,
    payload: Conversion1ProductUpdate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Validate constrained update fields before issuing the patch operation.
    validated = _validate_update_payload(payload.dict(exclude_unset=True), update=True)
    updated = bigquery.update_conversion1_product(
        product_code=product_code,
        patch_fields=validated,
        updated_by=actor.email if actor else None,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversion 1 product not found")
    return ApiResponse(ok=True, data={"product_code": product_code})
