from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_actor, get_bigquery
from app.models import ApiResponse, PelletBagCreate, PelletBagUpdate
from app.services.bigquery_service import BigQueryService

router = APIRouter(prefix="/api/pellet_bags", tags=["pellet_bags"])

# Keep dropdown values centralized for consistent backend and frontend validation behavior.
PURPOSE_OPTIONS = [
    "Unassigned",
    "R and D",
    "Validation",
    "Production",
    "Available for Sales",
    "Sales Reserved",
    "Sales Sent",
    "Obsolete and Disposed",
]

# Match requested yes/no options for reference sample tracking.
REFERENCE_SAMPLE_OPTIONS = ["Yes", "No"]

# QC status options are intentionally distinct from the shared test status set.
QC_STATUS_OPTIONS = ["Not Requested", "Requested", "Recieved", "Testing Complete"]

# Shared status list used by long moisture and density status fields.
BASE_STATUS_OPTIONS = ["Not Requested", "Requested", "Recieved", "Planned", "In Progress", "Complete", "Not received"]

# Injection moulding and film forming add Failed in addition to the shared options.
INJECTION_FILM_STATUS_OPTIONS = BASE_STATUS_OPTIONS + ["Failed"]

# Initial assignee list is loaded from table via metadata endpoint so it can be extended without code changes.
DEFAULT_ASSIGNEE_EMAILS = ["callum@notpla.com", "peter@notpla.com", "emily@notpla.com"]


def _normalize_optional_text(value: str | None) -> str | None:
    # Keep notes/customer and other free text as plain strings with normalized whitespace.
    normalized = (value or "").strip()
    return normalized or None


def _validate_optional_payload(payload: dict, *, update: bool = False) -> dict:
    # Validate dropdown-constrained optional fields and normalize plain-text inputs.
    purpose = _normalize_optional_text(payload.get("purpose"))
    if purpose and purpose not in PURPOSE_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid purpose")

    reference_sample_taken = _normalize_optional_text(payload.get("reference_sample_taken"))
    if reference_sample_taken and reference_sample_taken not in REFERENCE_SAMPLE_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid reference sample value")

    qc_status = _normalize_optional_text(payload.get("qc_status"))
    if qc_status and qc_status not in QC_STATUS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid qc_status")

    long_moisture_status = _normalize_optional_text(payload.get("long_moisture_status"))
    if long_moisture_status and long_moisture_status not in BASE_STATUS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid long_moisture_status")

    density_status = _normalize_optional_text(payload.get("density_status"))
    if density_status and density_status not in BASE_STATUS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid density_status")

    injection_moulding_status = _normalize_optional_text(payload.get("injection_moulding_status"))
    if injection_moulding_status and injection_moulding_status not in INJECTION_FILM_STATUS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid injection_moulding_status")

    film_forming_status = _normalize_optional_text(payload.get("film_forming_status"))
    if film_forming_status and film_forming_status not in INJECTION_FILM_STATUS_OPTIONS:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid film_forming_status")

    validated = {
        "short_moisture_percent": payload.get("short_moisture_percent"),
        "purpose": purpose,
        "reference_sample_taken": reference_sample_taken,
        "qc_status": qc_status,
        "long_moisture_status": long_moisture_status,
        "density_status": density_status,
        "injection_moulding_status": injection_moulding_status,
        "film_forming_status": film_forming_status,
        "injection_moulding_assignee_email": _normalize_optional_text(payload.get("injection_moulding_assignee_email")),
        "film_forming_assignee_email": _normalize_optional_text(payload.get("film_forming_assignee_email")),
        "remaining_mass_kg": payload.get("remaining_mass_kg"),
        "notes": _normalize_optional_text(payload.get("notes")),
        "customer": _normalize_optional_text(payload.get("customer")),
    }

    if update and all(value is None for value in validated.values()):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="No editable fields provided")

    return validated


@router.get("/meta", response_model=ApiResponse)
def get_pellet_bag_meta(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Serve all dropdown options from one endpoint for predictable page bootstrap.
    return ApiResponse(
        ok=True,
        data={
            "product_types": ["PR", "PF", "PI"],
            "purpose_options": PURPOSE_OPTIONS,
            "reference_sample_options": REFERENCE_SAMPLE_OPTIONS,
            "qc_status_options": QC_STATUS_OPTIONS,
            "status_options": BASE_STATUS_OPTIONS,
            "injection_film_status_options": INJECTION_FILM_STATUS_OPTIONS,
            "assignee_emails": bigquery.list_pellet_bag_assignees(default_emails=DEFAULT_ASSIGNEE_EMAILS),
        },
    )


@router.get("", response_model=ApiResponse)
def list_pellet_bags(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Return all pellet bag rows for the management table.
    return ApiResponse(ok=True, data={"items": bigquery.list_pellet_bags()})


@router.post("", response_model=ApiResponse)
def create_pellet_bags(
    payload: PelletBagCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Validate and normalize optional fields before persisting.
    validated_optional = _validate_optional_payload(payload.dict())
    items = bigquery.create_pellet_bags(
        compounding_how_code=payload.compounding_how_code,
        product_type=payload.product_type,
        bag_mass_kg=payload.bag_mass_kg,
        number_of_bags=payload.number_of_bags,
        optional_fields=validated_optional,
        created_by=actor.email if actor else None,
    )
    return ApiResponse(ok=True, data={"items": items})


@router.patch("/{pellet_bag_id}", response_model=ApiResponse)
def update_pellet_bag(
    pellet_bag_id: str,
    payload: PelletBagUpdate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Apply updates only to editable fields and stamp updater metadata server-side.
    validated_optional = _validate_optional_payload(payload.dict(exclude_unset=True), update=True)
    updated = bigquery.update_pellet_bag(
        pellet_bag_id=pellet_bag_id,
        updated_by=actor.email if actor else None,
        optional_fields=validated_optional,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pellet bag not found")
    return ApiResponse(ok=True, data={"pellet_bag_id": pellet_bag_id})
