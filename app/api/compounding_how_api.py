from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_actor, get_bigquery
from app.models import ApiResponse, CompoundingHowCreate, CompoundingHowUpdate
from app.services.bigquery_service import BigQueryService
from app.services.codegen_service import int_to_code

router = APIRouter(prefix="/api/compounding_how", tags=["compounding_how"])

# Failure mode options copied exactly from the provided screenshot list.
FAILURE_MODES = [
    "N/A",
    "Under-Plasticized",
    "Over-Plasticized",
    "Brittle Filament",
    "Powder Feed Block",
    "Liquid Feed Block",
    "Torque Limit",
    "Pressure Limit",
    "Barrel Blockage",
    "Unknown",
    "Sticky Film (direct to film)",
    "Brittle Film (direct to film)",
    "Heterogeneity",
]


@router.get("/meta", response_model=ApiResponse)
def get_compounding_how_meta(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Provide dropdown metadata for location code and failure mode selectors on initial page load.
    return ApiResponse(ok=True, data={"location_codes": bigquery.list_location_code_ids(), "failure_modes": FAILURE_MODES})


@router.get("", response_model=ApiResponse)
def list_compounding_how(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Return all active compounding how entries for the table beneath the creation form.
    return ApiResponse(ok=True, data={"items": bigquery.list_compounding_how()})


@router.post("", response_model=ApiResponse)
def create_compounding_how(
    payload: CompoundingHowCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Guard process suffix format so processing codes remain two-letter AB-style values.
    suffix = (payload.process_code_suffix or "").strip().upper()
    if len(suffix) != 2 or not suffix.isalpha():
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="process_code_suffix must be two letters")

    # Validate failure mode against the fixed option list to keep reporting consistent.
    if payload.failure_mode not in FAILURE_MODES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid failure mode")

    # Compose immutable processing code by appending generated process suffix to chosen location code.
    processing_code = f"{payload.location_code} {suffix}".strip()
    bigquery.create_compounding_how(
        processing_code=processing_code,
        location_code=payload.location_code,
        process_code_suffix=suffix,
        failure_mode=payload.failure_mode,
        machine_setup_url=(payload.machine_setup_url or "").strip() or None,
        processed_data_url=(payload.processed_data_url or "").strip() or None,
        created_by=actor.email if actor else None,
    )
    return ApiResponse(ok=True, data={"processing_code": processing_code})


@router.post("/allocate", response_model=ApiResponse)
def allocate_process_suffix(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Allocate the next AB-style process suffix from code counters for collision-safe generation.
    next_value = bigquery.allocate_counter("compounding_process_code", "", 1)
    return ApiResponse(ok=True, data={"process_code_suffix": int_to_code(next_value)})


@router.put("/{processing_code}", response_model=ApiResponse)
def update_compounding_how(
    processing_code: str,
    payload: CompoundingHowUpdate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Validate editable failure mode on update to enforce the same controlled vocabulary as create.
    if payload.failure_mode not in FAILURE_MODES:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid failure mode")
    bigquery.update_compounding_how(
        processing_code=processing_code,
        failure_mode=payload.failure_mode,
        machine_setup_url=(payload.machine_setup_url or "").strip() or None,
        processed_data_url=(payload.processed_data_url or "").strip() or None,
        updated_by=actor.email if actor else None,
    )
    return ApiResponse(ok=True, data={"processing_code": processing_code})
