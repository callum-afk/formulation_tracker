from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_actor, get_bigquery, get_settings, get_storage
from app.models import ApiResponse, IngredientCreate, IngredientImport, UploadConfirm, UploadRequest
from app.services.bigquery_service import BigQueryService
from app.services.codegen_service import format_sku, parse_sku
from app.services.storage_service import StorageService
from app.validators import ValidationError, validate_format, validate_pack_size_unit, validate_pack_size_value

router = APIRouter(prefix="/api/ingredients", tags=["ingredients"])

MAX_UPLOAD_BYTES = 20 * 1024 * 1024


def _validate_ingredient(payload: IngredientCreate) -> None:
    validate_pack_size_unit(payload.pack_size_unit)
    validate_pack_size_value(payload.pack_size_value)
    validate_format(payload.format)


@router.post("", response_model=ApiResponse)
def create_ingredient(
    payload: IngredientCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
    settings=Depends(get_settings),
) -> ApiResponse:
    try:
        _validate_ingredient(payload)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    seq = bigquery.allocate_counter("ingredient_seq", str(payload.category_code), 1)
    sku = format_sku(payload.category_code, seq, payload.pack_size_value)
    now = datetime.now(timezone.utc)
    bigquery.insert_ingredient(
        {
            "sku": sku,
            "category_code": payload.category_code,
            "seq": seq,
            "pack_size_value": payload.pack_size_value,
            "pack_size_unit": payload.pack_size_unit,
            "trade_name_inci": payload.trade_name_inci,
            "supplier": payload.supplier,
            "spec_grade": payload.spec_grade,
            "format": payload.format,
            "created_at": now,
            "updated_at": now,
            "created_by": actor.email if actor else None,
            "updated_by": actor.email if actor else None,
            "is_active": True,
            "msds_object_path": None,
            "msds_uploaded_at": None,
        }
    )
    ingredient = bigquery.get_ingredient(sku)
    return ApiResponse(ok=True, data={"sku": sku, "ingredient": ingredient})


@router.post("/import", response_model=ApiResponse)
def import_ingredient(
    payload: IngredientImport,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    try:
        _validate_ingredient(payload)
        category_code, seq, pack_size_value = parse_sku(payload.sku)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    now = datetime.now(timezone.utc)
    bigquery.insert_ingredient(
        {
            "sku": payload.sku,
            "category_code": category_code,
            "seq": seq,
            "pack_size_value": pack_size_value,
            "pack_size_unit": payload.pack_size_unit,
            "trade_name_inci": payload.trade_name_inci,
            "supplier": payload.supplier,
            "spec_grade": payload.spec_grade,
            "format": payload.format,
            "created_at": now,
            "updated_at": now,
            "created_by": actor.email if actor else None,
            "updated_by": actor.email if actor else None,
            "is_active": True,
            "msds_object_path": None,
            "msds_uploaded_at": None,
        }
    )
    ingredient = bigquery.get_ingredient(payload.sku)
    return ApiResponse(ok=True, data={"sku": payload.sku, "ingredient": ingredient})


@router.get("", response_model=ApiResponse)
def list_ingredients(
    q: str | None = None,
    category_code: int | None = None,
    format: str | None = None,
    pack_size_unit: str | None = None,
    is_active: bool | None = None,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    filters: Dict[str, object] = {}
    if q:
        filters["q"] = q
    if category_code is not None:
        filters["category_code"] = category_code
    if format:
        filters["format"] = format
    if pack_size_unit:
        filters["pack_size_unit"] = pack_size_unit
    if is_active is not None:
        filters["is_active"] = is_active
    rows = bigquery.list_ingredients(filters)
    return ApiResponse(ok=True, data={"items": rows})


@router.get("/{sku}", response_model=ApiResponse)
def get_ingredient(sku: str, bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    ingredient = bigquery.get_ingredient(sku)
    if not ingredient:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingredient not found")
    return ApiResponse(ok=True, data={"ingredient": ingredient})


@router.post("/{sku}/msds/upload_url", response_model=ApiResponse)
def msds_upload_url(
    sku: str,
    payload: UploadRequest,
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
) -> ApiResponse:
    if payload.content_type != "application/pdf":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF only")
    if payload.content_length > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File too large")
    ingredient = bigquery.get_ingredient(sku)
    if not ingredient:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingredient not found")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    object_path = f"msds/{sku}/{timestamp}_{payload.filename}"
    upload_url = storage.generate_upload_url(settings.bucket_msds, object_path, payload.content_type)
    return ApiResponse(
        ok=True,
        data={"upload_url": upload_url, "object_path": object_path, "expires_at": None},
    )


@router.post("/{sku}/msds/confirm", response_model=ApiResponse)
def msds_confirm(
    sku: str,
    payload: UploadConfirm,
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
) -> ApiResponse:
    if payload.content_type != "application/pdf":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF only")
    if payload.content_length > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File too large")
    if not storage.object_exists(settings.bucket_msds, payload.object_path):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Object not found")
    bigquery.update_msds(sku, payload.object_path)
    return ApiResponse(ok=True, data={"object_path": payload.object_path})


@router.get("/{sku}/msds/download_url", response_model=ApiResponse)
def msds_download_url(
    sku: str,
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
) -> ApiResponse:
    ingredient = bigquery.get_ingredient(sku)
    if not ingredient or not ingredient.get("msds_object_path"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="MSDS not found")
    url = storage.generate_download_url(settings.bucket_msds, ingredient["msds_object_path"])
    return ApiResponse(ok=True, data={"download_url": url})
