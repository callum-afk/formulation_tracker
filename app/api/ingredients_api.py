from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

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




def _safe_filename(filename: str) -> str:
    base = Path(filename).name
    return base.replace(" ", "_")

def _normalize_spec_grade(spec_grade: str | None) -> str | None:
    if spec_grade is None:
        return None
    value = spec_grade.strip()
    return value or None


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

    spec_grade = _normalize_spec_grade(payload.spec_grade)
    duplicate = bigquery.find_ingredient_duplicate(
        payload.category_code,
        payload.trade_name_inci,
        payload.supplier,
        spec_grade,
        payload.format,
        payload.pack_size_value,
        payload.pack_size_unit,
    )
    if duplicate:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ingredient already exists")

    product = bigquery.find_ingredient_product(
        payload.category_code,
        payload.trade_name_inci,
        payload.supplier,
        spec_grade,
        payload.format,
        payload.pack_size_unit,
    )
    if product:
        seq = int(product["seq"])
    else:
        seq = bigquery.allocate_counter("ingredient_seq", "global", 1)
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
            "spec_grade": spec_grade,
            "format": payload.format,
            "created_at": now,
            "updated_at": now,
            "created_by": actor.email if actor else None,
            "updated_by": actor.email if actor else None,
            "is_active": True,
            "msds_object_path": None,
            "msds_filename": None,
            "msds_content_type": None,
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

    spec_grade = _normalize_spec_grade(payload.spec_grade)
    duplicate = bigquery.find_ingredient_duplicate(
        category_code,
        payload.trade_name_inci,
        payload.supplier,
        spec_grade,
        payload.format,
        pack_size_value,
        payload.pack_size_unit,
    )
    if duplicate:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ingredient already exists")

    product = bigquery.find_ingredient_product(
        category_code,
        payload.trade_name_inci,
        payload.supplier,
        spec_grade,
        payload.format,
        payload.pack_size_unit,
    )
    if product and int(product["seq"]) != seq:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SKU sequence must match existing product",
        )
    if not product:
        # Block conflicts only when the same category already uses this sequence index.
        existing_seq = bigquery.find_ingredient_by_category_and_seq(category_code, seq)
        if existing_seq:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="SKU sequence already assigned",
            )

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
            "spec_grade": spec_grade,
            "format": payload.format,
            "created_at": now,
            "updated_at": now,
            "created_by": actor.email if actor else None,
            "updated_by": actor.email if actor else None,
            "is_active": True,
            "msds_object_path": None,
            "msds_filename": None,
            "msds_content_type": None,
            "msds_uploaded_at": None,
        }
    )
    # Raise the generated-SKU counter floor so future creates don't reuse imported sequence values.
    bigquery.set_counter_at_least("ingredient_seq", "global", seq + 1)

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


@router.post("/{sku}/msds", response_model=ApiResponse)
async def upload_msds(
    sku: str,
    file: UploadFile = File(...),
    replace_confirmed: bool = Form(False),
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
    actor=Depends(get_actor),
) -> ApiResponse:
    ingredient = bigquery.get_ingredient(sku)
    if not ingredient:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingredient not found")

    if ingredient.get("msds_object_path") and not replace_confirmed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ingredient already has an MSDS. Confirm replacement to continue.",
        )

    if file.content_type != "application/pdf":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF only")

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File too large")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    safe_name = _safe_filename(file.filename or "msds.pdf")
    object_path = f"msds/ingredients/{sku}/{timestamp}_{uuid4().hex}_{safe_name}"
    storage.upload_bytes(settings.bucket_msds, object_path, content, file.content_type)

    previous_object_path = ingredient.get("msds_object_path")
    bigquery.update_msds(
        sku,
        object_path,
        filename=file.filename or safe_name,
        content_type=file.content_type,
        updated_by=actor.email if actor else None,
    )

    if previous_object_path and previous_object_path != object_path:
        storage.delete_object(settings.bucket_msds, previous_object_path)

    return ApiResponse(ok=True, data={"object_path": object_path, "filename": file.filename, "content_type": file.content_type})


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
    url = storage.generate_download_url(settings.bucket_msds, ingredient["msds_object_path"], ttl_minutes=10)
    return ApiResponse(ok=True, data={"download_url": url})
