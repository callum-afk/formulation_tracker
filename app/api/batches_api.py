from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

from app.dependencies import get_actor, get_bigquery, get_settings, get_storage
from app.models import ApiResponse, IngredientBatchCreate, UploadConfirm, UploadRequest
from app.services.bigquery_service import BigQueryService
from app.services.storage_service import StorageService

router = APIRouter(prefix="/api/ingredient_batches", tags=["batches"])

MAX_UPLOAD_BYTES = 20 * 1024 * 1024


def _safe_filename(filename: str) -> str:
    base = Path(filename).name
    return base.replace(" ", "_")


@router.post("", response_model=ApiResponse)
def create_batch(
    payload: IngredientBatchCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    ingredient = bigquery.get_ingredient(payload.sku)
    if not ingredient:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingredient not found")
    now = datetime.now(timezone.utc)
    received_at = None
    if payload.received_at:
        try:
            parsed = datetime.fromisoformat(payload.received_at)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid received_at timestamp",
            ) from exc
        if parsed.tzinfo is None:
            received_at = parsed.replace(tzinfo=timezone.utc)
        else:
            received_at = parsed.astimezone(timezone.utc)
    bigquery.insert_batch(
        {
            "sku": payload.sku,
            "ingredient_batch_code": payload.ingredient_batch_code,
            "received_at": received_at,
            "notes": payload.notes,
            "quantity_value": payload.quantity_value,
            "quantity_unit": payload.quantity_unit,
            "created_at": now,
            "updated_at": now,
            "created_by": actor.email if actor else None,
            "updated_by": actor.email if actor else None,
            "is_active": True,
            "spec_object_path": None,
            "spec_uploaded_at": None,
        }
    )
    return ApiResponse(ok=True, data={"batch": payload.dict(), "owner": actor.email if actor else None})


@router.get("", response_model=ApiResponse)
def list_batches(sku: str, bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    rows = bigquery.list_batches(sku)
    return ApiResponse(ok=True, data={"items": rows})


@router.post("/{sku}/{batch_code}/coa", response_model=ApiResponse)
async def upload_coa(
    sku: str,
    batch_code: str,
    file: UploadFile = File(...),
    replace_confirmed: bool = Form(False),
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
) -> ApiResponse:
    batch = bigquery.get_batch(sku, batch_code)
    if not batch:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Batch not found")

    if batch.get("spec_object_path") and not replace_confirmed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Batch already has a CoA. Confirm replacement to continue.",
        )

    if file.content_type != "application/pdf":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF only")

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File too large")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    safe_name = _safe_filename(file.filename or "coa.pdf")
    object_path = f"specs/{sku}/{batch_code}/{timestamp}_{uuid4().hex}_{safe_name}"
    storage.upload_bytes(settings.bucket_specs, object_path, content, file.content_type)

    previous_object_path = batch.get("spec_object_path")
    bigquery.update_spec(sku, batch_code, object_path)

    if previous_object_path and previous_object_path != object_path:
        storage.delete_object(settings.bucket_specs, previous_object_path)

    return ApiResponse(ok=True, data={"object_path": object_path, "filename": file.filename, "content_type": file.content_type})


@router.post("/{sku}/{batch_code}/spec/upload_url", response_model=ApiResponse)
def spec_upload_url(
    sku: str,
    batch_code: str,
    payload: UploadRequest,
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
) -> ApiResponse:
    if payload.content_type != "application/pdf":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF only")
    if payload.content_length > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File too large")
    batches = bigquery.list_batches(sku)
    if not any(batch["ingredient_batch_code"] == batch_code for batch in batches):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Batch not found")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    object_path = f"specs/{sku}/{batch_code}/{timestamp}_{payload.filename}"
    upload_url = storage.generate_upload_url(settings.bucket_specs, object_path, payload.content_type)
    return ApiResponse(ok=True, data={"upload_url": upload_url, "object_path": object_path})


@router.post("/{sku}/{batch_code}/spec/confirm", response_model=ApiResponse)
def spec_confirm(
    sku: str,
    batch_code: str,
    payload: UploadConfirm,
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
) -> ApiResponse:
    if payload.content_type != "application/pdf":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="PDF only")
    if payload.content_length > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File too large")
    if not storage.object_exists(settings.bucket_specs, payload.object_path):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Object not found")
    bigquery.update_spec(sku, batch_code, payload.object_path)
    return ApiResponse(ok=True, data={"object_path": payload.object_path})


@router.get("/{sku}/{batch_code}/spec/download_url", response_model=ApiResponse)
def spec_download_url(
    sku: str,
    batch_code: str,
    bigquery: BigQueryService = Depends(get_bigquery),
    storage: StorageService = Depends(get_storage),
    settings=Depends(get_settings),
) -> ApiResponse:
    batches = bigquery.list_batches(sku)
    match = next((batch for batch in batches if batch["ingredient_batch_code"] == batch_code), None)
    if not match or not match.get("spec_object_path"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")
    url = storage.generate_download_url(settings.bucket_specs, match["spec_object_path"])
    return ApiResponse(ok=True, data={"download_url": url})
