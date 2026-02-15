from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_actor, get_bigquery, get_settings
from app.models import ApiResponse, BatchVariantCreate
from app.services.bigquery_service import BigQueryService
from app.services.codegen_service import int_to_code
from app.services.hashing_service import hash_batches

router = APIRouter(prefix="/api/batch_variants", tags=["batch_variants"])


@router.post("", response_model=ApiResponse)
def create_batch_variant(
    payload: BatchVariantCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
    settings=Depends(get_settings),
) -> ApiResponse:
    weight_variant = bigquery.get_weight(payload.set_code, payload.weight_code)
    if not weight_variant:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Weight variant not found")
    weight_skus = {item.get("sku") for item in (weight_variant.get("items") or [])}

    items = [(item.sku, item.ingredient_batch_code) for item in payload.items]
    missing_batches = [
        f"{sku}:{batch_code}"
        for sku, batch_code in items
        if not bigquery.get_batch(sku, batch_code)
    ]
    if missing_batches:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown batch(es): {', '.join(missing_batches)}",
        )

    unknown_skus = [sku for sku, _ in items if sku not in weight_skus]
    if unknown_skus:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"SKU(s) not in weight variant {payload.weight_code}: {', '.join(unknown_skus)}",
        )

    batch_hash = hash_batches(items)
    existing = bigquery.get_batch_variant_by_hash(payload.set_code, payload.weight_code, batch_hash)
    if existing:
        base_code = f"{payload.set_code} {payload.weight_code} {existing}"
        return ApiResponse(ok=True, data={"batch_variant_code": existing, "base_code": base_code})

    scope = f"{payload.set_code} {payload.weight_code}"
    next_value = bigquery.allocate_counter("batch_variant_code", scope, settings.code_start_batch)
    batch_variant_code = int_to_code(next_value)
    bigquery.insert_batch_variant(
        payload.set_code,
        payload.weight_code,
        batch_variant_code,
        batch_hash,
        items,
        actor.email if actor else None,
    )
    base_code = f"{payload.set_code} {payload.weight_code} {batch_variant_code}"
    return ApiResponse(ok=True, data={"batch_variant_code": batch_variant_code, "base_code": base_code})


@router.get("", response_model=ApiResponse)
def list_batch_variants(
    set_code: str,
    weight_code: str,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    rows = bigquery.list_batch_variants(set_code, weight_code)
    return ApiResponse(ok=True, data={"items": rows})
