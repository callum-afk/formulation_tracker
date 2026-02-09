from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_actor, get_bigquery, get_settings
from app.models import ApiResponse, IngredientSetCreate
from app.services.bigquery_service import BigQueryService
from app.services.codegen_service import int_to_code
from app.services.hashing_service import hash_set

router = APIRouter(prefix="/api/sets", tags=["sets"])


@router.post("", response_model=ApiResponse)
def create_set(
    payload: IngredientSetCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
    settings=Depends(get_settings),
) -> ApiResponse:
    missing_skus = [sku for sku in payload.skus if not bigquery.get_ingredient(sku)]
    if missing_skus:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown SKU(s): {', '.join(missing_skus)}",
        )
    set_hash = hash_set(payload.skus)
    existing = bigquery.get_set_by_hash(set_hash)
    if existing:
        return ApiResponse(ok=True, data={"set_code": existing, "skus": payload.skus})

    next_value = bigquery.allocate_counter("set_code", "", settings.code_start_set)
    set_code = int_to_code(next_value)
    bigquery.insert_set(set_code, set_hash, payload.skus, actor.email if actor else None)
    return ApiResponse(ok=True, data={"set_code": set_code, "skus": payload.skus})


@router.get("", response_model=ApiResponse)
def list_sets(bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    rows = bigquery.list_sets()
    return ApiResponse(ok=True, data={"items": rows})
