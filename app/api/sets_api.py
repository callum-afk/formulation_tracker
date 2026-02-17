from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
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
    # Validate every supplied SKU before creating the deduplicated set hash.
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

    # Allocate the next user-facing code and persist both parent and item rows.
    next_value = bigquery.allocate_counter("set_code", "", settings.code_start_set)
    set_code = int_to_code(next_value)
    bigquery.insert_set(set_code, set_hash, payload.skus, actor.email if actor else None)
    return ApiResponse(ok=True, data={"set_code": set_code, "skus": payload.skus})


@router.get("", response_model=ApiResponse)
def list_sets(
    q: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    # Return paged set data so the table scales cleanly to 100+ rows.
    rows, total = bigquery.list_sets_paginated(search=q.strip() if q else None, page=page, page_size=page_size)
    return ApiResponse(ok=True, data={"items": rows, "total": total, "page": page, "page_size": page_size})


@router.get("/{set_code}", response_model=ApiResponse)
def get_set(set_code: str, bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    row = bigquery.get_set(set_code)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Set not found")
    return ApiResponse(ok=True, data=row)
