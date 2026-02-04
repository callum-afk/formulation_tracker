from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_actor, get_bigquery, get_settings
from app.models import ApiResponse, DryWeightCreate
from app.services.bigquery_service import BigQueryService
from app.services.codegen_service import int_to_code
from app.services.hashing_service import hash_weights
from app.validators import ValidationError, round_weight, validate_weight_sum

router = APIRouter(prefix="/api/dry_weights", tags=["weights"])


@router.post("", response_model=ApiResponse)
def create_weights(
    payload: DryWeightCreate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
    settings=Depends(get_settings),
) -> ApiResponse:
    items = [(item.sku, round_weight(item.wt_percent)) for item in payload.items]
    try:
        validate_weight_sum(items)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    weight_hash = hash_weights(items)
    existing = bigquery.get_weight_by_hash(payload.set_code, weight_hash)
    if existing:
        return ApiResponse(ok=True, data={"weight_code": existing, "set_code": payload.set_code})

    next_value = bigquery.allocate_counter("weight_code", payload.set_code, settings.code_start_weight)
    weight_code = int_to_code(next_value)
    bigquery.insert_weight_variant(
        payload.set_code,
        weight_code,
        weight_hash,
        [(sku, float(wt)) for sku, wt in items],
        actor.email if actor else None,
    )
    return ApiResponse(ok=True, data={"weight_code": weight_code, "set_code": payload.set_code})


@router.get("", response_model=ApiResponse)
def list_weights(set_code: str, bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    rows = bigquery.list_weights(set_code)
    return ApiResponse(ok=True, data={"items": rows})
