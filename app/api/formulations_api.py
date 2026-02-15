from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, Depends

from app.dependencies import get_bigquery
from app.models import ApiResponse
from app.services.bigquery_service import BigQueryService

router = APIRouter(prefix="/api/formulations", tags=["formulations"])


@router.get("", response_model=ApiResponse)
def list_formulations(
    set_code: str | None = None,
    weight_code: str | None = None,
    batch_variant_code: str | None = None,
    sku: str | None = None,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    filters: Dict[str, str] = {}
    if set_code:
        filters["set_code"] = set_code
    if weight_code:
        filters["weight_code"] = weight_code
    if batch_variant_code:
        filters["batch_variant_code"] = batch_variant_code
    # Include optional SKU filter so users can search formulations containing a specific ingredient code.
    if sku:
        filters["sku"] = sku
    rows = bigquery.list_formulations(filters)
    return ApiResponse(ok=True, data={"items": rows})
