from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, Depends, Query

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
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    # Build optional filters from query params so users can browse all or narrow by one/more fields.
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

    # Return paginated formulations sorted newest-to-oldest by the underlying service query.
    rows, total = bigquery.list_formulations_paginated(filters=filters, page=page, page_size=page_size)
    return ApiResponse(ok=True, data={"items": rows, "total": total, "page": page, "page_size": page_size})
