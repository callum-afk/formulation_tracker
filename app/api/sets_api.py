from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.constants import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from app.dependencies import get_actor, get_bigquery, get_settings
from app.models import ApiResponse, IngredientSetCreate, IngredientSetUpdate
from app.services.bigquery_service import BigQueryService
from app.services.codegen_service import int_to_code
from app.services.hashing_service import hash_set
from app.services.permission_service import require_permission

router = APIRouter(prefix="/api/sets", tags=["sets"])


@router.post("", response_model=ApiResponse)
def create_set(
    payload: IngredientSetCreate,
    request: Request,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
    settings=Depends(get_settings),
) -> ApiResponse:
    # Enforce set edit access before any formulation-set write occurs.
    require_permission(request, "sets.edit")
    # Validate all supplied SKUs in one query to avoid one BigQuery roundtrip per row on large sets.
    existing_skus = bigquery.list_existing_ingredient_skus(payload.skus)
    # Keep response behavior identical by reporting only requested SKUs that were not found.
    missing_skus = [sku for sku in payload.skus if sku not in existing_skus]
    if missing_skus:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown SKU(s): {', '.join(missing_skus)}",
        )
    set_hash = hash_set(payload.skus)
    existing = bigquery.get_set_by_hash(set_hash)
    if existing:
        # Keep the existing deduplicated set while still allowing metadata-only saves on duplicate hashes.
        if payload.notes is not None or payload.material_workstream is not None:
            bigquery.update_set(existing, payload.notes, payload.material_workstream, actor.email if actor else None)
        return ApiResponse(
            ok=True,
            data={
                "set_code": existing,
                "skus": payload.skus,
                "notes": payload.notes,
                "material_workstream": payload.material_workstream,
            },
        )

    # Allocate the next user-facing code and persist both parent and item rows.
    next_value = bigquery.allocate_counter("set_code", "", settings.code_start_set)
    set_code = int_to_code(next_value)
    bigquery.insert_set(
        set_code,
        set_hash,
        payload.skus,
        actor.email if actor else None,
        payload.notes,
        payload.material_workstream,
    )
    return ApiResponse(
        ok=True,
        data={
            "set_code": set_code,
            "skus": payload.skus,
            "notes": payload.notes,
            "material_workstream": payload.material_workstream,
        },
    )


@router.get("", response_model=ApiResponse)
def list_sets(
    request: Request,
    q: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    # Enforce set listing access server-side for the API that powers the page.
    require_permission(request, "sets.view")
    # Return paged set data so the table scales cleanly to 100+ rows.
    rows, total = bigquery.list_sets_paginated(search=q.strip() if q else None, page=page, page_size=page_size)
    return ApiResponse(ok=True, data={"items": rows, "total": total, "page": page, "page_size": page_size})


@router.get("/{set_code}", response_model=ApiResponse)
def get_set(set_code: str, request: Request, bigquery: BigQueryService = Depends(get_bigquery)) -> ApiResponse:
    # Enforce set detail access server-side for direct API requests.
    require_permission(request, "sets.view")
    row = bigquery.get_set(set_code)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Set not found")
    return ApiResponse(ok=True, data=row)


@router.put("/{set_code}", response_model=ApiResponse)
def update_set(
    set_code: str,
    payload: IngredientSetUpdate,
    request: Request,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Restrict metadata edits to admins so non-admin users keep read-only set visibility.
    access = require_permission(request, "sets.edit")
    if not access.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only admins can edit sets")
    # Require an existing formulation set before applying metadata updates from the detail editor.
    row = bigquery.get_set(set_code)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Set not found")

    # Persist optional notes and material-workstream changes to the parent set row only.
    bigquery.update_set(set_code, payload.notes, payload.material_workstream, actor.email if actor else None)
    updated = bigquery.get_set(set_code)
    return ApiResponse(ok=True, data=updated)


@router.delete("/{set_code}", response_model=ApiResponse)
def delete_set(
    set_code: str,
    request: Request,
    bigquery: BigQueryService = Depends(get_bigquery),
) -> ApiResponse:
    # Restrict set deletion to admins and block deletes when downstream variants still depend on the set.
    access = require_permission(request, "sets.edit")
    if not access.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only admins can delete sets")
    row = bigquery.get_set(set_code)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Set not found")
    dependency_counts = bigquery.get_set_dependency_counts(set_code)
    blocking_dependencies = {name: count for name, count in dependency_counts.items() if count > 0}
    if blocking_dependencies:
        reason = ", ".join(f"{name}={count}" for name, count in blocking_dependencies.items())
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Set cannot be deleted because it is referenced by: {reason}",
        )
    bigquery.delete_set(set_code)
    return ApiResponse(ok=True, data={"set_code": set_code, "deleted": True})
