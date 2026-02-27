from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.pellet_bags_api import (
    DEFAULT_ASSIGNEE_EMAILS,
    STATUS_LIST_COLUMN_WHITELIST,
    get_allowed_status_options,
    normalize_status_value,
)
from app.dependencies import get_actor, get_bigquery
from app.models import ApiResponse, PelletBagStatusListUpdate
from app.services.bigquery_service import BigQueryService

# Keep the status-list specific API separate from the broader pellet_bags CRUD surface.
router = APIRouter(prefix="/api/pellet-bags", tags=["pellet_bag_status"])


@router.patch("/status", response_model=ApiResponse)
def update_pellet_bag_status_list_row(
    payload: PelletBagStatusListUpdate,
    bigquery: BigQueryService = Depends(get_bigquery),
    actor=Depends(get_actor),
) -> ApiResponse:
    # Validate user-supplied status-column names against an explicit whitelist to prevent arbitrary SQL updates.
    if payload.status_column not in STATUS_LIST_COLUMN_WHITELIST:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid status_column")

    # Normalize and validate status values against the same canonical options used by /pellet_bags.
    normalized_status_value = normalize_status_value(payload.status_value)
    allowed_statuses = get_allowed_status_options(payload.status_column)
    if not normalized_status_value or normalized_status_value not in allowed_statuses:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid status_value")

    # Normalize assignee values and enforce the same allowed assignee list used by /pellet_bags metadata.
    normalized_assigned_value = (payload.assigned_value or "").strip() or None
    allowed_assignees = bigquery.list_pellet_bag_assignees(default_emails=DEFAULT_ASSIGNEE_EMAILS)
    if normalized_assigned_value and normalized_assigned_value not in allowed_assignees:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid assigned_value")

    # Persist only the whitelisted status and mapped assignee columns for one active pellet bag row.
    updated_row = bigquery.update_pellet_bag_status_and_assignee(
        pellet_bag_code=payload.pellet_bag_code,
        status_column=payload.status_column,
        status_value=normalized_status_value,
        assigned_value=normalized_assigned_value,
        updated_by=actor.email if actor else None,
    )
    if not updated_row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pellet bag not found")

    # Return the canonical row payload expected by inline editor clients.
    return ApiResponse(ok=True, data={"updated_row": updated_row})
