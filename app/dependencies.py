from __future__ import annotations

from typing import Optional

from fastapi import Depends, Request

from app.auth import IapUser, require_iap_user
from app.config import Settings, load_settings
from app.services.bigquery_service import BigQueryService
from app.services.storage_service import StorageService


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_bigquery(request: Request) -> BigQueryService:
    return request.app.state.bigquery


def get_storage(request: Request) -> StorageService:
    return request.app.state.storage


def get_actor(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> Optional[IapUser]:
    if settings.disable_auth:
        return IapUser(email="local@example.com", user_id=None)
    return require_iap_user(
        x_goog_authenticated_user_email=request.headers.get(
            "X-Goog-Authenticated-User-Email"
        ),
        x_goog_authenticated_user_id=request.headers.get("X-Goog-Authenticated-User-Id"),
    )


def init_services(settings: Settings) -> tuple[BigQueryService, StorageService]:
    bigquery = BigQueryService(project_id=settings.project_id, dataset_id=settings.dataset_id)
    storage = StorageService(
        project_id=settings.project_id,
        bucket_msds=settings.bucket_msds,
        bucket_specs=settings.bucket_specs,
    )
    return bigquery, storage


def init_settings() -> Settings:
    return load_settings()
