from __future__ import annotations

from fastapi import Request

from app.auth import AuthContext, require_auth_context
from app.config import Settings, load_settings
from app.services.bigquery_service import BigQueryService
from app.services.storage_service import StorageService


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_bigquery(request: Request) -> BigQueryService:
    return request.app.state.bigquery


def get_storage(request: Request) -> StorageService:
    return request.app.state.storage


def get_actor(request: Request) -> AuthContext:
    return require_auth_context(request)


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
