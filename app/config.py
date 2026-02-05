from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    project_id: str
    dataset_id: str
    region: str
    bucket_msds: str
    bucket_specs: str
    cloud_run_service_name: str
    log_level: str
    code_start_set: int
    code_start_weight: int
    code_start_batch: int
    auth_mode: str


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    auth_mode = os.getenv("AUTH_MODE", "cloudrun").lower()
    if auth_mode not in {"cloudrun", "iap", "none"}:
        raise RuntimeError("AUTH_MODE must be one of: cloudrun, iap, none")

    return Settings(
        project_id=_get_env("PROJECT_ID"),
        dataset_id=_get_env("DATASET_ID"),
        region=_get_env("REGION"),
        bucket_msds=_get_env("BUCKET_MSDS"),
        bucket_specs=_get_env("BUCKET_SPECS"),
        cloud_run_service_name=_get_env("CLOUD_RUN_SERVICE_NAME"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        code_start_set=int(os.getenv("CODE_START_SET", "1")),
        code_start_weight=int(os.getenv("CODE_START_WEIGHT", "1")),
        code_start_batch=int(os.getenv("CODE_START_BATCH", "1")),
        auth_mode=auth_mode,
    )
