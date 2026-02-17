from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess


@dataclass(frozen=True)
class Settings:
    project_id: str
    dataset_id: str
    bq_location: str
    region: str
    bucket_msds: str
    bucket_specs: str
    cloud_run_service_name: str
    log_level: str
    code_start_set: int
    code_start_weight: int
    code_start_batch: int
    auth_mode: str
    app_version: str


def _resolve_app_version() -> str:
    # Prefer explicit env override for emergency rollbacks or local testing overrides.
    env_version = os.getenv("APP_VERSION")
    if env_version:
        return env_version

    # Read build-generated version metadata file so deployed containers always show current git info.
    version_file = Path("app/version.txt")
    if version_file.exists():
        version_text = version_file.read_text(encoding="utf-8").strip()
        if version_text:
            return version_text

    # Fallback to local git command for dev runs when version.txt was not generated at build time.
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--pretty=format:%h %s"],
            check=True,
            capture_output=True,
            text=True,
        )
        version = result.stdout.strip()
        if version:
            return version
    except Exception:
        pass
    return "dev"


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
        bq_location=os.getenv("BQ_LOCATION", "EU"),
        region=_get_env("REGION"),
        bucket_msds=_get_env("BUCKET_MSDS"),
        bucket_specs=_get_env("BUCKET_SPECS"),
        cloud_run_service_name=_get_env("CLOUD_RUN_SERVICE_NAME"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        code_start_set=int(os.getenv("CODE_START_SET", "1")),
        code_start_weight=int(os.getenv("CODE_START_WEIGHT", "1")),
        code_start_batch=int(os.getenv("CODE_START_BATCH", "1")),
        auth_mode=auth_mode,
        app_version=_resolve_app_version(),
    )
