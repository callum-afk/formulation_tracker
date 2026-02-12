from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import google.auth
from google.auth.transport.requests import Request
from google.cloud import storage


@dataclass
class StorageService:
    project_id: str
    bucket_msds: str
    bucket_specs: str

    def __post_init__(self) -> None:
        self.client = storage.Client(project=self.project_id)
        self.credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    def _bucket(self, name: str) -> storage.Bucket:
        return self.client.bucket(name)

    def _signing_kwargs(self) -> dict:
        self.credentials.refresh(Request())
        service_account_email = getattr(self.credentials, "service_account_email", None)
        if not service_account_email:
            return {}
        return {
            "service_account_email": service_account_email,
            "access_token": self.credentials.token,
        }

    def generate_upload_url(self, bucket_name: str, object_path: str, content_type: str) -> str:
        bucket = self._bucket(bucket_name)
        blob = bucket.blob(object_path)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15),
            method="PUT",
            content_type=content_type,
            **self._signing_kwargs(),
        )

    def generate_download_url(self, bucket_name: str, object_path: str, ttl_minutes: int = 10) -> str:
        bucket = self._bucket(bucket_name)
        blob = bucket.blob(object_path)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=ttl_minutes),
            method="GET",
            **self._signing_kwargs(),
        )

    def object_exists(self, bucket_name: str, object_path: str) -> bool:
        bucket = self._bucket(bucket_name)
        return bucket.blob(object_path).exists()

    def upload_bytes(self, bucket_name: str, object_path: str, content: bytes, content_type: str) -> None:
        bucket = self._bucket(bucket_name)
        blob = bucket.blob(object_path)
        blob.upload_from_string(content, content_type=content_type)

    def delete_object(self, bucket_name: str, object_path: str) -> None:
        bucket = self._bucket(bucket_name)
        blob = bucket.blob(object_path)
        if blob.exists():
            blob.delete()
