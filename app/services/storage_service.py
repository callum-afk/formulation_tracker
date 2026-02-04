from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from google.cloud import storage


@dataclass
class StorageService:
    project_id: str
    bucket_msds: str
    bucket_specs: str

    def __post_init__(self) -> None:
        self.client = storage.Client(project=self.project_id)

    def _bucket(self, name: str) -> storage.Bucket:
        return self.client.bucket(name)

    def generate_upload_url(self, bucket_name: str, object_path: str, content_type: str) -> str:
        bucket = self._bucket(bucket_name)
        blob = bucket.blob(object_path)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15),
            method="PUT",
            content_type=content_type,
        )

    def generate_download_url(self, bucket_name: str, object_path: str) -> str:
        bucket = self._bucket(bucket_name)
        blob = bucket.blob(object_path)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15),
            method="GET",
        )

    def object_exists(self, bucket_name: str, object_path: str) -> bool:
        bucket = self._bucket(bucket_name)
        return bucket.blob(object_path).exists()
