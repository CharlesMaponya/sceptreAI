from __future__ import annotations

from automl_api.core.config import Settings, get_settings
from automl_api.storage.azure import AzureBlobObjectStoreDriver
from automl_api.storage.contracts import (
    ObjectMetadata,
    ObjectStoreDriver,
    RayDataSourceDescriptor,
)
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from automl_api.storage.gcs import GCSObjectStoreDriver
from automl_api.storage.s3 import S3ObjectStoreDriver

type StoredObject = ObjectMetadata


class ObjectStore:
    """Deprecated compatibility facade; concrete callers use ObjectStoreDriver."""

    def put_bytes(self, key: str, content: bytes):
        raise NotImplementedError

    def read_bytes(self, uri: str) -> bytes:
        raise NotImplementedError

    def read_head(self, uri: str, length: int = 4096) -> bytes:
        raise NotImplementedError

    def exists(self, uri: str) -> bool:
        raise NotImplementedError

    def dataframe_source(self, uri: str):
        raise NotImplementedError

    def delete(self, uri: str) -> None:
        raise NotImplementedError

    def healthcheck(self) -> None:
        raise NotImplementedError

    def size(self, uri: str) -> int:
        raise NotImplementedError


class EmbeddedObjectStore(EmbeddedObjectStoreDriver):
    """Compatibility name retained while callers migrate to ObjectStoreDriver."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(root=settings.local_object_store_path, bucket=settings.object_store_bucket)


class MinioObjectStore(S3ObjectStoreDriver):
    """Legacy name for the supported public-SDK S3-compatible driver."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(
            bucket=settings.object_store_bucket,
            endpoint_url=settings.object_store_endpoint,
            public_endpoint_url=settings.object_store_public_endpoint,
            region=settings.object_store_region,
            access_key=settings.object_store_access_key,
            secret_key=settings.object_store_secret_key,
            compatible=True,
        )


def get_object_store(settings: Settings | None = None) -> ObjectStoreDriver:
    settings = settings or get_settings()
    driver = settings.object_store_type.strip().lower()
    if driver == "minio":
        driver = "s3_compatible"

    if driver == "embedded":
        if settings.environment.lower() in {"production", "staging"}:
            raise ValueError("Embedded storage is forbidden in staging and production.")
        return EmbeddedObjectStoreDriver(
            root=settings.local_object_store_path,
            bucket=settings.object_store_bucket,
        )
    if driver == "s3_compatible":
        return S3ObjectStoreDriver(
            bucket=settings.object_store_bucket,
            endpoint_url=settings.object_store_endpoint,
            public_endpoint_url=settings.object_store_public_endpoint,
            region=settings.object_store_region,
            access_key=settings.object_store_access_key,
            secret_key=settings.object_store_secret_key,
            compatible=True,
        )
    if driver == "aws_s3":
        return S3ObjectStoreDriver(
            bucket=settings.object_store_bucket,
            region=settings.object_store_region,
            compatible=False,
        )
    if driver == "gcs":
        return GCSObjectStoreDriver(
            bucket=settings.object_store_bucket,
            project=settings.gcs_project,
        )
    if driver == "azure_blob":
        if not settings.azure_storage_account or not settings.object_store_endpoint:
            raise ValueError(
                "Azure Blob storage requires AZURE_STORAGE_ACCOUNT and OBJECT_STORE_ENDPOINT."
            )
        return AzureBlobObjectStoreDriver(
            account_url=settings.object_store_endpoint,
            account_name=settings.azure_storage_account,
            container=settings.object_store_bucket,
        )
    raise ValueError(
        f"Unknown object-store driver '{settings.object_store_type}'. "
        "Expected embedded, s3_compatible, aws_s3, gcs, or azure_blob."
    )


def legacy_dataframe_source(driver: ObjectStoreDriver, uri: str) -> tuple[str, dict[str, object]]:
    """Temporary tuple facade for code not yet migrated to RayDataSourceDescriptor."""
    descriptor: RayDataSourceDescriptor = driver.dataframe_source(uri)
    return descriptor.path, descriptor.filesystem_options
