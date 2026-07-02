from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from automl_api.core.config import Settings, get_settings


@dataclass(frozen=True)
class StoredObject:
    uri: str
    storage_path: Path | None = None


class ObjectStore:
    def put_bytes(self, key: str, content: bytes) -> StoredObject:
        raise NotImplementedError

    def read_bytes(self, uri: str) -> bytes:
        raise NotImplementedError

    def read_head(self, uri: str, length: int = 4096) -> bytes:
        raise NotImplementedError

    def exists(self, uri: str) -> bool:
        raise NotImplementedError

    def dataframe_source(self, uri: str) -> tuple[str, dict[str, object]]:
        raise NotImplementedError


class EmbeddedObjectStore(ObjectStore):
    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.object_store_bucket
        self.root = settings.local_object_store_path

    def put_bytes(self, key: str, content: bytes) -> StoredObject:
        normalized_key = key.strip("/")
        storage_path = self.root / self.bucket / normalized_key
        storage_path.parent.mkdir(parents=True, exist_ok=True)
        storage_path.write_bytes(content)
        return StoredObject(
            uri=f"minio://{self.bucket}/{normalized_key}",
            storage_path=storage_path,
        )

    def read_bytes(self, uri: str) -> bytes:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured embedded store.")
        key = uri.removeprefix(prefix).strip("/")
        return (self.root / self.bucket / key).read_bytes()

    def read_head(self, uri: str, length: int = 4096) -> bytes:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured embedded store.")
        key = uri.removeprefix(prefix).strip("/")
        with (self.root / self.bucket / key).open("rb") as source:
            return source.read(length)

    def exists(self, uri: str) -> bool:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            return False
        key = uri.removeprefix(prefix).strip("/")
        return (self.root / self.bucket / key).is_file()

    def dataframe_source(self, uri: str) -> tuple[str, dict[str, object]]:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured embedded store.")
        key = uri.removeprefix(prefix).strip("/")
        return str((self.root / self.bucket / key).resolve()), {}


class MinioObjectStore(ObjectStore):
    def __init__(self, settings: Settings) -> None:
        from minio import Minio

        if not settings.object_store_endpoint:
            raise ValueError("OBJECT_STORE_ENDPOINT is required for remote MinIO storage.")
        if not settings.object_store_access_key or not settings.object_store_secret_key:
            raise ValueError("MinIO access and secret keys are required.")

        parsed_endpoint = urlparse(settings.object_store_endpoint)
        endpoint = parsed_endpoint.netloc or parsed_endpoint.path
        self.endpoint_url = settings.object_store_endpoint
        self.access_key = settings.object_store_access_key
        self.secret_key = settings.object_store_secret_key
        self.bucket = settings.object_store_bucket
        self.fallback = EmbeddedObjectStore(settings)
        self.client = Minio(
            endpoint,
            access_key=settings.object_store_access_key,
            secret_key=settings.object_store_secret_key,
            secure=parsed_endpoint.scheme == "https",
        )

    def put_bytes(self, key: str, content: bytes) -> StoredObject:
        normalized_key = key.strip("/")
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
        self.client.put_object(
            self.bucket,
            normalized_key,
            io.BytesIO(content),
            length=len(content),
            content_type="application/octet-stream",
        )
        return StoredObject(uri=f"minio://{self.bucket}/{normalized_key}")

    def read_bytes(self, uri: str) -> bytes:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured MinIO store.")
        key = uri.removeprefix(prefix).strip("/")
        response = None
        try:
            response = self.client.get_object(self.bucket, key)
            return response.read()
        except Exception as exc:
            fallback_path = self.fallback.root / self.bucket / key
            if fallback_path.exists():
                return fallback_path.read_bytes()
            raise OSError(f"Could not read MinIO object '{key}': {exc}") from exc
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def read_head(self, uri: str, length: int = 4096) -> bytes:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured MinIO store.")
        key = uri.removeprefix(prefix).strip("/")
        response = None
        try:
            response = self.client.get_object(self.bucket, key, length=length)
            return response.read()
        except Exception as exc:
            fallback_path = self.fallback.root / self.bucket / key
            if fallback_path.exists():
                with fallback_path.open("rb") as source:
                    return source.read(length)
            raise OSError(f"Could not read MinIO object '{key}': {exc}") from exc
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def exists(self, uri: str) -> bool:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            return False
        key = uri.removeprefix(prefix).strip("/")
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except Exception:
            return False

    def dataframe_source(self, uri: str) -> tuple[str, dict[str, object]]:
        prefix = f"minio://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured MinIO store.")
        key = uri.removeprefix(prefix).strip("/")
        try:
            self.client.stat_object(self.bucket, key)
        except Exception as exc:
            fallback_path = self.fallback.root / self.bucket / key
            if fallback_path.exists():
                return str(fallback_path.resolve()), {}
            raise OSError(f"Could not locate MinIO object '{key}': {exc}") from exc
        return (
            f"s3://{self.bucket}/{key}",
            {
                "key": self.access_key,
                "secret": self.secret_key,
                "client_kwargs": {"endpoint_url": self.endpoint_url},
            },
        )


def get_object_store(settings: Settings | None = None) -> ObjectStore:
    settings = settings or get_settings()
    if (
        settings.object_store_type.lower() == "minio"
        and settings.object_store_endpoint
        and settings.object_store_access_key
        and settings.object_store_secret_key
    ):
        return MinioObjectStore(settings)
    return EmbeddedObjectStore(settings)
