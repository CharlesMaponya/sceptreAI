from __future__ import annotations

from types import SimpleNamespace

import pytest
from automl_api.core.config import Settings
from automl_api.storage.object_store import (
    EmbeddedObjectStore,
    MinioObjectStore,
    ObjectStore,
    get_object_store,
)


class _Response:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.closed = False
        self.released = False

    def read(self) -> bytes:
        return self.content

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class _MinioClient:
    def __init__(self) -> None:
        self.bucket_present = False
        self.objects: dict[tuple[str, str], bytes] = {}
        self.fail = False
        self.responses: list[_Response] = []

    def bucket_exists(self, _bucket: str) -> bool:
        return self.bucket_present

    def make_bucket(self, _bucket: str) -> None:
        self.bucket_present = True

    def put_object(
        self,
        bucket: str,
        key: str,
        source: object,
        *,
        length: int,
        content_type: str,
    ) -> None:
        assert content_type == "application/octet-stream"
        self.objects[(bucket, key)] = source.read(length)  # type: ignore[attr-defined]

    def get_object(self, bucket: str, key: str, *, length: int | None = None) -> _Response:
        if self.fail:
            raise RuntimeError("object service unavailable")
        content = self.objects[(bucket, key)]
        response = _Response(content[:length] if length is not None else content)
        self.responses.append(response)
        return response

    def stat_object(self, bucket: str, key: str) -> SimpleNamespace:
        if self.fail:
            raise RuntimeError("object service unavailable")
        return SimpleNamespace(size=len(self.objects[(bucket, key)]))

    def remove_object(self, bucket: str, key: str) -> None:
        if self.fail:
            raise RuntimeError("object service unavailable")
        del self.objects[(bucket, key)]


def _settings(tmp_path: object, *, remote: bool = False) -> Settings:
    return Settings(
        object_store_type="minio" if remote else "embedded",
        object_store_endpoint="http://minio.test:9000" if remote else None,
        object_store_access_key="access" if remote else None,
        object_store_secret_key="secret" if remote else None,
        object_store_bucket="datasets",
        local_object_store_path=tmp_path,  # type: ignore[arg-type]
    )


def test_abstract_object_store_contract_is_not_silently_implemented() -> None:
    store = ObjectStore()
    calls = [
        lambda: store.put_bytes("key", b"value"),
        lambda: store.read_bytes("uri"),
        lambda: store.read_head("uri"),
        lambda: store.exists("uri"),
        lambda: store.dataframe_source("uri"),
        lambda: store.delete("uri"),
        store.healthcheck,
        lambda: store.size("uri"),
    ]
    for call in calls:
        with pytest.raises(NotImplementedError):
            call()


def test_embedded_store_round_trip_and_uri_isolation(tmp_path) -> None:
    store = EmbeddedObjectStore(_settings(tmp_path))
    stored = store.put_bytes("/project/data.csv", b"a,b\n1,2\n")

    assert stored.storage_path is not None
    assert store.exists(stored.uri)
    assert store.read_bytes(stored.uri) == b"a,b\n1,2\n"
    assert store.read_head(stored.uri, 3) == b"a,b"
    assert store.size(stored.uri) == 8
    source, options = store.dataframe_source(stored.uri)
    assert source == str(stored.storage_path.resolve())
    assert options == {}
    store.healthcheck()

    assert not store.exists("minio://another-bucket/project/data.csv")
    for operation in (
        store.read_bytes,
        store.read_head,
        store.dataframe_source,
        store.delete,
        store.size,
    ):
        with pytest.raises(ValueError, match="configured embedded store"):
            operation("minio://another-bucket/project/data.csv")

    store.delete(stored.uri)
    assert not store.exists(stored.uri)


def test_remote_store_round_trip_and_credentials(monkeypatch, tmp_path) -> None:
    client = _MinioClient()
    monkeypatch.setattr("minio.Minio", lambda *_args, **_kwargs: client)
    store = MinioObjectStore(_settings(tmp_path, remote=True))

    stored = store.put_bytes("/project/data.parquet", b"parquet-bytes")
    assert client.bucket_present
    assert store.read_bytes(stored.uri) == b"parquet-bytes"
    assert store.read_head(stored.uri, 7) == b"parquet"
    assert all(response.closed and response.released for response in client.responses)
    assert store.exists(stored.uri)
    assert store.size(stored.uri) == 13
    source, options = store.dataframe_source(stored.uri)
    assert source == "s3://datasets/project/data.parquet"
    assert options == {
        "key": "access",
        "secret": "secret",
        "client_kwargs": {"endpoint_url": "http://minio.test:9000"},
    }
    store.healthcheck()
    store.delete(stored.uri)
    assert not store.exists(stored.uri)


def test_remote_store_uses_explicit_local_fallback_only_for_existing_objects(
    monkeypatch,
    tmp_path,
) -> None:
    client = _MinioClient()
    monkeypatch.setattr("minio.Minio", lambda *_args, **_kwargs: client)
    store = MinioObjectStore(_settings(tmp_path, remote=True))
    fallback = store.fallback.put_bytes("project/fallback.csv", b"abcdef")
    client.fail = True

    assert store.read_bytes(fallback.uri) == b"abcdef"
    assert store.read_head(fallback.uri, 2) == b"ab"
    assert store.dataframe_source(fallback.uri) == (str(fallback.storage_path.resolve()), {})
    assert store.size(fallback.uri) == 6
    store.delete(fallback.uri)
    assert fallback.storage_path is not None and not fallback.storage_path.exists()

    missing = "minio://datasets/project/missing.csv"
    for operation, message in (
        (store.read_bytes, "Could not read"),
        (store.read_head, "Could not read"),
        (store.dataframe_source, "Could not locate"),
        (store.delete, "Could not delete"),
        (store.size, "Could not stat"),
    ):
        with pytest.raises(OSError, match=message):
            operation(missing)


def test_remote_store_configuration_and_factory_fail_closed(monkeypatch, tmp_path) -> None:
    with pytest.raises(ValueError, match="ENDPOINT"):
        get_object_store(Settings(object_store_type="minio"))
    with pytest.raises(ValueError, match="ENDPOINT"):
        MinioObjectStore(Settings(object_store_type="minio"))
    with pytest.raises(ValueError, match="access and secret"):
        MinioObjectStore(
            Settings(object_store_type="minio", object_store_endpoint="http://minio.test")
        )

    assert isinstance(get_object_store(_settings(tmp_path)), EmbeddedObjectStore)
    client = _MinioClient()
    monkeypatch.setattr("minio.Minio", lambda *_args, **_kwargs: client)
    assert isinstance(get_object_store(_settings(tmp_path, remote=True)), MinioObjectStore)
    with pytest.raises(ValueError, match="Unsupported production object-store driver"):
        get_object_store(
            Settings(
                environment="production",
                object_store_type="unknown-driver",
                local_object_store_path=tmp_path,
            )
        )


def test_remote_store_rejects_foreign_bucket_uris(monkeypatch, tmp_path) -> None:
    client = _MinioClient()
    monkeypatch.setattr("minio.Minio", lambda *_args, **_kwargs: client)
    store = MinioObjectStore(_settings(tmp_path, remote=True))
    foreign = "minio://another-bucket/project/data.csv"

    assert not store.exists(foreign)
    for operation in (
        store.read_bytes,
        store.read_head,
        store.dataframe_source,
        store.delete,
        store.size,
    ):
        with pytest.raises(ValueError, match="configured remote store"):
            operation(foreign)
