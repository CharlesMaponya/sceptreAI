"""Metadata for exact immutable versions cannot fall back to latest."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.storage.contracts import UploadContractError
from botocore.stub import Stubber
from test_versioned_streams import provider as provider


def test_stat_selects_original_native_version(provider, monkeypatch):
    store, uri, native = provider
    if store.driver_name == "aws_s3":
        with Stubber(native) as stub:
            stub.add_response(
                "head_object",
                {"ContentLength": 10, "VersionId": "v1"},
                {"Bucket": "bucket", "Key": "key", "VersionId": "v1"},
            )
            metadata = store.stat(uri, version="v1")
            stub.assert_no_pending_responses()
        version_key, version = "version_id", "v1"
    elif store.driver_name == "gcs":
        blob = SimpleNamespace(
            name="key",
            size=10,
            generation=7,
            metageneration=1,
            etag=None,
            crc32c=None,
            md5_hash=None,
            content_type=None,
        )
        get = MagicMock(return_value=blob)
        monkeypatch.setattr(store.bucket, "get_blob", get)
        metadata = store.stat(uri, version="7")
        get.assert_called_once_with("key", generation=7)
        version_key, version = "generation", "7"
    else:
        blob = SimpleNamespace(
            blob_name="key",
            get_blob_properties=MagicMock(
                return_value=SimpleNamespace(
                    size=10, version_id="v1", etag=None, content_settings=None
                )
            ),
        )
        get = MagicMock(return_value=blob)
        monkeypatch.setattr(store.container, "get_blob_client", get)
        metadata = store.stat(uri, version="v1")
        get.assert_called_once_with("key", version_id="v1")
        version_key, version = "version_id", "v1"
    assert metadata.uri == uri and metadata.byte_size == 10
    assert metadata.provider_headers[version_key] == version


def test_stat_refuses_missing_or_foreign_response_version(provider, monkeypatch):
    store, uri, native = provider
    for returned in [None, "replacement"]:
        if store.driver_name == "aws_s3":
            with Stubber(native) as stub:
                reply = {"ContentLength": 10, **({"VersionId": returned} if returned else {})}
                stub.add_response(
                    "head_object", reply, {"Bucket": "bucket", "Key": "key", "VersionId": "v1"}
                )
                with pytest.raises(UploadContractError):
                    store.stat(uri, version="v1")
                stub.assert_no_pending_responses()
        elif store.driver_name == "gcs":
            monkeypatch.setattr(
                store.bucket,
                "get_blob",
                MagicMock(return_value=SimpleNamespace(generation=returned)),
            )
            with pytest.raises(UploadContractError):
                store.stat(uri, version="7")
        else:
            monkeypatch.setattr(
                store.container,
                "get_blob_client",
                MagicMock(
                    return_value=SimpleNamespace(
                        get_blob_properties=lambda value=returned: SimpleNamespace(version_id=value)
                    )
                ),
            )
            with pytest.raises(UploadContractError):
                store.stat(uri, version="v1")


@pytest.mark.parametrize("version", ["", "null", True, 1, "v\n1"])
def test_invalid_metadata_version_never_calls_native(provider, monkeypatch, version):
    store, uri, native = provider
    denied = MagicMock(side_effect=AssertionError("Invalid metadata version reached provider"))
    if store.driver_name == "aws_s3":
        monkeypatch.setattr(native, "head_object", denied)
    elif store.driver_name == "gcs":
        monkeypatch.setattr(store.bucket, "get_blob", denied)
    else:
        monkeypatch.setattr(store.container, "get_blob_client", denied)
    with pytest.raises(UploadContractError):
        store.stat(uri, version=version)
    denied.assert_not_called()
