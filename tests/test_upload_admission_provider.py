"""Multipart retry lookup must not infer absence from a partial inventory."""

from unittest.mock import MagicMock
from xml.sax.saxutils import escape

import pytest
from automl_api.storage.contracts import UploadContractError
from automl_api.storage.s3 import S3ObjectStoreDriver
from test_phase2_upload_drivers import S3Client, _begin
from test_s3_sdk_retry_ownership import driver as sdk_driver
from test_s3_sdk_retry_ownership import storage_http as storage_http


@pytest.fixture(params=[False, True], ids=["aws", "compatible"])
def admission(request):
    native = S3Client()
    native.create_multipart_upload = MagicMock(wraps=native.create_multipart_upload)
    store = S3ObjectStoreDriver(
        bucket="bucket",
        client=native,
        compatible=request.param,
        endpoint_url="https://objects.test",
        access_key="test",
        secret_key="test",
    )
    return store, native


def page(*ids, **extra):
    return dict(
        IsTruncated=False, Uploads=[dict(Key=_begin().object_key, UploadId=i) for i in ids], **extra
    )


def test_retry_finds_original_on_later_page_without_replacement(admission):
    store, native = admission
    native.list_multipart_uploads = MagicMock(
        side_effect=[
            dict(
                IsTruncated=True,
                Uploads=[],
                NextKeyMarker=_begin().object_key,
                NextUploadIdMarker="previous",
            ),
            page("original"),
        ]
    )
    result = store.begin_upload(_begin())
    assert result.provider_id == "original"
    assert native.list_multipart_uploads.call_args.kwargs == dict(
        Bucket="bucket",
        Prefix=_begin().object_key,
        MaxUploads=1000,
        KeyMarker=_begin().object_key,
        UploadIdMarker="previous",
    )
    native.create_multipart_upload.assert_not_called()


def test_exact_identity_requires_complete_inventory_even_after_a_match(admission):
    store, native = admission
    native.list_multipart_uploads = MagicMock(
        side_effect=[
            dict(
                IsTruncated=True,
                Uploads=page("original")["Uploads"],
                NextKeyMarker=_begin().object_key,
                NextUploadIdMarker="original",
            ),
            page("different"),
        ]
    )
    with pytest.raises(UploadContractError, match="ambiguous"):
        store.begin_upload(_begin())
    native.create_multipart_upload.assert_not_called()


@pytest.mark.parametrize(
    "reply",
    [
        None,
        {},
        {"IsTruncated": "false"},
        {"IsTruncated": False, "Uploads": {}},
        {"IsTruncated": False, "Uploads": [None]},
        {"IsTruncated": False, "Uploads": [{"UploadId": "id"}]},
        {"IsTruncated": False, "Bucket": "foreign"},
        {"IsTruncated": False, "Prefix": "foreign"},
        {"IsTruncated": True, "NextKeyMarker": "key"},
    ],
)
def test_unqualified_inventory_cannot_authorize_creation(admission, reply):
    store, native = admission
    native.list_multipart_uploads = MagicMock(return_value=reply)
    with pytest.raises(UploadContractError):
        store.begin_upload(_begin())
    native.create_multipart_upload.assert_not_called()


@pytest.mark.parametrize("identity", [None, "", " ", False, "bad\nvalue", "x" * 1025])
def test_invalid_inventory_identity_cannot_be_reused(admission, identity):
    store, native = admission
    native.list_multipart_uploads = MagicMock(return_value=page(identity))
    with pytest.raises(UploadContractError):
        store.begin_upload(_begin())
    native.create_multipart_upload.assert_not_called()


def test_repeated_pagination_marker_is_bounded_and_cannot_create(admission):
    store, native = admission
    native.list_multipart_uploads = MagicMock(
        return_value=dict(
            IsTruncated=True,
            Uploads=[],
            NextKeyMarker=_begin().object_key,
            NextUploadIdMarker="repeat",
        )
    )
    with pytest.raises(UploadContractError):
        store.begin_upload(_begin())
    assert native.list_multipart_uploads.call_count == 2
    native.create_multipart_upload.assert_not_called()


def test_inventory_page_budget_cannot_turn_into_absence(admission):
    store, native = admission

    def advancing(**_):
        return dict(
            IsTruncated=True,
            Uploads=[],
            NextKeyMarker=_begin().object_key,
            NextUploadIdMarker=str(native.list_multipart_uploads.call_count),
        )

    native.list_multipart_uploads = MagicMock(side_effect=advancing)
    with pytest.raises(UploadContractError):
        store.begin_upload(_begin())
    assert native.list_multipart_uploads.call_count == 100
    native.create_multipart_upload.assert_not_called()


@pytest.mark.parametrize("error", [PermissionError("denied"), OSError("response lost")])
def test_failed_page_after_original_match_is_not_qualified_recovery(admission, error):
    store, native = admission
    native.list_multipart_uploads = MagicMock(
        side_effect=[
            dict(
                IsTruncated=True,
                Uploads=page("original")["Uploads"],
                NextKeyMarker=_begin().object_key,
                NextUploadIdMarker="original",
            ),
            error,
        ]
    )
    with pytest.raises(type(error), match=str(error)):
        store.begin_upload(_begin())
    assert native.list_multipart_uploads.call_count == 2
    native.create_multipart_upload.assert_not_called()


@pytest.mark.parametrize("fault", ["regression", "key-echo", "id-echo"])
def test_pagination_scope_cannot_change_between_pages(admission, fault):
    store, native = admission
    first = dict(
        IsTruncated=True,
        Uploads=[],
        NextKeyMarker=_begin().object_key + ".b",
        NextUploadIdMarker="first",
    )
    second = dict(
        IsTruncated=True,
        Uploads=[],
        NextKeyMarker=_begin().object_key + ".a",
        NextUploadIdMarker="second",
    )
    if fault == "key-echo":
        second = dict(IsTruncated=False, KeyMarker="foreign")
    elif fault == "id-echo":
        second = dict(IsTruncated=False, UploadIdMarker="foreign")
    native.list_multipart_uploads = MagicMock(side_effect=[first, second])
    with pytest.raises(UploadContractError):
        store.begin_upload(_begin())
    assert native.list_multipart_uploads.call_count == 2
    native.create_multipart_upload.assert_not_called()


def test_prefix_neighbor_is_never_reused(admission):
    store, native = admission
    neighbor = dict(Key=_begin().object_key + ".neighbor", UploadId="neighbor")
    native.multipart_uploads = [neighbor.copy()]
    native.list_multipart_uploads = MagicMock(
        return_value=dict(IsTruncated=False, Uploads=[neighbor])
    )
    result = store.begin_upload(_begin())
    assert result.provider_id == "upload-1"
    native.create_multipart_upload.assert_called_once()
    assert native.multipart_uploads[0] == neighbor and len(native.multipart_uploads) == 2
    assert native.aborts == 0


@pytest.mark.parametrize(
    "reply",
    [
        None,
        {},
        {"UploadId": None},
        {"UploadId": False},
        {"UploadId": ""},
        {"UploadId": "x" * 1025},
        {"UploadId": "bad\nvalue"},
        {"UploadId": "valid", "Bucket": "foreign"},
        {"UploadId": "valid", "Key": "foreign"},
    ],
)
def test_invalid_creation_reply_never_invents_provider_identity(admission, reply):
    store, native = admission
    native.list_multipart_uploads = MagicMock(return_value=page())
    native.create_multipart_upload = MagicMock(return_value=reply)
    with pytest.raises(UploadContractError):
        store.begin_upload(_begin())
    native.create_multipart_upload.assert_called_once()


def test_begin_refuses_hidden_sdk_retries_before_inventory(admission):
    store, native = admission
    native.meta.config.retries = {"total_max_attempts": 2}
    native.list_multipart_uploads = MagicMock()
    with pytest.raises(UploadContractError, match="single-attempt"):
        store.begin_upload(_begin())
    native.list_multipart_uploads.assert_not_called()
    native.create_multipart_upload.assert_not_called()


def xml_page(ids, *, truncated=False):
    key = escape(_begin().object_key)
    entries = "".join(
        f"<Upload><Key>{key}</Key><UploadId>{escape(i)}</UploadId></Upload>" for i in ids
    )
    markers = (
        f"<NextKeyMarker>{key}</NextKeyMarker><NextUploadIdMarker>page-one</NextUploadIdMarker>"
        if truncated
        else ""
    )
    return (
        '<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Bucket>bucket</Bucket><Prefix>{key}</Prefix>{entries}{markers}"
        f"<IsTruncated>{str(truncated).lower()}</IsTruncated></ListMultipartUploadsResult>"
    )


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize("fault", [None, "disconnect", "503"])
def test_actual_sdk_lost_creation_response_recovers_complete_inventory(
    storage_http, compatible, fault
):
    state = storage_http
    state.fault = fault
    store = sdk_driver(state, compatible)
    try:
        error = None
        try:
            store.begin_upload(_begin())
        except Exception as exc:
            error = exc
        assert (error is None) == (fault is None)
        assert state.uploads == [(_begin().object_key, "upload-1")]
        state.inventory = lambda query: (
            xml_page(["upload-1"]) if "upload-id-marker" in query else xml_page([], truncated=True)
        )
        result = store.begin_upload(_begin())
        assert result.provider_id == "upload-1" and len(state.uploads) == 1
        assert state.listings[-1]["key-marker"] == [_begin().object_key]
        assert state.listings[-1]["upload-id-marker"] == ["page-one"]
        assert state.listings[-1]["max-uploads"] == ["1000"]
    finally:
        store.client.close()


@pytest.mark.parametrize("compatible", [False, True])
def test_actual_sdk_ambiguous_later_page_preserves_both_histories(storage_http, compatible):
    state = storage_http
    state.uploads = [(_begin().object_key, "original"), (_begin().object_key, "other")]
    before = list(state.uploads)
    state.inventory = lambda query: (
        xml_page(["other"])
        if "upload-id-marker" in query
        else xml_page(["original"], truncated=True)
    )
    store = sdk_driver(state, compatible)
    try:
        with pytest.raises(UploadContractError, match="ambiguous"):
            store.begin_upload(_begin())
        assert state.uploads == before and len(state.listings) == 2
    finally:
        store.client.close()
