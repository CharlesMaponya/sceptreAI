"""Multipart progress must not stop after the provider's first page."""

from unittest.mock import MagicMock

import pytest
from automl_api.storage.contracts import UploadContractError
from automl_api.storage.s3 import S3ObjectStoreDriver
from test_phase2_upload_drivers import S3Client, _begin


def _case():
    client = S3Client()
    store = S3ObjectStoreDriver(
        bucket="bucket",
        client=client,
        compatible=True,
        endpoint_url="https://objects.test",
        access_key="unit",
        secret_key="unit",
    )
    upload = store.begin_upload(_begin())
    return store, client, upload


def test_progress_reads_all_original_parts_without_overcounting_or_recreation():
    store, client, upload = _case()
    client.list_parts = MagicMock(
        side_effect=[
            {
                "IsTruncated": True,
                "NextPartNumberMarker": 1,
                "Parts": [{"PartNumber": 1, "Size": 6, "ETag": "one"}],
            },
            {"IsTruncated": False, "Parts": [{"PartNumber": 2, "Size": 4, "ETag": "two"}]},
        ]
    )
    progress = store.query_progress(upload)
    assert progress.confirmed_bytes == 10 and progress.complete
    assert [r.unit_number for r in progress.receipts] == [1, 2]
    assert client.list_parts.call_args.kwargs["PartNumberMarker"] == 1


@pytest.mark.parametrize(
    "page",
    [
        {},
        {"IsTruncated": "false"},
        {"IsTruncated": False, "Bucket": "foreign"},
        {"IsTruncated": False, "Key": "foreign"},
        {"IsTruncated": False, "UploadId": "foreign"},
        {"IsTruncated": False, "Parts": {}},
        {"IsTruncated": False, "Parts": [None]},
        {"IsTruncated": False, "Parts": [{"PartNumber": True, "Size": 6}]},
        {"IsTruncated": False, "Parts": [{"PartNumber": 1, "Size": True}]},
        {"IsTruncated": False, "Parts": [{"PartNumber": 1, "Size": -1}]},
        {"IsTruncated": False, "Parts": [{"PartNumber": 10001, "Size": 6}]},
        {"IsTruncated": True, "Parts": [{"PartNumber": 1, "Size": 6}]},
        {"IsTruncated": True, "NextPartNumberMarker": 0, "Parts": [{"PartNumber": 1, "Size": 6}]},
        {
            "IsTruncated": True,
            "NextPartNumberMarker": True,
            "Parts": [{"PartNumber": 1, "Size": 6}],
        },
        {"IsTruncated": True, "NextPartNumberMarker": 2, "Parts": [{"PartNumber": 1, "Size": 6}]},
        {
            "IsTruncated": False,
            "Parts": [{"PartNumber": 1, "Size": 6}, {"PartNumber": 1, "Size": 6}],
        },
    ],
)
def test_partial_foreign_or_malformed_parts_inventory_cannot_be_adopted(page):
    store, client, upload = _case()
    client.list_parts = MagicMock(return_value=page)
    with pytest.raises(UploadContractError):
        store.query_progress(upload)


def test_repeated_part_page_cannot_double_count_confirmed_bytes():
    store, client, upload = _case()
    client.list_parts = MagicMock(
        side_effect=[
            {
                "IsTruncated": True,
                "NextPartNumberMarker": 1,
                "Parts": [{"PartNumber": 1, "Size": 6}],
            },
            {"IsTruncated": False, "Parts": [{"PartNumber": 1, "Size": 6}]},
        ]
    )
    with pytest.raises(UploadContractError):
        store.query_progress(upload)


def test_part_inventory_page_budget_is_bounded_even_when_markers_advance():
    store, client, upload = _case()

    def page(**kwargs):
        number = kwargs.get("PartNumberMarker", 0) + 1
        return {
            "IsTruncated": True,
            "NextPartNumberMarker": number,
            "Parts": [{"PartNumber": number, "Size": 6}],
        }

    client.list_parts = MagicMock(side_effect=page)
    with pytest.raises(UploadContractError, match="bounded page budget"):
        store.query_progress(upload)
    assert client.list_parts.call_count == 100
