"""Validate native signature controls, including real botocore refresh ordering."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from urllib.parse import urlencode

import boto3
import pytest
from automl_api.services import final_test_credentials, refit_access
from automl_api.storage.read_capabilities import validate_native_read_url
from automl_api.storage.s3 import S3ObjectStoreDriver
from botocore.config import Config
from botocore.credentials import RefreshableCredentials


@pytest.fixture
def clock(monkeypatch):
    state = [datetime(2026, 10, 18, 12, 0, 0, tzinfo=UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return state[0]

    monkeypatch.setattr(refit_access, "datetime", Clock)
    return state


@pytest.mark.parametrize(
    "driver,prefix,version_key",
    [
        ("aws_s3", "X-Amz-", "versionId"),
        ("s3_compatible", "X-Amz-", "versionId"),
        ("gcs", "X-Goog-", "generation"),
    ],
)
@pytest.mark.parametrize(
    "fault", ["none", "delayed", "version", "duplicate", "long", "userinfo", "missing"]
)
def test_capability_native_controls_have_a_positive_control(
    clock, driver, prefix, version_key, fault
):
    now = clock[0]
    params = {
        prefix + "Date": now.strftime("%Y%m%dT%H%M%SZ"),
        prefix + "Expires": "30",
        version_key: "123",
    }
    url = "https://objects.invalid/input?" + urlencode(params)
    validate_native_read_url(url, driver, now + timedelta(seconds=60), "123", now=now)
    if fault == "none":
        return
    if fault == "delayed":
        params[prefix + "Date"] = (now + timedelta(seconds=40)).strftime("%Y%m%dT%H%M%SZ")
    elif fault == "version":
        params[version_key] = "124"
    elif fault == "long":
        params[prefix + "Expires"] = "901"
    elif fault == "missing":
        del params[prefix + "Expires"]
    url = "https://objects.invalid/input?" + urlencode(params)
    if fault == "duplicate":
        url += "&" + urlencode({prefix + "Expires": "30"})
    if fault == "userinfo":
        url = url.replace("https://", "https://:password@")
    with pytest.raises(ValueError):
        validate_native_read_url(url, driver, now + timedelta(seconds=60), "123", now=now)


@pytest.mark.parametrize("fault", ["none", "write", "http", "version", "delayed", "naive"])
def test_azure_sas_is_read_only_version_bound_and_absolute(clock, fault):
    now = clock[0]
    params = {
        "se": (now + timedelta(seconds=30)).isoformat(),
        "sp": "r",
        "spr": "https",
        "versionid": "version-1",
    }
    url = "https://account.blob.core.windows.net/container/input?" + urlencode(params)
    validate_native_read_url(url, "azure_blob", now + timedelta(seconds=60), "version-1", now=now)
    if fault == "none":
        return
    if fault == "write":
        params["sp"] = "rw"
    elif fault == "http":
        params["spr"] = "http,https"
    elif fault == "version":
        params["versionid"] = "version-2"
    elif fault == "delayed":
        params["se"] = (now + timedelta(seconds=61)).isoformat()
    else:
        params["se"] = (now + timedelta(seconds=30)).replace(tzinfo=None).isoformat()
    with pytest.raises(ValueError):
        validate_native_read_url(
            "https://account.blob.core.windows.net/container/input?" + urlencode(params),
            "azure_blob",
            now + timedelta(seconds=60),
            "version-1",
            now=now,
        )


@pytest.mark.parametrize("kind", ["refit", "final"])
def test_real_botocore_refresh_cannot_extend_the_authorized_deadline(clock, monkeypatch, kind):
    import botocore.auth

    monkeypatch.setattr(botocore.auth, "get_current_datetime", lambda: clock[0])
    client = boto3.client(
        "s3",
        endpoint_url="https://objects.invalid",
        region_name="us-east-1",
        aws_access_key_id="unit-key",
        aws_secret_access_key="unit-secret",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    store = S3ObjectStoreDriver(bucket="safe", client=client)
    item = SimpleNamespace(uri="s3://safe/input.parquet", byte_size=100)
    deadline = clock[0] + timedelta(seconds=60)
    if kind == "refit":

        def mint():
            return refit_access.mint_refit_read_url(
                store, item, deadline, object_version="version-1"
            )
    else:
        monkeypatch.setattr(final_test_credentials, "datetime", refit_access.datetime)
        monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
        manifest = SimpleNamespace(
            provider="aws",
            bucket="safe",
            objects=[SimpleNamespace(key="input.parquet", version="version-1")],
        )

        def mint():
            return final_test_credentials.mint_read_urls(manifest, deadline)

    assert mint()
    refreshed = []

    def refresh():
        clock[0] += timedelta(seconds=40)
        refreshed.append(True)
        return dict(
            access_key="refreshed-unit-key",
            secret_key="refreshed-unit-secret",
            token="unit-token",
            expiry_time=(clock[0] + timedelta(hours=1)).isoformat(),
        )

    credentials = RefreshableCredentials(
        access_key="old-unit-key",
        secret_key="old-unit-secret",
        token="old-unit-token",
        expiry_time=clock[0] - timedelta(seconds=1),
        refresh_using=refresh,
        method="unit-refresh",
        time_fetcher=lambda: clock[0],
    )
    # Exercise the installed SDK's actual credential-refresh-before-signing path.
    client._request_signer._credentials = credentials
    with pytest.raises(ValueError, match="original deadline"):
        mint()
    assert refreshed == [True]
