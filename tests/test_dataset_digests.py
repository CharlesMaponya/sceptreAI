import io
import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api import preparation_worker
from automl_api.models.enums import DatasetFormat
from automl_api.services import profiling, profiling_jobs, training, validation
from automl_api.services.dataset_digests import (
    DatasetDigestError,
    require_verified_digest,
    verify_dataset_bytes,
)
from automl_api.training import pipeline
from digest_fixtures import verified_raw_version
from fastapi.testclient import TestClient


@pytest.mark.parametrize(
    "changes",
    [
        {"content_hash_algorithm": None},
        {"content_hash_algorithm": "md5"},
        {"content_hash_algorithm": "SHA256"},
        {"content_hash_scope": None},
        {"content_hash_scope": "multipart_manifest"},
        {"content_hash_verification_status": "pending"},
        {"content_hash_verification_status": "verified_legacy"},
        {"content_hash_verified_at": None},
        {"content_hash_verified_at": datetime.now()},
        {"content_hash_verified_at": "2026-10-16"},
        {"content_hash": None},
        {"content_hash": "a" * 63},
        {"content_hash": "Z" * 64},
        {"content_hash": "sha256:" + "a" * 64},
        {"content_hash": "A" * 64},
        {"byte_size": None},
        {"byte_size": -1},
        {"byte_size": True},
        {"byte_size": "7"},
    ],
)
def test_digest_metadata_rejects_unqualified_provenance_with_valid_control(changes):
    valid = verified_raw_version()
    assert require_verified_digest(valid) == valid.content_hash
    with pytest.raises(DatasetDigestError):
        require_verified_digest(verified_raw_version(**changes))
    with pytest.raises(DatasetDigestError):
        require_verified_digest(SimpleNamespace())


def test_actual_bytes_verify_empty_and_reject_size_or_content_changes():
    for content in (b"", b"raw-data"):
        version = verified_raw_version(content)
        verify_dataset_bytes(version, content)
        for changed in (content + b"x", b"x" * len(content)):
            if changed == content:
                continue
            with pytest.raises(DatasetDigestError):
                verify_dataset_bytes(version, changed)


@pytest.mark.parametrize("getter", ["training", "profiling", "profiling_job", "validation"])
def test_raw_admission_rejects_before_storage_lookup(monkeypatch, getter):
    invalid = verified_raw_version(content_hash_scope="multipart_manifest")
    db = MagicMock()
    db.scalar.return_value = invalid
    store = MagicMock()
    monkeypatch.setattr(validation, "get_object_store", store)
    ids = (uuid.uuid4(), uuid.uuid4(), uuid.uuid4())
    with pytest.raises(DatasetDigestError):
        {
            "training": lambda: training._get_dataset_version(db, *ids[:2]),
            "profiling": lambda: profiling._get_project_dataset_version(db, *ids),
            "profiling_job": lambda: profiling_jobs._get_version(db, *ids),
            "validation": lambda: validation._dataset_version(db, *ids[:2]),
        }[getter]()
    store.assert_not_called()
    db.commit.assert_not_called()


@pytest.mark.parametrize("consumer", ["training", "profiling", "splitter"])
def test_raw_consumption_rejects_before_provider_work(monkeypatch, consumer):
    version = verified_raw_version(content_hash_verification_status="verified_legacy")
    module = {"training": pipeline, "profiling": profiling, "splitter": preparation_worker}[
        consumer
    ]
    store = MagicMock()
    monkeypatch.setattr(module, "get_object_store", store)
    with pytest.raises(DatasetDigestError):
        {
            "training": pipeline._load_dataframe,
            "profiling": profiling._load_rows,
            "splitter": preparation_worker._verify_raw,
        }[consumer](version)
    store.assert_not_called()


@pytest.mark.parametrize("consumer", ["training", "profiling"])
def test_mutated_raw_bytes_fail_before_parsing_or_warning_fallback(monkeypatch, consumer):
    version = verified_raw_version(
        b"original",
        object_uri="secret://path",
        original_filename="raw.csv",
        format=DatasetFormat.CSV,
    )
    module = pipeline if consumer == "training" else profiling
    store = MagicMock()
    store.read_bytes.return_value = b"changed!"
    monkeypatch.setattr(module, "get_object_store", lambda: store)
    parser = MagicMock()
    monkeypatch.setattr(module.pd, "read_csv", parser)
    if consumer == "profiling":
        monkeypatch.setattr(profiling, "_load_csv_rows", parser)
    with pytest.raises(DatasetDigestError):
        (pipeline._load_dataframe if consumer == "training" else profiling._load_rows)(version)
    parser.assert_not_called()


def test_domain_error_is_stable_api_conflict_without_object_paths():
    from automl_api.main import create_app

    app = create_app()

    @app.get("/__digest_test")
    def probe():
        require_verified_digest(SimpleNamespace(object_uri="secret://private-object"))

    response = TestClient(app).get("/__digest_test")
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "dataset_digest_unverified"
    assert "secret" not in response.text


@pytest.mark.parametrize("content,declared_size", [(b"", 1), (b"larger", 3)])
def test_splitter_counts_bytes_even_when_hash_matches_misleading_header(
    monkeypatch, content, declared_size
):
    version = verified_raw_version(content, byte_size=declared_size, object_uri="memory://raw")
    source = io.BytesIO(content)
    store = MagicMock()
    store.stat.return_value = SimpleNamespace(byte_size=declared_size)
    store.open_stream.return_value = source
    monkeypatch.setattr(preparation_worker, "get_object_store", lambda: store)
    with pytest.raises(ValueError, match="byte size"):
        preparation_worker._verify_raw(version)
    assert source.closed


@pytest.mark.parametrize("invalid_side", ["current", "reference"])
def test_drift_rejects_unverified_reference_or_current_before_sampling(monkeypatch, invalid_side):
    from automl_api.training import analysis

    reference_id = uuid.uuid4()
    run = SimpleNamespace(
        project_id=uuid.uuid4(), params={"reference_dataset_version_id": str(reference_id)}
    )
    current = verified_raw_version()
    reference = verified_raw_version(project_id=run.project_id)
    (current if invalid_side == "current" else reference).content_hash_scope = "multipart_manifest"
    db = MagicMock()
    db.__enter__.return_value = db
    db.get.return_value = reference
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    sampler = MagicMock()
    monkeypatch.setattr(analysis, "sample_source", sampler)
    with pytest.raises(DatasetDigestError):
        analysis._execute_drift(run, current)
    sampler.assert_not_called()
