"""Reject blank upload identity while preserving existing text coercion and normalization."""

import pytest
from automl_api.schemas.datasets import DatasetUploadRequest
from pydantic import ValidationError


@pytest.mark.parametrize("field", ["dataset_name", "filename"])
@pytest.mark.parametrize("value", [" ", "\n", "\t"])
def test_whitespace_only_upload_identity_is_rejected(field, value):
    payload = {"dataset_name": "customers", "filename": "data.csv", field: value}
    with pytest.raises(ValidationError):
        DatasetUploadRequest.model_validate(payload)


@pytest.mark.parametrize("field", ["dataset_name", "filename"])
@pytest.mark.parametrize("value", [None, 123, []])
def test_nontext_upload_identity_is_a_validation_error(field, value):
    payload = {"dataset_name": "customers", "filename": "data.csv", field: value}
    with pytest.raises(ValidationError):
        DatasetUploadRequest.model_validate(payload)


def test_valid_upload_identity_keeps_existing_normalization():
    result = DatasetUploadRequest(dataset_name=" customers ", filename=" data.csv ")
    assert result.dataset_name == "customers" and result.filename == "data.csv"


@pytest.mark.parametrize("field", ["dataset_name", "filename"])
def test_byte_whitespace_cannot_bypass_normalized_identity_validation(field):
    payload = {"dataset_name": "customers", "filename": "data.csv", field: b" "}
    with pytest.raises(ValidationError):
        DatasetUploadRequest.model_validate(payload)


@pytest.mark.parametrize("field", ["dataset_name", "filename"])
def test_supported_byte_coercion_keeps_existing_normalization(field):
    expected = "customers" if field == "dataset_name" else "data.csv"
    payload = {"dataset_name": "customers", "filename": "data.csv", field: f" {expected} ".encode()}
    assert getattr(DatasetUploadRequest.model_validate(payload), field) == expected
