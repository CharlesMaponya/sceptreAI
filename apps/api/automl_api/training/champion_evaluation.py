"""Evaluate one frozen pipeline; verify a signed result without reopening final data."""

from __future__ import annotations

import hashlib
import json
import tempfile
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Annotated, Literal
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, Request, build_opener

import joblib
import pandas as pd
import pyarrow.parquet as pq
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator

from automl_api.models.enums import TaskType
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.final_test_credentials import FinalDataManifest, FinalDataTemplate
from automl_api.services.temporal import normalize_temporal_features
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.storage.contracts import ObjectMetadata
from automl_api.training.champion_refit import Digest, Positive, RefitObject, _copy_verified
from automl_api.training.evaluation import (
    classification_evaluation,
    regression_evaluation,
    resolve_primary_metric,
)
from automl_api.training.refit_worker import _NoRedirect, _tls_context


class EvaluationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    target_column: str
    task_type: Literal["regression", "classification"]
    primary_metric: str
    positive_label: str | None = None
    final_rows: Positive
    final_row_digest: Digest
    max_decoded_bytes: Positive
    max_input_bytes: Positive
    max_model_bytes: Positive

    @model_validator(mode="after")
    def validate_policy(self):
        if self.target_column in {"", "row_id", "split_role", "source_ordinal"}:
            raise ValueError("Evaluation target must be a data column")
        resolve_primary_metric(TaskType(self.task_type), self.primary_metric)
        return self


class EvaluationDataPolicy(EvaluationPolicy):
    final_data: FinalDataTemplate

    @model_validator(mode="after")
    def validate_data_budget(self):
        if sum(o.byte_size for o in self.final_data.objects) > self.max_input_bytes:
            raise ValueError("Final objects exceed the registered input budget")
        return self


class EvaluationPlan(EvaluationPolicy):
    project_id: uuid.UUID
    scope_id: uuid.UUID
    attempt_id: uuid.UUID
    allocation_id: uuid.UUID
    frozen_pipeline: RefitObject
    final_manifest: FinalDataManifest
    result_public_key: Digest  # Raw 32-byte Ed25519 public key, registered before execution.

    @model_validator(mode="after")
    def validate_execution(self):
        if self.final_manifest.scope_id != self.scope_id:
            raise ValueError("Evaluation manifest belongs to another scope")
        if sum(o.byte_size for o in self.final_manifest.objects) > self.max_input_bytes:
            raise ValueError("Final objects exceed the registered input budget")
        if self.frozen_pipeline.byte_size > self.max_model_bytes:
            raise ValueError("Frozen pipeline exceeds the registered model budget")
        return self

    @property
    def digest(self):
        return canonical_request_hash(self.model_dump(mode="json"))


class EvaluationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    revision: Literal["sceptre-final-result-v1"] = "sceptre-final-result-v1"
    plan_digest: Digest
    project_id: uuid.UUID
    scope_id: uuid.UUID
    attempt_id: uuid.UUID
    allocation_id: uuid.UUID
    frozen_pipeline_digest: Digest
    final_manifest_digest: Digest
    final_row_digest: Digest
    final_rows: Positive
    primary_metric: str
    metrics: dict[str, FiniteFloat]


class SignedEvaluationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    result: EvaluationResult
    signature: Annotated[str, Field(pattern=r"^[0-9a-f]{128}$")]


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _binding(plan):
    return dict(
        plan_digest=plan.digest,
        project_id=plan.project_id,
        scope_id=plan.scope_id,
        attempt_id=plan.attempt_id,
        allocation_id=plan.allocation_id,
        frozen_pipeline_digest=plan.frozen_pipeline.sha256,
        final_manifest_digest=plan.final_manifest.digest,
        final_row_digest=plan.final_row_digest,
        final_rows=plan.final_rows,
        primary_metric=plan.primary_metric,
    )


def verify_result(payload: bytes, plan: EvaluationPlan, *, expected_digest: str):
    """Recovery reads only this bounded artifact, never the final-data grant."""
    if len(payload) > 65536 or hashlib.sha256(payload).hexdigest() != expected_digest:
        raise ValueError("Evaluation result size or byte digest differs")
    signed = SignedEvaluationResult.model_validate_json(payload)
    if payload != _canonical(signed.model_dump(mode="json")):
        raise ValueError("Evaluation result is not canonical")
    actual = signed.result.model_dump()
    if any(actual[key] != value for key, value in _binding(plan).items()):
        raise ValueError("Evaluation result belongs to another execution plan")
    if plan.primary_metric not in signed.result.metrics:
        raise ValueError("Evaluation result lacks its primary metric")
    try:
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(plan.result_public_key)).verify(
            bytes.fromhex(signed.signature), _canonical(signed.result.model_dump(mode="json"))
        )
    except InvalidSignature:
        raise ValueError("Evaluation result signature is invalid") from None
    return signed.result


def _final_uri(manifest, obj):
    if manifest.provider == "azure":
        return f"azure://{manifest.bucket}/{obj.key}"
    scheme = "gs" if manifest.provider == "gcp" else "s3"
    return f"{scheme}://{manifest.bucket}/{obj.key}"


class GrantedFinalStore:
    """Read only exact objects from one verified grant; never retry a consumed URL."""

    def __init__(self, plan, grant, authority_public_key, *, ca_file=None):
        try:
            receipt = SimpleNamespace(**grant["receipt"])
            manifest = FinalDataManifest.model_validate(grant["manifest"])
            expires = datetime.fromisoformat(grant["expires_at"])
            urls = grant["urls"]
            binding = receipt.payload
            valid = (
                receipt.signature_algorithm == "ed25519"
                and verify_receipt(receipt, authority_public_key)
                and receipt.operation == "grant_issued"
                and str(receipt.allocation_id) == str(plan.allocation_id)
                and receipt.provider == plan.final_manifest.provider
                and manifest == plan.final_manifest
                and binding["manifest_digest"] == manifest.digest
                and binding["evaluator_attempt_id"] == str(plan.attempt_id)
                and binding["frozen_pipeline_digest"] == plan.frozen_pipeline.sha256
                and datetime.fromisoformat(binding["expires_at"]) == expires
                and expires > datetime.now(UTC)
                and len(urls) == len(manifest.objects)
                and binding["capabilities_digest"]
                == hashlib.sha256(json.dumps(urls, separators=(",", ":")).encode()).hexdigest()
            )
            for url in urls:
                parsed = urlsplit(url)
                valid = valid and parsed.scheme == "https" and bool(parsed.hostname)
                valid = valid and not (parsed.username or parsed.password or parsed.fragment)
            if not valid:
                raise ValueError("Invalid grant")
        except (KeyError, TypeError, ValueError, AttributeError):
            raise ValueError("Final-data grant signature, expiry or lineage is invalid") from None
        self.expires_at = expires
        self.objects = {
            _final_uri(manifest, obj): (obj, url)
            for obj, url in zip(manifest.objects, urls, strict=True)
        }
        self.opened = set()
        # No authority/control bearer headers or environment credentials in this transport.
        self.opener = build_opener(_NoRedirect(), HTTPSHandler(context=_tls_context(ca_file)))

    def stat(self, uri):
        obj, _ = self.objects[uri]
        return ObjectMetadata(uri=uri, byte_size=obj.byte_size)

    def open_stream(self, uri):
        _, url = self.objects[uri]
        if uri in self.opened or datetime.now(UTC) >= self.expires_at:
            raise ValueError("Final-data capability was consumed or expired")
        # Consume before I/O: a transport failure never causes another evaluation read.
        self.opened.add(uri)
        return self.opener.open(Request(url, method="GET"), timeout=60)


def execute_evaluation(plan, pipeline_store, final_store, *, signing_key, progress=lambda: None):
    """Predict once on bounded registered rows and return a signed aggregate result.

    final_store must be an adapter for the single authority-issued grant, pinned
    to manifest object versions. This function neither mints nor retries grants.
    The caller durably writes these bytes before committing their digest.
    """
    if signing_key.public_key().public_bytes_raw().hex() != plan.result_public_key:
        raise ValueError("Result signer differs from the registered evaluator key")
    with tempfile.TemporaryFile() as stream:
        _copy_verified(pipeline_store, plan.frozen_pipeline, stream, progress)
        fitted = joblib.load(stream)
    features = list(getattr(fitted, "feature_names_in_", []))
    if not features or plan.target_column in features or len(set(features)) != len(features):
        raise ValueError("Frozen pipeline input schema is invalid")
    if any(name in {"row_id", "split_role", "source_ordinal"} for name in features):
        raise ValueError("Frozen pipeline includes row metadata as features")
    if plan.task_type == "classification":
        classes = list(getattr(fitted, "classes_", []))
        if not classes or (len(classes) == 2 and plan.positive_label not in map(str, classes)):
            raise ValueError("Classification requires frozen classes and a binary positive label")
    frames = {"inputs": [], "labels": []}
    identities = {"inputs": set(), "labels": set()}
    decoded = pandas_bytes = 0
    for obj in plan.final_manifest.objects:
        item = RefitObject(
            uri=_final_uri(plan.final_manifest, obj), sha256=obj.sha256, byte_size=obj.byte_size
        )
        with tempfile.TemporaryFile() as stream:
            _copy_verified(final_store, item, stream, progress)
            parquet = pq.ParquetFile(stream)
            decoded += sum(
                parquet.metadata.row_group(i).total_byte_size
                for i in range(parquet.metadata.num_row_groups)
            )
            if (
                decoded > plan.max_decoded_bytes
                or len(identities[obj.role]) + parquet.metadata.num_rows > plan.final_rows
            ):
                raise ValueError("Final data exceeds registered rows or decoded budget")
            for batch in parquet.iter_batches(batch_size=8192):
                frame = batch.to_pandas()
                pandas_bytes += int(frame.memory_usage(index=True, deep=True).sum())
                if pandas_bytes > plan.max_decoded_bytes:
                    raise ValueError("Final dataframe exceeds registered memory budget")
                if "row_id" not in frame or frame["row_id"].isna().any():
                    raise ValueError("Final data lacks row identities")
                for row_id in frame["row_id"]:
                    if not isinstance(row_id, str) or row_id in identities[obj.role]:
                        raise ValueError("Final row identities are invalid or duplicated")
                    identities[obj.role].add(row_id)
                if obj.role == "inputs":
                    if plan.target_column in frame or not set(features) <= set(frame):
                        raise ValueError("Final inputs contain labels or lack frozen features")
                    if "split_role" in frame and not frame["split_role"].eq("final_test").all():
                        raise ValueError("Final inputs contain another split role")
                    frame = frame[["row_id", *features]]
                elif set(frame) != {"row_id", plan.target_column}:
                    raise ValueError("Final labels have an unexpected schema")
                frames[obj.role].append(frame)
                progress()
    if identities["inputs"] != identities["labels"] or len(identities["inputs"]) != plan.final_rows:
        raise ValueError("Final input and label row identities differ")
    row_xor = 0
    for row_id in identities["inputs"]:
        row_xor ^= int.from_bytes(hashlib.sha256(row_id.encode()).digest(), "big")
    if f"{row_xor:064x}" != plan.final_row_digest:
        raise ValueError("Final row digest differs from preregistration")
    inputs = pd.concat(frames["inputs"], ignore_index=True).set_index("row_id").sort_index()
    labels = pd.concat(frames["labels"], ignore_index=True).set_index("row_id")
    target = labels.loc[inputs.index, plan.target_column]
    if target.isna().any():
        raise ValueError("Final labels contain missing values")
    x = normalize_temporal_features(inputs[features])
    progress()
    predictions = fitted.predict(x)
    if plan.task_type == "classification":
        metrics, _ = classification_evaluation(fitted, x, target, predictions, plan.positive_label)
    else:
        metrics, _ = regression_evaluation(
            pd.Series(dtype=float), target, predictions, TaskType.REGRESSION
        )
    result = EvaluationResult(**_binding(plan), metrics=metrics)
    if plan.primary_metric not in result.metrics:
        raise ValueError("Final evaluation did not produce its required metric")
    progress()
    signed = SignedEvaluationResult(
        result=result, signature=signing_key.sign(_canonical(result.model_dump(mode="json"))).hex()
    )
    payload = _canonical(signed.model_dump(mode="json"))
    verify_result(payload, plan, expected_digest=hashlib.sha256(payload).hexdigest())
    return payload
