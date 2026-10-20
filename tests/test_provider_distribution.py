"""Central distribution signing, role isolation and independent consumer references."""

import copy
import hashlib
import json
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from automl_api.models.enums import FinalTestStatus
from automl_api.models.qualification import FinalTestAllocation, FinalTestAuthorityReceipt
from automl_api.qualification_control import create_app
from automl_api.services import final_test_authority as authority
from automl_api.services import provider_distribution as distribution
from automl_api.services.final_test_credentials import FinalDataManifest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from test_qualification_control import authority_api  # noqa: F401


@pytest.fixture
def distribution_case(authority_api):  # noqa: F811
    _, scope, kwargs = authority_api
    project = kwargs["principals"][0].project_reference
    manifest = FinalDataManifest(
        project_reference=project,
        scope_id=scope,
        split_digest=hashlib.sha256(scope.bytes).hexdigest(),
        provider="aws",
        bucket="private-locked-final-bucket",
        objects=[
            {
                "role": role,
                "key": "private-locked-final/" + role,
                "version": "immutable-version",
                "sha256": value * 64,
                "byte_size": 17,
            }
            for role, value in [("inputs", "a"), ("labels", "f")]
        ],
    )
    request = {
        "fixture_revision": "fixture-v1",
        "split_revision": "split-v1",
        "copies": [
            {
                "provider": provider,
                "schema_digest": "b" * 64,
                "objects": [
                    {
                        "role": role,
                        "uri": root + "/safe/roles/split_role=" + role + "/part.parquet",
                        "version": "1",
                        "sha256": value * 64,
                        "byte_size": 12,
                        "rows": 2,
                        "row_digest": value * 64,
                    }
                    for role, value in [("train", "c"), ("validation", "d")]
                ],
            }
            for provider, root in [
                ("aws", "s3://safe-aws"),
                ("gcp", "gs://safe-gcp"),
                ("azure", "az://account/safe-container"),
            ]
        ],
    }
    with TestClient(
        create_app(**kwargs, manifests=[manifest]), headers={"Authorization": "Bearer allocator"}
    ) as client:
        allocation = client.post(
            "/allocations",
            json={
                "split_digest": manifest.split_digest,
                "scope_id": str(scope),
                "canonical_provider": "aws",
                "provider_manifest_digest": manifest.digest,
                "fixture_revision": request["fixture_revision"],
                "split_revision": request["split_revision"],
            },
        )
        assert allocation.status_code == 200, allocation.text
        allocation_id = uuid.UUID(allocation.json()["allocation_id"])
        path = f"/allocations/{allocation_id}/distributions"
        refs = dict(
            allocation_id=allocation_id,
            project_reference=project,
            scope_id=scope,
            canonical_provider="aws",
            fixture_revision="fixture-v1",
            split_revision="split-v1",
            split_digest=manifest.split_digest,
        )
        yield client, kwargs, manifest, request, path, refs


def count(case):
    _, kwargs, _, _, _, refs = case
    with kwargs["session_factory"]() as db:
        return db.scalar(
            select(func.count())
            .select_from(FinalTestAuthorityReceipt)
            .where(
                FinalTestAuthorityReceipt.allocation_id == refs["allocation_id"],
                FinalTestAuthorityReceipt.operation.like("distribution_%"),
            )
        )


def issued(case):
    client, kwargs, _, request, path, _ = case
    response = client.post(path, json=request)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    return response.json()


def test_three_signed_manifests_replay_and_never_disclose_final_locations_to_other_providers(
    distribution_case,
):
    case = distribution_case
    client, kwargs, final, request, path, refs = case
    receipts = issued(case)
    assert len(receipts) == 3 and count(case) == 3
    assert client.post(path, json=request).json() == receipts and count(case) == 3
    for receipt, provider in zip(receipts, distribution.PROVIDERS, strict=True):
        manifest = distribution.verify_distribution(
            receipt, kwargs["signing_secret"].public_key(), provider=provider, **refs
        )
        assert manifest.provider == provider
        assert set(manifest.safe_data.logical_roles()[1]) == {"train", "validation"}
        if provider == "aws":
            assert manifest.final_data == final and manifest.mode == "promotional"
        else:
            assert manifest.final_data is None and manifest.mode == "validation_only"
            raw = json.dumps(receipt)
            assert final.bucket not in raw and all(o.key not in raw for o in final.objects)
            assert len(manifest.final_role_digests.inputs) == 64


@pytest.mark.parametrize(
    "field",
    [
        "allocation_id",
        "scope_id",
        "project_reference",
        "provider",
        "canonical_provider",
        "fixture_revision",
        "split_revision",
        "split_digest",
    ],
)
def test_consumer_requires_separately_trusted_references(distribution_case, field):
    _, kwargs, _, _, _, refs = distribution_case
    receipt = issued(distribution_case)[1]
    values = {**refs, "provider": "gcp"}
    values[field] = (
        uuid.uuid4()
        if field.endswith("_id")
        else "azure"
        if field in {"provider", "canonical_provider"}
        else "different"
    )
    with pytest.raises(ValueError):
        distribution.verify_distribution(receipt, kwargs["signing_secret"].public_key(), **values)


def resign(receipt, key):
    body = authority._receipt_bytes(
        **{
            k: receipt[k]
            for k in ["allocation_id", "operation", "provider", "request_digest", "payload"]
        }
    )
    receipt["receipt_digest"] = hashlib.sha256(body).hexdigest()
    receipt["signature"] = key.sign(body).hex()
    return receipt


@pytest.mark.parametrize(
    "mutation",
    [
        "signature",
        "key",
        "algorithm",
        "request_hash",
        "outer_provider",
        "outer_allocation",
        "operation",
        "payload",
        "extra",
        "missing_default",
        "number_type",
        "final_location",
    ],
)
def test_consumer_rejects_tampering_and_validly_signed_inconsistent_envelopes(
    distribution_case,
    mutation,
):
    _, kwargs, _, _, _, refs = distribution_case
    receipt = copy.deepcopy(issued(distribution_case)[1])
    key = kwargs["signing_secret"]
    public = key.public_key()
    if mutation == "signature":
        receipt["signature"] = "0" * 128
    elif mutation == "key":
        public = Ed25519PrivateKey.generate().public_key()
    elif mutation == "algorithm":
        receipt["signature_algorithm"] = "hmac-sha256"
    elif mutation == "request_hash":
        receipt["request_digest"] = "0" * 64
        resign(receipt, key)
    elif mutation == "outer_provider":
        receipt["provider"] = "azure"
        resign(receipt, key)
    elif mutation == "outer_allocation":
        receipt["allocation_id"] = str(uuid.uuid4())
        resign(receipt, key)
    elif mutation == "operation":
        receipt["operation"] = "distribution_azure"
        resign(receipt, key)
    elif mutation == "payload":
        receipt["payload"]["manifest"]["safe_data"]["objects"][0]["sha256"] = "0" * 64
    elif mutation == "extra":
        receipt["payload"]["manifest"]["unknown"] = "hidden"
    elif mutation == "missing_default":
        del receipt["payload"]["manifest"]["final_data"]
    elif mutation == "number_type":
        receipt["payload"]["manifest"]["safe_data"]["objects"][0]["byte_size"] = 12.0
    else:
        receipt["payload"]["manifest"]["final_data"] = issued(distribution_case)[0]["payload"][
            "manifest"
        ]["final_data"]
    with pytest.raises(ValueError):
        distribution.verify_distribution(receipt, public, provider="gcp", **refs)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_provider",
        "duplicate_provider",
        "schema",
        "row_count",
        "row_digest",
        "final_role",
        "query",
        "traversal",
        "backend",
        "generation",
        "bool_size",
    ],
)
def test_invalid_or_logically_different_provider_copies_are_rejected(distribution_case, mutation):
    client, _, _, request, path, _ = distribution_case
    request = copy.deepcopy(request)
    item = request["copies"][1]["objects"][0]
    if mutation == "missing_provider":
        request["copies"].pop()
    elif mutation == "duplicate_provider":
        request["copies"][1] = copy.deepcopy(request["copies"][0])
    elif mutation == "schema":
        request["copies"][1]["schema_digest"] = "1" * 64
    elif mutation == "row_count":
        item["rows"] = 3
    elif mutation == "row_digest":
        item["row_digest"] = "1" * 64
    elif mutation == "final_role":
        item["role"] = "inputs"
    elif mutation == "query":
        item["uri"] += "?token=private"
    elif mutation == "traversal":
        item["uri"] = item["uri"].replace("/safe/", "/safe/%2e%2e/")
    elif mutation == "backend":
        item["uri"] = item["uri"].replace("gs://", "s3://")
    elif mutation == "generation":
        item["version"] = "null"
    else:
        item["byte_size"] = True
    assert client.post(path, json=request).status_code == 422
    assert count(distribution_case) == 0


def test_partition_layout_can_differ_while_logical_rows_match(distribution_case):
    client, _, _, request, path, _ = distribution_case
    request = copy.deepcopy(request)
    original = request["copies"][1]["objects"][0]
    other = {
        **original,
        "uri": original["uri"].replace("part.parquet", "part-2.parquet"),
        "rows": 1,
        "row_digest": "0" * 64,
        "sha256": "1" * 64,
    }
    original["rows"] = 1
    request["copies"][1]["objects"].append(other)
    assert client.post(path, json=request).status_code == 200


@pytest.mark.parametrize("caller", ["aws", "gcp", "azure", "other-project", "other-scope"])
def test_provider_identities_cannot_issue_distribution(distribution_case, caller):
    client, _, _, request, path, _ = distribution_case
    assert (
        client.post(path, json=request, headers={"Authorization": "Bearer " + caller}).status_code
        == 403
    )
    assert count(distribution_case) == 0


def test_project_and_installed_manifest_are_required(distribution_case):
    client, kwargs, _, request, path, refs = distribution_case
    allocator = kwargs["principals"][0].model_copy(
        update={"project_reference": "different-project"}
    )
    with TestClient(
        create_app(**{**kwargs, "principals": [allocator]}, manifests=[]),
        headers={"Authorization": "Bearer allocator"},
    ) as other:
        assert other.post(path, json=request).status_code == 403
    with TestClient(
        create_app(**kwargs, manifests=[]), headers={"Authorization": "Bearer allocator"}
    ) as other:
        assert other.post(path, json=request).status_code == 409
    assert count(distribution_case) == 0


@pytest.mark.parametrize("late", ["opened", "refit", "partial"])
def test_first_distribution_cannot_follow_results_or_fill_partial_history(distribution_case, late):
    client, kwargs, _, request, path, refs = distribution_case
    with kwargs["session_factory"]() as db, db.begin():
        allocation = db.get(FinalTestAllocation, refs["allocation_id"])
        if late == "opened":
            allocation.status = FinalTestStatus.OPENED
        else:
            authority._signed_receipt(
                db,
                allocation=allocation,
                operation="refit_published" if late == "refit" else "distribution_aws",
                provider="aws",
                request_digest="a" * 64,
                signing_secret=kwargs["signing_secret"],
                payload={},
            )
    assert client.post(path, json=request).status_code == 409
    assert count(distribution_case) == (1 if late == "partial" else 0)


def test_identical_replay_remains_available_after_open_without_new_signatures(distribution_case):
    client, kwargs, _, request, path, refs = distribution_case
    receipts = issued(distribution_case)
    with kwargs["session_factory"]() as db, db.begin():
        db.get(FinalTestAllocation, refs["allocation_id"]).status = FinalTestStatus.OPENED
    assert client.post(path, json=request).json() == receipts and count(distribution_case) == 3


@pytest.mark.parametrize("conflict", [False, True])
def test_concurrent_issuance_serializes_whole_distribution(distribution_case, conflict):
    client, _, _, request, path, _ = distribution_case
    barrier = Barrier(6)

    def post(i):
        value = copy.deepcopy(request)
        if conflict and i % 2:
            value["fixture_revision"] = "competing"
        barrier.wait(timeout=10)
        return client.post(path, json=value)

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(post, range(6)))
    assert sorted(r.status_code for r in responses) == (
        [200] * 3 + [409] * 3 if conflict else [200] * 6
    )
    successful = [r.json() for r in responses if r.status_code == 200]
    assert all(v == successful[0] for v in successful) and count(distribution_case) == 3


def test_connection_loss_before_second_receipt_rolls_back_whole_distribution(
    distribution_case,
    monkeypatch,
):
    client, kwargs, _, request, path, _ = distribution_case
    original = authority._signed_receipt
    calls = []

    def write(db, **values):
        calls.append(True)
        if len(calls) == 2:
            assert make_url(str(db.get_bind().url)).database.endswith("_tests")
            pid = db.scalar(text("SELECT pg_backend_pid()"))
            with kwargs["session_factory"]() as other, other.begin():
                assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
        return original(db, **values)

    monkeypatch.setattr(authority, "_signed_receipt", write)
    assert client.post(path, json=request).status_code == 503
    assert count(distribution_case) == 0
    monkeypatch.setattr(authority, "_signed_receipt", original)
    assert client.post(path, json=request).status_code == 200 and count(distribution_case) == 3


def test_cli_uses_trusted_references_and_never_prints_locations(distribution_case, tmp_path):
    _, kwargs, _, _, _, refs = distribution_case
    receipt = issued(distribution_case)[1]
    receipt_path = tmp_path / "distribution.json"
    receipt_path.write_text(json.dumps(receipt))
    public = tmp_path / "public.pem"
    public.write_bytes(
        kwargs["signing_secret"]
        .public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    )
    command = [
        sys.executable,
        str(Path("scripts/validate_provider_distribution.py")),
        str(receipt_path),
        "--public-key",
        str(public),
        "--provider",
        "gcp",
    ]
    for key, value in refs.items():
        command += ["--" + key.replace("_", "-"), str(value)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "verified": True,
        "revision": "provider-distribution-v1",
        "provider": "gcp",
        "mode": "validation_only",
        "safe_objects": 2,
    }
    assert "gs://" not in result.stdout and "private-locked-final" not in result.stdout
    receipt["payload"]["manifest"]["safe_data"]["objects"][0]["uri"] += "?private-secret"
    receipt_path.write_text(json.dumps(receipt))
    failed = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert failed.returncode == 1 and failed.stdout == ""
    assert failed.stderr == "Provider distribution verification failed\n"


@pytest.mark.parametrize("provider", ["aws", "gcp", "azure"])
def test_known_final_bytes_cannot_be_declared_as_safe_copy(distribution_case, provider):
    client, _, final, request, path, _ = distribution_case
    request = copy.deepcopy(request)
    item = next(c for c in request["copies"] if c["provider"] == provider)["objects"][0]
    item["sha256"] = final.objects[0].sha256
    assert client.post(path, json=request).status_code == 409
    assert count(distribution_case) == 0


@pytest.mark.parametrize("scheme", ["s3", "s3c"])
def test_known_final_location_cannot_be_disguised_as_train_role(distribution_case, scheme):
    _, kwargs, final, request, path, refs = distribution_case
    key = "locked/roles/split_role=train/part.parquet"
    final = final.model_copy(
        update={
            "objects": (
                final.objects[0].model_copy(update={"key": key}),
                final.objects[1],
            )
        }
    )
    with kwargs["session_factory"]() as db, db.begin():
        db.get(FinalTestAllocation, refs["allocation_id"]).provider_manifest_digest = final.digest
    request = copy.deepcopy(request)
    request["copies"][0]["objects"][0]["uri"] = f"{scheme}://{final.bucket}/{key}"
    with TestClient(
        create_app(**kwargs, manifests=[final]), headers={"Authorization": "Bearer allocator"}
    ) as client:
        assert client.post(path, json=request).status_code == 409
    assert count(distribution_case) == 0


def test_unknown_allocation_fails_without_server_error(distribution_case):
    client, _, _, request, _, _ = distribution_case
    assert (
        client.post(f"/allocations/{uuid.uuid4()}/distributions", json=request).status_code == 404
    )
    assert count(distribution_case) == 0


def test_distribution_then_registered_evaluation_keeps_one_shot_credentials(distribution_case):
    from unittest.mock import Mock

    _, kwargs, final, request, path, refs = distribution_case
    mint = Mock(return_value=["https://objects.test/inputs", "https://objects.test/labels"])
    with TestClient(
        create_app(**kwargs, manifests=[final], mint=mint),
        headers={"Authorization": "Bearer allocator"},
    ) as client:
        receipts = client.post(path, json=request)
        assert receipts.status_code == 200
        allocation_path = path.removesuffix("/distributions")
        published = client.post(
            allocation_path + "/refit",
            json={
                "refit_attempt_id": str(uuid.uuid4()),
                "frozen_pipeline_digest": "d" * 64,
                "frozen_pipeline_uri": "s3://safe-aws/projects/synthetic/pipeline.joblib",
                "refit_policy_digest": "e" * 64,
            },
        )
        assert published.status_code == 200, published.text
        registered = client.post(
            allocation_path + "/evaluators",
            json={
                "evaluator_attempt_id": str(uuid.uuid4()),
                "expected_generation": 0,
            },
        )
        assert registered.status_code == 200, registered.text
        header = {"Authorization": "Bearer " + registered.json()["access_token"]}
        payload = {"provider_manifest_digest": final.digest}
        grant = client.post(allocation_path + "/credentials", headers=header, json=payload)
        assert grant.status_code == 200, grant.text
        assert (
            client.post(allocation_path + "/credentials", headers=header, json=payload).status_code
            == 409
        )
        committed = client.post(
            allocation_path + "/commit", headers=header, json={**payload, "result_digest": "a" * 64}
        )
        assert committed.status_code == 200, committed.text
        assert client.post(path, json=request).json() == receipts.json()
        mint.assert_called_once()
        assert mint.call_args.args[0] == final
        with kwargs["session_factory"]() as db:
            assert (
                db.get(FinalTestAllocation, refs["allocation_id"]).status
                == FinalTestStatus.COMMITTED
            )
        assert count(distribution_case) == 3
