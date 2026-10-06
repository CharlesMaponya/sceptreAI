from __future__ import annotations

import hashlib
import io
import json
import uuid
from types import SimpleNamespace
from unittest.mock import Mock

import joblib
import pandas as pd
import pytest
from automl_api.qualification_control import create_app
from automl_api.services.final_test_authority import verify_receipt
from automl_api.storage.contracts import ObjectMetadata
from automl_api.training.champion_evaluation import (
    EvaluationPlan,
    GrantedFinalStore,
    execute_evaluation,
    verify_result,
)
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from test_qualification_control import headers

pytest_plugins = ["test_qualification_control"]


@pytest.fixture
def evaluation_case():
    def build(*, classification=False, mutation=None):
        key = Ed25519PrivateKey.generate()
        scope = uuid.uuid4()
        model = Pipeline(
            [
                ("scale", StandardScaler()),
                (
                    "model",
                    LogisticRegression(random_state=7) if classification else LinearRegression(),
                ),
            ]
        )
        model.fit(
            pd.DataFrame({"x": range(20)}),
            [int(i >= 10) if classification else 3 * i + 2 for i in range(20)],
        )
        stream = io.BytesIO()
        joblib.dump(model, stream)
        objects = {"s3://models/frozen.joblib": stream.getvalue()}
        rows = [f"final-{i}" for i in range(10)]
        inputs = pd.DataFrame({"row_id": rows, "x": list(range(5, 15)), "split_role": "final_test"})
        labels = pd.DataFrame(
            {
                "row_id": rows,
                "target": [int(i >= 10) if classification else 3 * i + 2 for i in range(5, 15)],
            }
        )
        if mutation == "duplicate":
            inputs.loc[1, "row_id"] = rows[0]
        elif mutation == "missing":
            labels.loc[1, "row_id"] = "different"
        elif mutation == "leak":
            inputs["target"] = labels["target"]
        elif mutation == "role":
            inputs.loc[0, "split_role"] = "train"
        elif mutation == "null":
            labels.loc[0, "target"] = None
        entries = []
        for role, frame in (("inputs", inputs), ("labels", labels.iloc[::-1])):
            content = frame.to_parquet(index=False)
            objects[f"s3://locked-final/{role}.parquet"] = content
            entries.append(
                dict(
                    role=role,
                    key=f"{role}.parquet",
                    version="1",
                    sha256=hashlib.sha256(content).hexdigest(),
                    byte_size=len(content),
                )
            )
        row_xor = 0
        for row in rows:
            row_xor ^= int.from_bytes(hashlib.sha256(row.encode()).digest(), "big")
        model_bytes = objects["s3://models/frozen.joblib"]
        plan = EvaluationPlan(
            project_id=uuid.uuid4(),
            scope_id=scope,
            attempt_id=uuid.uuid4(),
            allocation_id=uuid.uuid4(),
            frozen_pipeline=dict(
                uri="s3://models/frozen.joblib",
                sha256=hashlib.sha256(model_bytes).hexdigest(),
                byte_size=len(model_bytes),
            ),
            final_manifest=dict(
                project_reference="fixture",
                scope_id=scope,
                split_digest="b" * 64,
                provider="aws",
                bucket="locked-final",
                objects=entries,
            ),
            target_column="target",
            task_type="classification" if classification else "regression",
            primary_metric="accuracy" if classification else "rmse",
            positive_label="1" if classification else None,
            final_rows=10,
            final_row_digest=f"{row_xor:064x}",
            max_decoded_bytes=1000000,
            max_input_bytes=1000000,
            max_model_bytes=1000000,
            result_public_key=key.public_key().public_bytes_raw().hex(),
        )
        store = Mock()
        store.stat.side_effect = lambda uri: ObjectMetadata(uri=uri, byte_size=len(objects[uri]))
        store.open_stream.side_effect = lambda uri: io.BytesIO(objects[uri])
        return plan, key, store, objects

    return build


@pytest.mark.parametrize("classification", [False, True])
def test_frozen_prediction_signed_recovery_without_data(
    evaluation_case, monkeypatch, classification
):
    plan, key, store, _ = evaluation_case(classification=classification)
    # A refit/clone would violate the evaluator contract, even if predictions looked plausible.
    monkeypatch.setattr(Pipeline, "fit", Mock(side_effect=AssertionError("Must never fit")))
    payload = execute_evaluation(plan, store, store, signing_key=key)
    assert store.open_stream.call_count == 3
    store.open_stream.side_effect = AssertionError("Recovery must not read final data")
    store.stat.side_effect = AssertionError("Recovery must not inspect final data")
    result = verify_result(payload, plan, expected_digest=hashlib.sha256(payload).hexdigest())
    assert result.metrics[plan.primary_metric] == pytest.approx(1 if classification else 0)
    assert "predictions" not in json.loads(payload)["result"]
    assert "final-0" not in payload.decode()


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "leak", "role", "null"])
def test_invalid_final_data_never_produces_result(evaluation_case, mutation):
    plan, key, store, _ = evaluation_case(mutation=mutation)
    with pytest.raises(ValueError):
        execute_evaluation(plan, store, store, signing_key=key)


def test_binary_positive_label_cannot_be_selected_from_final_data(evaluation_case):
    plan, key, pipeline_store, _ = evaluation_case(classification=True)
    final_store = Mock()
    final_store.stat.side_effect = AssertionError("Must reject before final data access")
    with pytest.raises(ValueError, match="positive label"):
        execute_evaluation(
            plan.model_copy(update={"positive_label": None}),
            pipeline_store,
            final_store,
            signing_key=key,
        )


@pytest.mark.parametrize("mutation", ["bytes", "rows", "memory", "row_digest", "signer"])
def test_registered_bounds_and_signer_fail_closed(evaluation_case, mutation):
    plan, key, store, objects = evaluation_case()
    if mutation == "bytes":
        objects["s3://locked-final/inputs.parquet"] += b"corruption"
    elif mutation == "rows":
        plan = plan.model_copy(update={"final_rows": 9})
    elif mutation == "memory":
        plan = plan.model_copy(update={"max_decoded_bytes": 1})
    elif mutation == "row_digest":
        plan = plan.model_copy(update={"final_row_digest": "f" * 64})
    else:
        key = Ed25519PrivateKey.generate()
    with pytest.raises(ValueError):
        execute_evaluation(plan, store, store, signing_key=key)


def test_recovery_rejects_changed_result_signature_and_execution(evaluation_case):
    plan, key, store, _ = evaluation_case()
    payload = execute_evaluation(plan, store, store, signing_key=key)
    digest = hashlib.sha256(payload).hexdigest()
    for field, value in (
        ("attempt_id", uuid.uuid4()),
        ("scope_id", uuid.uuid4()),
        ("allocation_id", uuid.uuid4()),
        ("final_row_digest", "f" * 64),
    ):
        with pytest.raises(ValueError):
            verify_result(payload, plan.model_copy(update={field: value}), expected_digest=digest)
    changed = json.loads(payload)
    changed["result"]["metrics"]["rmse"] = 100.0
    forged = json.dumps(changed, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(ValueError, match="signature"):
        verify_result(forged, plan, expected_digest=hashlib.sha256(forged).hexdigest())
    with pytest.raises(ValueError, match="byte digest"):
        verify_result(payload, plan, expected_digest="0" * 64)


def test_authority_grant_evaluation_commit_and_recovery(authority_api, evaluation_case):
    _, scope, kwargs = authority_api
    plan, key, pipeline_store, objects = evaluation_case()
    manifest = plan.final_manifest.model_copy(
        update={"scope_id": scope, "project_reference": f"authority-test-{scope}"}
    )
    plan = plan.model_copy(update={"scope_id": scope, "final_manifest": manifest})
    urls = [f"https://objects.test/{obj.key}?versionId={obj.version}" for obj in manifest.objects]
    mint = Mock(return_value=urls)
    with TestClient(create_app(**kwargs, manifests=[manifest], mint=mint)) as client:
        allocated = client.post(
            "/allocations",
            headers=headers("allocator"),
            json=dict(
                split_digest=manifest.split_digest,
                scope_id=str(scope),
                canonical_provider="aws",
                provider_manifest_digest=manifest.digest,
            ),
        )
        assert allocated.status_code == 200, allocated.text
        allocation_id = allocated.json()["allocation_id"]
        plan = plan.model_copy(update={"allocation_id": uuid.UUID(allocation_id)})
        path = f"/allocations/{allocation_id}"
        assert (
            client.post(
                path + "/refit",
                headers=headers("allocator"),
                json=dict(
                    refit_attempt_id=str(uuid.uuid4()),
                    frozen_pipeline_digest=plan.frozen_pipeline.sha256,
                    frozen_pipeline_uri=plan.frozen_pipeline.uri,
                    refit_policy_digest="c" * 64,
                ),
            ).status_code
            == 200
        )
        registration = client.post(
            path + "/evaluators",
            headers=headers("allocator"),
            json=dict(evaluator_attempt_id=str(plan.attempt_id), expected_generation=0),
        )
        assert registration.status_code == 200
        auth = headers(registration.json()["access_token"])
        opened = {"provider_manifest_digest": manifest.digest}
        response = client.post(path + "/credentials", headers=auth, json=opened)
        assert response.status_code == 200, response.text
        grant = response.json()
        public_key = kwargs["signing_secret"].public_key()
        for mutation in ("signature", "url", "expiry", "manifest"):
            changed = json.loads(json.dumps(grant))
            if mutation == "signature":
                changed["receipt"]["signature"] = "0" * 128
            elif mutation == "url":
                changed["urls"][0] += "&changed=1"
            elif mutation == "expiry":
                changed["expires_at"] = "2099-01-01T00:00:00+00:00"
            else:
                changed["manifest"]["scope_id"] = str(uuid.uuid4())
            with pytest.raises(ValueError, match="grant"):
                GrantedFinalStore(plan, changed, public_key)
        store = GrantedFinalStore(plan, grant, public_key)
        requests = []

        def read(request, timeout):
            assert request.full_url in urls and request.get_method() == "GET"
            assert not request.has_header("Authorization") and timeout == 60
            requests.append(request.full_url)
            obj = manifest.objects[urls.index(request.full_url)]
            return io.BytesIO(objects[f"s3://locked-final/{obj.key}"])

        store.opener = Mock()
        store.opener.open.side_effect = read
        payload = execute_evaluation(plan, pipeline_store, store, signing_key=key)
        assert len(requests) == 2
        with pytest.raises(ValueError, match="consumed"):
            store.open_stream("s3://locked-final/inputs.parquet")
        digest = hashlib.sha256(payload).hexdigest()
        # Simulate process loss after durable artifact publication: recovery only verifies bytes.
        recovered = verify_result(payload, plan, expected_digest=digest)
        assert recovered.metrics["rmse"] == pytest.approx(0)
    with TestClient(create_app(**kwargs, manifests=[manifest], mint=mint)) as restarted:
        commit = {**opened, "result_digest": digest}
        first = restarted.post(path + "/commit", headers=auth, json=commit)
        assert first.status_code == 200, first.text
        assert verify_receipt(SimpleNamespace(**first.json()), public_key)
        assert restarted.post(path + "/commit", headers=auth, json=commit).json() == first.json()
        assert restarted.post(path + "/credentials", headers=auth, json=opened).status_code == 409
        assert (
            restarted.post(
                path + "/commit", headers=auth, json={**commit, "result_digest": "0" * 64}
            ).status_code
            == 409
        )
    mint.assert_called_once()
