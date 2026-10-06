import base64
import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.api.routes import evaluation_control
from automl_api.db.session import get_db
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.security.tokens import TokenError, decode_token
from automl_api.services import evaluation_access
from automl_api.services.evaluation_keys import KEY_FIELD, prepare_evaluator_credentials
from automl_api.training.champion_evaluation import (
    EvaluationResult,
    SignedEvaluationResult,
    _binding,
    _canonical,
)
from cryptography.hazmat.primitives import serialization
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_champion_evaluation_keys import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_keys import handoff_case as handoff_case
from test_champion_evaluation_keys import key_case as key_case

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]
SECRET = "evaluation-control-test-secret-01234567890123456789"


@pytest.fixture
def control_case(key_case, policy_case, monkeypatch):
    (db, scope, factory, authority, authority_key, _), k8s = key_case
    _, store, _, _ = policy_case
    plan, authority_token, _ = prepare_evaluator_credentials(
        factory, scope.id, k8s, authority, authority_public_key=authority_key
    )
    attempt = db.get(WorkflowAttempt, plan.attempt_id)
    attempt.status = AttemptStatus.SUBMITTED
    db.flush()
    monkeypatch.setattr(
        evaluation_access, "get_settings", lambda: SimpleNamespace(jwt_secret_key=SECRET)
    )
    token = evaluation_access.evaluation_token(attempt, scope.scope_deadline_at)
    monkeypatch.setattr(evaluation_control, "get_session_factory", lambda: factory)
    monkeypatch.setattr(evaluation_control, "get_object_store", lambda: store)
    monkeypatch.setattr(evaluation_control, "authority_public_key", lambda: authority_key)

    def mint(actual_store, item, deadline):
        assert actual_store is store and item == plan.frozen_pipeline
        assert 0 < (deadline - datetime.now(UTC)).total_seconds() <= 90
        return {"url": "https://objects.test/frozen", "expires_at": deadline.isoformat()}

    monkeypatch.setattr(evaluation_control, "mint_refit_read_url", mint)
    signer = serialization.load_pem_private_key(
        base64.b64decode(k8s.core.secret.data[KEY_FIELD]), password=None
    )
    result = EvaluationResult(**_binding(plan), metrics={"rmse": 1.0})
    signed = SignedEvaluationResult(
        result=result, signature=signer.sign(_canonical(result.model_dump(mode="json"))).hex()
    )
    payload = _canonical(signed.model_dump(mode="json"))
    app = FastAPI()
    app.include_router(evaluation_control.router, prefix="/api/v1")

    def database():
        with factory() as session:
            yield session

    app.dependency_overrides[get_db] = database
    with TestClient(
        app,
        base_url=f"https://app.test/api/v1/internal/evaluations/{attempt.id}/",
        headers={"Authorization": f"Bearer {token}"},
    ) as client:
        yield client, db, scope, attempt, plan, payload, authority, authority_token, store, token


def test_scoped_result_upload_and_authority_receipt_complete_once(control_case):
    client, db, scope, attempt, plan, payload, authority, authority_token, _, token = control_case
    with pytest.raises(TokenError):
        decode_token(token, secret=SECRET, expected_type="access")
    assert client.post("start").json() == plan.model_dump(mode="json")
    assert client.post("start").status_code == 409
    grant = client.get("pipeline")
    assert grant.status_code == 200 and grant.headers["cache-control"] == "no-store"
    assert client.post("heartbeat").json() == {"renewed": True}
    uploaded = client.put("result", content=payload)
    assert uploaded.status_code == 200, uploaded.text
    assert uploaded.json()["sha256"] == hashlib.sha256(payload).hexdigest()
    assert client.put("result", content=payload).json() == uploaded.json()
    # Synthetic authority fixture uses its explicit open contract; no final objects are read here.
    path = f"allocations/{plan.allocation_id}"
    auth = {"Authorization": f"Bearer {authority_token}"}
    opened = {"provider_manifest_digest": plan.final_manifest.digest}
    assert authority.post(path + "/open", headers=auth, json=opened).status_code == 200
    committed = authority.post(
        path + "/commit", headers=auth, json={**opened, "result_digest": uploaded.json()["sha256"]}
    )
    assert committed.status_code == 200, committed.text
    assert client.post("publish", json=committed.json()).json() == uploaded.json()
    assert client.post("publish", json=committed.json()).json() == uploaded.json()
    db.refresh(scope)
    db.refresh(attempt)
    assert scope.status == ScopeStatus.SUCCEEDED and attempt.status == AttemptStatus.SUCCEEDED
    assert client.get("pipeline").status_code == 409


@pytest.mark.parametrize("fault", ["lease", "fence", "cancel"])
def test_stale_worker_cannot_get_pipeline_or_write_result(control_case, fault):
    client, db, scope, attempt, _, payload, *_ = control_case
    assert client.post("start").status_code == 200
    db.refresh(attempt)
    if fault == "lease":
        attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    elif fault == "fence":
        attempt.fencing_token = uuid.uuid4().hex
    else:
        scope.status = ScopeStatus.CANCELLED
    db.flush()
    assert client.get("pipeline").status_code == 409
    assert client.put("result", content=payload).status_code == 409


def test_result_bounds_corruption_cross_attempt_and_failure_record(control_case):
    client, db, scope, attempt, _, payload, *_ = control_case
    assert client.post("start").status_code == 200
    assert (
        client.get(
            f"https://app.test/api/v1/internal/evaluations/{uuid.uuid4()}/pipeline"
        ).status_code
        == 401
    )
    assert client.put("result", content=b"x" * 65537).status_code == 413
    assert (
        client.put("result", content=payload.replace(b'"rmse":1.0', b'"rmse":2.0')).status_code
        == 409
    )
    assert client.post("publish", json={"signature": "forged"}).status_code == 409
    assert client.post("fail").json() == {"recorded": True}
    assert client.post("fail").json() == {"recorded": True}
    db.refresh(attempt)
    db.refresh(scope)
    assert attempt.status == AttemptStatus.RUNNING and scope.status == ScopeStatus.RUNNING
    events = list(
        db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == attempt.id,
                WorkflowEvent.event_key == "evaluation_worker_failed",
            )
        )
    )
    assert len(events) == 1 and events[0].payload == {"reason": "Evaluator worker failed"}
