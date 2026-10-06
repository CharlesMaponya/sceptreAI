from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from threading import Barrier
from types import SimpleNamespace

import httpx
import pytest
from automl_api.models.enums import ScopeStatus
from automl_api.models.workflows import WorkflowEvent
from automl_api.qualification_control import create_app
from automl_api.services.evaluation_authority import prepare_evaluator
from automl_api.services.evaluation_reconciler import (
    _abort_registration,
    reconcile_evaluation_jobs_once,
    reconcile_scope,
)
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.workflow_state import InvalidTransition
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_evaluation_jobs import EvaluatorKubernetes
from test_champion_identity import publish, register
from test_qualification_control import allocate, headers

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


@pytest.mark.parametrize("registered", [False, True])
def test_abort_is_project_scoped_replayable_and_cannot_replace_registered_recovery(
    authority_api, registered
):
    client, scope, options = authority_api
    path = f"/allocations/{allocate(client, scope)}"
    publish(client, path)
    if registered:
        assert register(client, path)[1].status_code == 200
    body = dict(scope_id=str(scope), provider_manifest_digest="a" * 64, reason="Handoff stopped")
    assert (
        client.post(path + "/recovery/abort", json=body, headers=headers("aws")).status_code == 403
    )
    other = options["principals"][0].model_copy(update={"project_reference": "other-project"})
    with TestClient(create_app(**{**options, "principals": [other]})) as wrong:
        assert (
            wrong.post(
                path + "/recovery/abort", json=body, headers=headers("allocator")
            ).status_code
            == 404
        )
    result = client.post(path + "/recovery/abort", json=body, headers=headers("allocator"))
    if registered:
        assert result.status_code == 409
        return
    assert result.status_code == 200, result.text
    assert verify_receipt(SimpleNamespace(**result.json()), options["signing_secret"].public_key())
    assert (
        client.post(path + "/recovery/abort", json=body, headers=headers("allocator")).json()
        == result.json()
    )
    assert (
        client.post(
            path + "/recovery/abort",
            json={**body, "reason": "different"},
            headers=headers("allocator"),
        ).status_code
        == 409
    )
    assert register(client, path)[1].status_code == 409
    assert client.post(
        path + "/open", json={"provider_manifest_digest": "a" * 64}, headers=headers("aws")
    ).status_code in {403, 409}


@pytest.mark.parametrize("boundary", ["allocations", "/refit", "/evaluators"])
@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("operator_aborted", [False, True])
def test_expired_incomplete_handoff_is_sealed_with_or_without_local_plan(
    handoff_case, boundary, monkeypatch, cancelled, operator_aborted
):
    db, scope, factory, authority, public_key, result_key = handoff_case
    original = authority.post

    def crash(path, **kwargs):
        if path.endswith(boundary):
            if boundary == "allocations":
                assert original(path, **kwargs).status_code == 200
            raise httpx.ReadError("Controller crashed during registration")
        return original(path, **kwargs)

    monkeypatch.setattr(authority, "post", crash)
    with pytest.raises(httpx.ReadError):
        prepare_evaluator(
            factory,
            scope.id,
            authority,
            authority_public_key=public_key,
            result_public_key=result_key,
        )
    monkeypatch.setattr(authority, "post", original)
    prior_receipt = None
    if operator_aborted:
        intent = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.event_key == "evaluation_authority_intent",
                WorkflowEvent.project_id == scope.project_id,
            )
        ).payload["allocation"]
        allocation = authority.post("allocations", json=intent).json()
        response = authority.post(
            f"allocations/{allocation['allocation_id']}/recovery/abort",
            json={
                "scope_id": str(scope.id),
                "provider_manifest_digest": intent["provider_manifest_digest"],
                "reason": "Operator stopped the abandoned registration",
            },
        )
        assert response.status_code == 200
        prior_receipt = response.json()
    db.refresh(scope)
    if cancelled:
        scope.status = ScopeStatus.CANCELLED
    else:
        scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    k8s = EvaluatorKubernetes()
    assert (
        reconcile_evaluation_jobs_once(
            factory,
            k8s,
            authority_factory=lambda _: nullcontext(authority),
            public_key=public_key,
            store=object(),
        )
        == 1
    )
    db.refresh(scope)
    assert scope.status == (ScopeStatus.CANCELLED if cancelled else ScopeStatus.FAILED)
    # Local terminal replay must not register a worker or reopen the allocation.
    for _ in range(2):
        reconcile_scope(factory, k8s, scope.id, authority, public_key=public_key, store=None)
    intent = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.event_key == "evaluation_authority_intent",
            WorkflowEvent.project_id == scope.project_id,
        )
    ).payload["allocation"]
    allocation = authority.post("allocations", json=intent)
    assert allocation.status_code == 200
    assert allocation.json()["status"] == "failed"
    state = authority.get(f"allocations/{allocation.json()['allocation_id']}").json()
    assert [r["operation"] for r in state["receipts"]].count("abort") == 1
    assert not any(r["operation"].startswith("evaluator_") for r in state["receipts"])
    assert not any(k8s.objects.values())
    if prior_receipt is not None:
        assert next(r for r in state["receipts"] if r["operation"] == "abort") == prior_receipt
        closed = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.event_key == (
                    "evaluation_handoff_closed"
                    if boundary == "allocations"
                    else "evaluation_failed"
                ),
                WorkflowEvent.project_id == scope.project_id,
            )
        )
        assert closed.payload == prior_receipt


def test_registration_and_abort_serialize_to_one_winner(authority_api):
    client, scope, _ = authority_api
    path = f"/allocations/{allocate(client, scope)}"
    publish(client, path)
    barrier = Barrier(2)

    def abort():
        barrier.wait(timeout=10)
        return client.post(
            path + "/recovery/abort",
            headers=headers("allocator"),
            json={
                "scope_id": str(scope),
                "provider_manifest_digest": "a" * 64,
                "reason": "Stopped",
            },
        )

    def registration():
        barrier.wait(timeout=10)
        return register(client, path)[1]

    with ThreadPoolExecutor(max_workers=2) as pool:
        aborted, registered = pool.submit(abort), pool.submit(registration)
        results = [aborted.result(), registered.result()]
    assert sorted(r.status_code for r in results) == [200, 409]
    state = client.get(path, headers=headers("allocator")).json()
    operations = {r["operation"] for r in state["receipts"]}
    assert ("abort" in operations) != ("evaluator_1" in operations)
    assert state["status"] == ("failed" if "abort" in operations else "allocated")


@pytest.mark.parametrize(
    "fault", ["signature", "scope", "manifest", "digest", "cas", "missing", "duplicate"]
)
def test_prior_abort_recovery_rejects_substituted_evidence(authority_api, fault):
    client, scope, options = authority_api
    identity = allocate(client, scope)
    path = f"/allocations/{identity}"
    response = client.post(
        path + "/recovery/abort",
        headers=headers("allocator"),
        json={
            "scope_id": str(scope),
            "provider_manifest_digest": "a" * 64,
            "reason": "Operator stop",
        },
    )
    assert response.status_code == 200
    state = client.get(path, headers=headers("allocator")).json()
    receipt = next(r for r in state["receipts"] if r["operation"] == "abort")
    if fault == "missing":
        state["receipts"] = []
    elif fault == "duplicate":
        state["receipts"].append(receipt)
    elif fault == "cas":
        state["cas_version"] = 2
    elif fault == "scope":
        receipt["payload"]["scope_id"] = "00000000-0000-0000-0000-000000000000"
    elif fault == "manifest":
        receipt["payload"]["provider_manifest_digest"] = "b" * 64
    elif fault == "digest":
        receipt["request_digest"] = "b" * 64
    else:
        receipt["signature"] = "invalid"
    authority = SimpleNamespace(
        post=lambda endpoint, **kw: client.post("/" + endpoint, headers=headers("allocator"), **kw),
        get=lambda endpoint: httpx.Response(
            200, json=state, request=httpx.Request("GET", "https://authority.test")
        ),
    )
    with pytest.raises(InvalidTransition):
        _abort_registration(
            authority, identity, scope, "a" * 64, options["signing_secret"].public_key()
        )
