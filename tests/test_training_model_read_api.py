"""Real SQL custody and bound parent authority around a conditional read signer."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowEvent,
)
from automl_api.services import attempt_object_grants as reads
from automl_api.services.attempt_object_grants import has_unretired_attempt_credentials
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.training import pipeline
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from test_training_model_output_api import bound_run as bound_run
from test_training_model_output_api import capacity_client as capacity_client
from test_training_model_output_api import committed_tenant as committed_tenant
from test_training_model_output_api import context_api as context_api
from test_training_model_output_api import model_api as model_api
from test_training_model_output_api import phase1_engine as phase1_engine
from test_training_model_output_api import selected_scope as selected_scope
from test_training_model_publication import authorized_entry
from test_training_model_publication import publication as publication


@pytest.fixture
def model_read(publication, monkeypatch):
    c, _ = publication
    entry = authorized_entry(c)
    pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")
    body = {"run_id": str(c.scope[1]), "candidate_name": c.name, "request_id": str(uuid.uuid4())}
    calls = []
    state = {}

    def mint(store, item, deadline, *, object_version):
        assert c.scope[0].pool.checkedout() == 0
        assert object_version == "fixture-v1"
        assert item.uri == entry["model_artifact_uri"]
        with Session(c.scope[0]) as db:
            row = db.scalars(
                select(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id
                    == state.get("attempt_id", c.scope[2]),
                    WorkflowAttemptCredentialGrant.request_id
                    == uuid.UUID(state.get("request_id", body["request_id"])),
                )
            ).one()
            assert row.status == "issuing" and row.issuance_owner is not None
        calls.append((item, deadline, object_version))
        if "during_mint" in state:
            state["during_mint"]()
        return {
            "url": "https://objects.invalid/owned-private-read",
            "expires_at": deadline.isoformat(),
        }

    monkeypatch.setattr(reads, "mint_refit_read_url", mint)

    def call(payload=None, attempt_id=None):
        value = body if payload is None else payload
        state["request_id"] = value.get("request_id", body["request_id"])
        state["attempt_id"] = attempt_id or c.scope[2]
        return c.client.post(
            f"/internal/attempts/{attempt_id or c.scope[2]}/training/models/read",
            json=value,
            headers=c.headers,
        )

    try:
        yield c, entry, body, calls, state, call
    finally:
        with Session(c.scope[0]) as db, db.begin():
            db.execute(
                delete(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.project_id == c.scope[4],
                    WorkflowAttemptCredentialGrant.method == "GET",
                )
            )


def test_parent_reads_published_exact_model_once_and_replay_has_no_url(model_read):
    c, entry, body, calls, _, call = model_read
    first = call()
    assert first.status_code == 200, first.text
    value = first.json()
    assert first.headers["Cache-Control"] == "no-store"
    assert value["uri"] == entry["model_artifact_uri"]
    assert value["object_version"] == "fixture-v1" and value["byte_limit"] == len(c.payload)
    assert value["request_digest"] == canonical_request_hash(body)
    second = call()
    assert second.status_code == 200, second.text
    assert second.json() == {**value, "url": None} and len(calls) == 1
    with Session(c.scope[0]) as db:
        row = db.get(WorkflowAttemptCredentialGrant, uuid.UUID(value["grant_id"]))
        assert (
            row.role == "checkpoint" and row.method == "GET" and row.object_version == "fixture-v1"
        )
        assert datetime.now(UTC) < row.expires_at <= datetime.now(UTC) + timedelta(seconds=60)
        assert "https://" not in str(row.__dict__)
        assert has_unretired_attempt_credentials(db, c.scope[2])


@pytest.mark.parametrize(
    "fault",
    [
        "pod",
        "source",
        "unpublished",
        "failed",
        "completion",
        "intent",
        "tracking",
        "metadata",
        "policy",
        "revoked",
    ],
)
def test_changed_authority_cannot_mint_model_read(model_read, fault):
    from automl_api.models.workflows import OutboxEntry
    from automl_api.services.attempt_object_grants import revoke_attempt_grants

    c, entry, _, calls, _, call = model_read
    if fault == "pod":
        review, pod, _ = c.identity
        review.status.user.extra["authentication.kubernetes.io/pod-uid"] = ["another-pod"]
        pod.metadata.uid = "another-pod"
    else:
        with Session(c.scope[0]) as db, db.begin():
            run = db.get(ModelRun, c.scope[1])
            if fault == "source":
                run.params = {**run.params, "sample_tier_rows": 123}
            elif fault == "unpublished":
                run.tags = {**run.tags, "leaderboard": []}
            elif fault == "failed":
                run.tags = {**run.tags, "leaderboard": [{**entry, "status": "failed"}]}
            elif fault == "revoked":
                revoke_attempt_grants(db, c.scope[2])
            elif fault == "policy":
                outbox = db.scalars(
                    select(OutboxEntry).where(
                        OutboxEntry.aggregate_id == run.id,
                        OutboxEntry.topic == "ray.training.submit",
                    )
                ).one()
                outbox.payload = {
                    **outbox.payload,
                    "credential_protocol": "ray-training-metadata-v9",
                }
            else:
                key = {
                    "completion": "attempt.output.completed.",
                    "intent": "training.model.grant.",
                    "tracking": "training.tracking.published.",
                    "metadata": "training.metadata.ready.",
                }[fault]
                events = list(
                    db.scalars(
                        select(WorkflowEvent).where(
                            WorkflowEvent.attempt_id == c.scope[2],
                            WorkflowEvent.event_key.startswith(key),
                        )
                    )
                )
                assert events, key
                for event in events:
                    event.result_digest = "b" * 64
    response = call()
    assert response.status_code == 401, response.text
    assert response.headers["Cache-Control"] == "no-store" and calls == []


@pytest.mark.parametrize("fault", ["cancel", "source", "token", "mint"])
def test_authority_change_during_mint_never_discloses_url_or_remints(model_read, fault):
    from automl_api.models.enums import AttemptStatus

    c, _, _, calls, state, call = model_read

    def change():
        if fault == "mint":
            raise RuntimeError("owned signer uncertainty")
        if fault == "token":
            c.identity[0].status.authenticated = False
        else:
            with Session(c.scope[0]) as db, db.begin():
                if fault == "cancel":
                    db.get(WorkflowAttempt, c.scope[2]).status = AttemptStatus.CANCELLED
                else:
                    run = db.get(ModelRun, c.scope[1])
                    run.params = {**run.params, "sample_tier_rows": 123}

    state["during_mint"] = change
    response = call()
    assert response.status_code == 503 and "objects.invalid" not in response.text
    assert len(calls) == 1
    assert call().status_code in {401, 503} and len(calls) == 1
    with Session(c.scope[0]) as db:
        row = db.scalars(
            select(WorkflowAttemptCredentialGrant).where(
                WorkflowAttemptCredentialGrant.attempt_id == c.scope[2],
                WorkflowAttemptCredentialGrant.method == "GET",
            )
        ).one()
        assert row.status == "unknown" and has_unretired_attempt_credentials(db, c.scope[2])


@pytest.mark.parametrize(
    "extra",
    [
        {"uri": "s3://foreign/model"},
        {"object_version": "latest"},
        {"role": "train"},
        {"object_index": 1},
    ],
)
def test_model_read_rejects_client_selected_storage_fields(model_read, extra):
    _, _, body, calls, _, call = model_read
    assert call({**body, **extra}).status_code == 422 and calls == []


@pytest.mark.parametrize("fault", [None, "allowlist", "lineage", "source"])
def test_predecessor_model_read_requires_explicit_original_custody(model_read, fault):
    from automl_api.models.enums import AttemptStatus
    from automl_api.services import reconciler
    from automl_api.services.attempt_identity import attempt_audience

    c, _, _, calls, _, call = model_read
    with Session(c.scope[0]) as db, db.begin():
        old = db.get(WorkflowAttempt, c.scope[2])
        new = reconciler._replace_lost_ray_attempt(db, old, reason="owned model read fixture")
        assert new is not None and old.status == AttemptStatus.SUPERSEDED
        new.status, new.ray_job_name = AttemptStatus.SUBMITTED, "owned-replacement"
        new_id, fence, generation = new.id, new.fencing_token, new.generation
    review, pod, _ = c.identity
    pod.metadata.labels["automl.platform/attempt-id"] = str(new_id)
    pod.metadata.labels["automl.platform/generation"] = str(generation)
    review.status.audiences = [attempt_audience(new_id, fence)]
    response = c.client.post(
        f"/internal/attempts/{new_id}/parent/start",
        json={"run_id": str(c.scope[1]), "worker_id": str(uuid.uuid4()), "run_kind": "training"},
        headers=c.headers,
    )
    assert response.status_code == 200, response.text
    with Session(c.scope[0]) as db, db.begin():
        new = db.get(WorkflowAttempt, new_id)
        if fault == "allowlist":
            new.predecessor_checkpoint_allowlist = []
        elif fault == "lineage":
            new.predecessor_attempt_id = None
        elif fault == "source":
            run = db.get(ModelRun, c.scope[1])
            run.params = {**run.params, "sample_tier_rows": 123}
    response = call(attempt_id=new_id)
    assert response.status_code == (200 if fault is None else 401), response.text
    assert len(calls) == (1 if fault is None else 0)
    if fault is None:
        assert response.json()["object_version"] == "fixture-v1"
        assert response.json()["attempt_id"] == str(new_id)
        with Session(c.scope[0]) as db:
            assert has_unretired_attempt_credentials(db, new_id)


def test_deleted_read_receipt_fails_closed_without_url_disclosure(model_read, monkeypatch):
    from automl_api.services import training_model_read

    c, _, _, calls, _, call = model_read
    original = training_model_read.issue_input_grant

    def disappear(*args, **kwargs):
        reply = original(*args, **kwargs)
        with Session(c.scope[0]) as db, db.begin():
            db.execute(
                delete(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.id == uuid.UUID(reply["grant_id"]),
                    WorkflowAttemptCredentialGrant.project_id == c.scope[4],
                )
            )
        return reply

    monkeypatch.setattr(training_model_read, "issue_input_grant", disappear)
    response = call()
    assert response.status_code == 401 and "objects.invalid" not in response.text
    assert len(calls) == 1


def test_read_declaration_is_bounded_before_identity_or_storage(model_read):
    c, _, _, calls, _, _ = model_read
    response = c.client.post(
        f"/internal/attempts/{c.scope[2]}/training/models/read",
        content=b"x" * 4097,
        headers=c.headers,
    )
    assert response.status_code == 413 and calls == []


def test_read_nonce_cannot_be_rebound_to_another_published_candidate(model_read):
    c, entry, body, calls, _, call = model_read
    assert call().status_code == 200
    other = "RandomForestRegressor" if c.name == "Ridge" else "Ridge"
    response = c.upload(candidate_name=other, body=b"other-model", request_id=uuid.uuid4())
    assert response.status_code == 200, response.text
    data = response.json()
    second = {
        "model": other,
        "status": "succeeded",
        "metrics": {"rmse": 0.3},
        "model_artifact_uri": data["uri"],
        "model_artifact_sha256": data["sha256"],
        "mlflow_run_id": c.ensure_tracking(other),
    }
    pipeline._persist_partial_leaderboard(c.scope[1], [entry, second], "rmse")
    response = call({**body, "candidate_name": other})
    assert response.status_code == 401, response.text
    assert len(calls) == 1
