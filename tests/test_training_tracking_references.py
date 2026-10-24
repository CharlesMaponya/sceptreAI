"""Original tracking custody is required for a current model and tournament."""

import uuid

import pytest
from automl_api.models.workflows import WorkflowEvent
from automl_api.services.training_tracking import PUBLISHED
from sqlalchemy import delete
from sqlalchemy.orm import Session
from test_training_result_api import bound_run as bound_run
from test_training_result_api import capacity_client as capacity_client
from test_training_result_api import committed_tenant as committed_tenant
from test_training_result_api import context_api as context_api
from test_training_result_api import model_api as model_api
from test_training_result_api import phase1_engine as phase1_engine
from test_training_result_api import result_api as result_api
from test_training_result_api import selected_scope as selected_scope


@pytest.mark.parametrize("role", ["parent", "candidate"])
def test_unqualified_tracking_receipt_cannot_publish_final_result(result_api, role):
    c, _, publish, _ = result_api
    name = "parent" if role == "parent" else f"candidate:{c.name}"
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2], WorkflowEvent.event_key == PUBLISHED + name
            )
        )
    assert publish().status_code == 401


def test_current_model_write_requires_original_candidate_tracking(model_api):
    import hashlib

    c = model_api
    response = c.client.put(
        f"/internal/attempts/{c.scope[2]}/models/{c.scope[1]}/{c.name}/{hashlib.sha256(c.payload).hexdigest()}",
        content=c.payload,
        headers={**c.headers, "X-Request-ID": str(c.nonce)},
    )
    assert response.status_code == 401
    assert c.writes == []


@pytest.mark.parametrize("value", [None, "foreign", "parent", "other_candidate"])
def test_candidate_tracking_id_must_match_the_exact_model_reservation(result_api, value):
    import copy

    c, body, publish, _ = result_api
    changed = copy.deepcopy(body)
    if value == "parent":
        value = c.tracking.parent_id
    elif value == "other_candidate":
        other = next(entry["model"] for entry in body["leaderboard"] if entry["model"] != c.name)
        value = c.ensure_tracking(other)
    changed["leaderboard"][0]["mlflow_run_id"] = value
    assert publish(changed).status_code == 401


def test_final_parent_tracking_cannot_name_another_native_run(result_api):
    import copy

    c, body, publish, _ = result_api
    changed = copy.deepcopy(body)
    changed["mlflow_run_id"] = c.ensure_tracking()
    assert publish(changed).status_code == 401


@pytest.mark.parametrize(
    "field",
    [
        "attempt_id",
        "command_id",
        "role",
        "tracking_run_id",
        "owner_digest",
        "receipt_digest",
        "missing",
    ],
)
def test_model_slot_tracking_snapshot_cannot_be_changed_or_erased(result_api, field):
    import uuid

    from automl_api.services.workflow_state import canonical_request_hash
    from sqlalchemy import select

    c, _, publish, _ = result_api
    with Session(c.scope[0]) as db, db.begin():
        slot = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == f"training.model.grant.{c.candidate_id}",
            )
        )
        payload = dict(slot.payload)
        if field == "missing":
            payload.pop("tracking_receipt")
        else:
            reference = dict(payload["tracking_receipt"])
            reference[field] = str(uuid.uuid4()) if field.endswith("_id") else "f" * 64
            payload["tracking_receipt"] = reference
        slot.payload = payload
        slot.result_digest = canonical_request_hash(payload)
    assert c.receipt().status_code == 401
    assert publish().status_code == 401
    assert len(c.writes) == 1


def test_model_writer_must_be_the_original_candidate_tracking_pod(model_api):
    c = model_api
    c.ensure_tracking()
    review, pod, _ = c.identity
    pod.metadata.uid = "another-valid-model-producer"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    assert c.upload().status_code == 401
    assert c.writes == c.saved() == []


@pytest.mark.parametrize("status", ["pending", "running", "failed", "cancelled", "skipped"])
def test_non_successful_entries_cannot_publish_tracking_ids(result_api, status):
    import copy

    c, body, _, _ = result_api
    entries = copy.deepcopy(body["leaderboard"])
    target = entries[1]
    target.update(status=status, mlflow_run_id=c.tracking.parent_id)
    response = c.client.post(
        f"/internal/attempts/{c.scope[2]}/training/progress",
        json={
            "run_id": str(c.scope[1]),
            "request_id": str(uuid.uuid4()),
            "client_id": str(uuid.uuid4()),
            "sequence": 1,
            "action": "partial",
            "entries": entries,
            "primary_metric": body["primary_metric"],
        },
        headers=c.headers,
    )
    assert response.status_code == 401


def test_stored_result_replay_revalidates_original_tracking_receipts(result_api):
    c, _, publish, receipt = result_api
    assert publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == PUBLISHED + f"candidate:{c.name}",
            )
        )
    assert publish().status_code == receipt().status_code == 401


def test_pending_snapshot_cannot_hide_corrupted_terminal_tracking(result_api, monkeypatch):
    from automl_api.training import pipeline

    c, body, _, _ = result_api
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(c.scope[2]))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", c.scope[3])
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(c.scope[0]))
    pipeline._persist_partial_leaderboard(c.scope[1], body["leaderboard"], body["primary_metric"])
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == PUBLISHED + f"candidate:{c.name}",
            )
        )
    from automl_api.services.workflow_state import StaleFence

    with pytest.raises(StaleFence):
        pipeline._persist_partial_leaderboard(
            c.scope[1],
            [
                {"model": item["model"], "status": "pending", "metrics": {}}
                for item in body["leaderboard"]
            ],
            body["primary_metric"],
        )


@pytest.mark.parametrize("kind", ["partial", "final"])
def test_sql_pending_snapshot_cannot_hide_unapproved_tracking_id(result_api, monkeypatch, kind):
    import copy

    from automl_api.services.workflow_state import StaleFence
    from automl_api.training import pipeline

    c, body, _, _ = result_api
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(c.scope[2]))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", c.scope[3])
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(c.scope[0]))
    pipeline._persist_partial_leaderboard(c.scope[1], body["leaderboard"], body["primary_metric"])
    entries = copy.deepcopy(body["leaderboard"])
    entries[0].update(status="pending", mlflow_run_id="foreign-tracking")
    with pytest.raises(StaleFence, match="Only successful"):
        if kind == "partial":
            pipeline._persist_partial_leaderboard(c.scope[1], entries, body["primary_metric"])
        else:
            result = pipeline.TournamentResult(
                metrics=body["metrics"],
                model=None,
                params={},
                leaderboard=entries,
                primary_metric=body["primary_metric"],
            )
            pipeline._persist_training_success(c.scope[1], result, c.tracking.parent_id)
