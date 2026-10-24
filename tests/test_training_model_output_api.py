"""Training model custody uses the shared broker and a sealed candidate slot."""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.api.routes import attempt_control
from automl_api.models.enums import AttemptStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    OutboxEntry,
    TrainingCandidate,
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
)
from automl_api.services import attempt_output_grants as outputs
from automl_api.services.attempt_object_grants import (
    has_unretired_attempt_credentials,
    output_completion,
)
from automl_api.services.evaluation_publication import _append
from automl_api.services.training_model_output import require_training_model_completion
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.contracts import ObjectMetadata
from sqlalchemy import delete, event, select
from sqlalchemy.orm import Session
from test_training_context_api import capacity_client as capacity_client
from test_training_context_api import committed_tenant as committed_tenant
from test_training_context_api import context_api as context_api
from test_training_context_api import phase1_engine as phase1_engine
from test_training_context_api import selected_scope as selected_scope


@pytest.fixture
def bound_run(selected_scope, capacity_client):
    from automl_api.models.iam import User
    from automl_api.schemas.training import TrainingLaunchRequest
    from automl_api.services import training

    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        launched = training.launch_training_run(
            db,
            db.get(User, user_id),
            project_id,
            TrainingLaunchRequest(
                **{**body, "candidate_models": ["Ridge", "RandomForestRegressor"]}
            ),
            capacity_client,
            idempotency_key="model-output-test",
        )
        run_id = launched.run.id
    return engine, run_id


@pytest.fixture
def model_api(context_api, monkeypatch, tmp_path, request):
    scope, _, client, identity = context_api
    engine, run_id, attempt_id, fence, project_id = scope
    with Session(engine) as db:
        candidate = db.scalars(
            select(TrainingCandidate).where(
                TrainingCandidate.model_run_id == run_id,
            )
        ).first()
        entry = db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.topic == "ray.training.submit",
                OutboxEntry.aggregate_id == run_id,
            )
        ).one()
        assert entry.payload["attempt_id"] == str(attempt_id)
        assert entry.payload["credential_protocol"] == "ray-training-model-read-v10"
        if getattr(request, "param", None) == "saved-v8":
            entry.payload = {**entry.payload, "credential_protocol": "ray-training-tracking-v8"}
            db.commit()
        candidate_id, name = candidate.id, candidate.estimator_key
    from threading import local

    activity = local()

    def checkout(*args):
        activity.connections = getattr(activity, "connections", 0) + 1

    def checkin(*args):
        activity.connections -= 1

    event.listen(engine, "checkout", checkout)
    event.listen(engine, "checkin", checkin)
    writes, objects = [], {}
    store = SimpleNamespace(
        driver_name="s3_compatible", uri_for_key=lambda key: "s3://owned/" + key
    )
    monkeypatch.setattr(attempt_control, "get_object_store", lambda: store)

    def write(actual_store, uri, source, *, byte_size, sha256, grant_digest):
        assert getattr(activity, "connections", 0) == 0 and actual_store is store
        with Session(engine) as db, db.begin():
            row = db.scalars(
                select(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
                    WorkflowAttemptCredentialGrant.object_uri == uri,
                )
            ).one()
            assert row.status == "issuing" and row.issuance_owner is not None
            assert row.grant_digest == grant_digest
        assert uri not in objects
        source.seek(0)
        payload = source.read()
        assert len(payload) == byte_size and hashlib.sha256(payload).hexdigest() == sha256
        objects[uri] = payload
        writes.append(uri)
        return ObjectMetadata(
            uri=uri, byte_size=byte_size, provider_headers={"version_id": "fixture-v1"}
        )

    monkeypatch.setattr(outputs, "write_attempt_output", write)
    token = identity[2].create_token_review.call_args.args[0].spec.token
    headers = {"Authorization": "Bearer " + token}
    from training_tracking_fixture import owned_tracking

    tracking_context = owned_tracking(scope, client, headers, monkeypatch, tmp_path)
    tracking = tracking_context.__enter__()

    def ensure_tracking(candidate_name=name):
        return tracking.ensure(f"candidate:{candidate_name}")

    payload = b"owned-serialized-model"
    nonce = uuid.uuid4()
    base = f"/internal/attempts/{attempt_id}/models/{run_id}"

    def upload(*, candidate_name=name, body=payload, request_id=nonce, digest=None, extra=None):
        if candidate_name in {"Ridge", "RandomForestRegressor"}:
            ensure_tracking(candidate_name)
        return client.put(
            f"{base}/{candidate_name}/{digest or hashlib.sha256(body).hexdigest()}",
            content=body,
            headers={**headers, "X-Request-ID": str(request_id), **(extra or {})},
        )

    def receipt(*, candidate_name=name, request_id=nonce):
        return client.get(f"{base}/{candidate_name}/receipts/{request_id}", headers=headers)

    def saved():
        with Session(engine) as db:
            return list(
                db.scalars(
                    select(WorkflowAttemptCredentialGrant).where(
                        WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
                    )
                )
            )

    yield SimpleNamespace(
        scope=scope,
        client=client,
        identity=identity,
        upload=upload,
        receipt=receipt,
        saved=saved,
        writes=writes,
        objects=objects,
        candidate_id=candidate_id,
        name=name,
        nonce=nonce,
        headers=headers,
        payload=payload,
        native=write,
        tracking=tracking,
        ensure_tracking=ensure_tracking,
    )
    tracking_context.__exit__(None, None, None)
    event.remove(engine, "checkout", checkout)
    event.remove(engine, "checkin", checkin)
    with Session(engine) as db, db.begin():
        db.execute(
            delete(WorkflowAttemptCredentialGrant).where(
                WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
            )
        )


def test_training_model_is_reserved_completed_and_receipt_only_replayable(model_api):
    c = model_api
    response = c.upload()
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["candidate_id"] == str(c.candidate_id)
    assert data["run_id"] == str(c.scope[1]) and data["attempt_id"] == str(c.scope[2])
    assert data["uri"].endswith(f"/models/{c.candidate_id}/{data['sha256']}.joblib")
    assert response.headers["Cache-Control"] == "no-store"
    assert c.upload().json() == data and c.receipt().json() == data
    assert len(c.writes) == 1
    with Session(c.scope[0]) as db:
        require_training_model_completion(
            db,
            c.scope[2],
            c.scope[3],
            c.candidate_id,
            {key: data[key] for key in ("uri", "sha256", "byte_size")},
        )
        with pytest.raises(StaleFence):
            require_training_model_completion(
                db,
                c.scope[2],
                c.scope[3],
                c.candidate_id,
                {"uri": data["uri"], "sha256": "0" * 64, "byte_size": 1},
            )


@pytest.mark.parametrize("change", ["nonce", "digest", "size", "candidate", "missing"])
def test_training_model_slot_cannot_be_reissued_or_substituted(model_api, change):
    c = model_api
    assert c.upload().status_code == 200
    options = {
        "nonce": {"request_id": uuid.uuid4()},
        "digest": {"digest": "a" * 64},
        "size": {"body": b"x"},
        "candidate": {"candidate_name": "missing"},
        "missing": {"candidate_name": "missing", "request_id": uuid.uuid4()},
    }
    assert c.upload(**options[change]).status_code == 401
    assert c.receipt(candidate_name="missing").status_code == 401
    assert len(c.writes) == len(c.saved()) == 1


@pytest.mark.parametrize("fault", ["length", "chunked", "hash", "too_large"])
def test_training_model_invalid_transport_never_writes(model_api, fault):
    options = {
        "length": {"extra": {"Content-Length": "-1"}},
        "chunked": {"extra": {"Transfer-Encoding": "chunked"}},
        "hash": {"digest": "f" * 64},
        "too_large": {"extra": {"Content-Length": "5000000001"}},
    }
    assert model_api.upload(**options[fault]).status_code in {401, 422, 503}
    assert not model_api.writes


@pytest.mark.parametrize(
    "change", ["cancel", "revoke", "lease", "params", "candidate", "policy", "pod"]
)
def test_training_model_fresh_authority_is_required_after_identity_io(model_api, change):
    c = model_api
    c.ensure_tracking()
    review, pod, auth = c.identity

    def verify(*args, **kwargs):
        assert c.scope[0].pool.checkedout() == 0
        with Session(c.scope[0]) as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.scope[2])
            if change == "cancel":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "revoke":
                _append(db, attempt, "attempt.credentials.revoked", {})
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "params":
                run = db.get(ModelRun, c.scope[1])
                run.params = {**run.params, "sample_tier_rows": 123}
            elif change == "candidate":
                db.get(TrainingCandidate, c.candidate_id).params = {"alpha": 123}
            elif change == "policy":
                entry = db.scalars(
                    select(OutboxEntry).where(
                        OutboxEntry.topic == "ray.training.submit",
                        OutboxEntry.aggregate_id == c.scope[1],
                    )
                ).one()
                entry.payload = {**entry.payload, "credential_protocol": "ray-training-context-v4"}
            else:
                pod.metadata.labels["automl.platform/attempt-id"] = str(uuid.uuid4())
        return review

    auth.create_token_review.side_effect = verify
    assert c.upload().status_code == 401
    assert not c.writes


def test_training_model_unknown_native_ack_never_reissues_even_after_expiry(model_api, monkeypatch):
    c = model_api

    def lost(*args, **kwargs):
        c.native(*args, **kwargs)
        raise OSError("accepted response lost")

    monkeypatch.setattr(outputs, "write_attempt_output", lost)
    assert c.upload().status_code == 503
    assert c.upload().status_code == c.receipt().status_code == 503
    assert c.upload(request_id=uuid.uuid4()).status_code == 401
    row = c.saved()[0]
    assert row.status == "unknown" and len(c.writes) == 1
    with Session(c.scope[0]) as db:
        assert output_completion(db, db.get(WorkflowAttemptCredentialGrant, row.id)) is None
        assert has_unretired_attempt_credentials(db, c.scope[2])


def test_training_model_completion_survives_cancellation_without_disclosure(model_api, monkeypatch):
    c = model_api

    def cancelled(*args, **kwargs):
        result = c.native(*args, **kwargs)
        with Session(c.scope[0]) as db, db.begin():
            db.get(WorkflowAttempt, c.scope[2]).status = AttemptStatus.CANCELLED
        return result

    monkeypatch.setattr(outputs, "write_attempt_output", cancelled)
    assert c.upload().status_code == 503
    assert c.receipt().status_code == 401
    row = c.saved()[0]
    with Session(c.scope[0]) as db:
        assert output_completion(db, db.get(WorkflowAttemptCredentialGrant, row.id)) is not None
    assert len(c.writes) == 1


def test_training_model_allows_independent_candidate_slots_and_mutable_progress(model_api):
    c = model_api
    assert c.upload().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        run = db.get(ModelRun, c.scope[1])
        run.tags = {**run.tags, "leaderboard": [], "worker_progress": "changed"}
    assert (
        c.upload(candidate_name="RandomForestRegressor", request_id=uuid.uuid4()).status_code == 200
    )
    assert c.receipt().status_code == 200
    assert len(c.writes) == len(c.saved()) == 2


def test_duplicate_during_native_write_observes_original_owner(model_api, monkeypatch):
    c = model_api
    duplicates = []

    def write(*args, **kwargs):
        duplicates.append(c.upload().status_code)
        return c.native(*args, **kwargs)

    monkeypatch.setattr(outputs, "write_attempt_output", write)
    assert c.upload().status_code == 200
    assert duplicates == [503] and len(c.writes) == 1


def test_two_candidates_write_concurrently_without_holding_sql(model_api, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    c = model_api
    barrier = Barrier(2)

    def write(*args, **kwargs):
        barrier.wait(timeout=15)
        return c.native(*args, **kwargs)

    monkeypatch.setattr(outputs, "write_attempt_output", write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(c.upload)
        second = pool.submit(
            c.upload, candidate_name="RandomForestRegressor", request_id=uuid.uuid4()
        )
        assert first.result(timeout=30).status_code == second.result(timeout=30).status_code == 200
    assert len(c.writes) == len(c.saved()) == 2


def test_sql_loss_before_issuance_commit_never_starts_storage(model_api):
    from sqlalchemy import event, text

    c = model_api
    failed = []

    def before_flush(db, *_):
        if any(
            isinstance(row, WorkflowAttemptCredentialGrant) and row.status == "issuing"
            for row in db.dirty
        ):
            db.info["kill_model_issuance"] = True

    def before_commit(db):
        db.flush()
        if db.info.get("kill_model_issuance"):
            pid = db.scalar(text("SELECT pg_backend_pid()"))
            with c.scope[0].connect().execution_options(isolation_level="AUTOCOMMIT") as other:
                assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            failed.append(pid)

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "before_commit", before_commit)
    try:
        assert c.upload().status_code == 503
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "before_commit", before_commit)
    assert len(failed) == 1 and not c.writes
    row = c.saved()[0]
    assert row.status == "pending" and row.issuance_owner is None
    assert c.upload().status_code == 200 and len(c.writes) == 1


def test_rotated_identity_can_recover_only_its_original_pod_receipt(model_api):
    c = model_api
    assert c.upload().status_code == 200
    review, pod, _ = c.identity
    pod.metadata.uid = "different-otherwise-valid-writer"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    assert c.receipt().status_code == 401
    assert len(c.writes) == 1
