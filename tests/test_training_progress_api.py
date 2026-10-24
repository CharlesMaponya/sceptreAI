"""Progress authority and ordering exercise owned PostgreSQL transactions."""

import copy
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import TrainingCandidate, WorkflowAttempt, WorkflowEvent
from automl_api.schemas.training_progress import TrainingProgressRequest
from automl_api.services.workflow_state import canonical_request_hash
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_training_result_api import bound_run as bound_run
from test_training_result_api import capacity_client as capacity_client
from test_training_result_api import committed_tenant as committed_tenant
from test_training_result_api import context_api as context_api
from test_training_result_api import model_api as model_api
from test_training_result_api import phase1_engine as phase1_engine
from test_training_result_api import result_api as result_api
from test_training_result_api import selected_scope as selected_scope


@pytest.fixture
def progress_api(result_api):
    c, result, final, _ = result_api
    body = dict(
        run_id=result["run_id"],
        request_id=str(uuid.uuid4()),
        client_id=str(uuid.uuid4()),
        sequence=1,
        action="partial",
        entries=copy.deepcopy(result["leaderboard"]),
        primary_metric=result["primary_metric"],
    )
    base = f"/internal/attempts/{c.scope[2]}/training/progress"

    def publish(payload=None):
        return c.client.post(base, json=body if payload is None else payload, headers=c.headers)

    def receipt(payload=None):
        item = body if payload is None else payload
        return c.client.get(
            f"{base}/{item['run_id']}/receipts/{item['request_id']}", headers=c.headers
        )

    return c, body, publish, receipt, final


def phase(body, name):
    return {
        key: value
        for key, value in {
            **body,
            "action": "phase",
            "candidate_name": name,
            "phase": "cross_validating",
        }.items()
        if key not in {"entries", "primary_metric"}
    }


def next_request(body, **changes):
    return {
        **copy.deepcopy(body),
        "request_id": str(uuid.uuid4()),
        "sequence": body["sequence"] + 1,
        **changes,
    }


def test_partial_commit_exact_receipt_and_final_result_share_terminal_metadata(progress_api):
    c, body, publish, receipt, final = progress_api
    first = publish()
    assert first.status_code == 200, first.text
    assert first.json()["changed"] is True
    assert first.json()["request_digest"] == canonical_request_hash(
        TrainingProgressRequest.model_validate(body).model_dump(mode="json")
    )
    assert first.headers["cache-control"] == "no-store"
    assert receipt().json() == publish().json() == first.json()
    response = final()
    assert response.status_code == 200, response.text
    assert publish(next_request(body)).json()["changed"] is False


def test_candidate_pod_can_publish_phase_but_cannot_publish_parent_aggregate(progress_api):
    c, body, publish, receipt, _ = progress_api
    review, pod, _ = c.identity
    pod.metadata.uid = "separate-valid-candidate-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    item = phase(body, c.name)
    response = publish(item)
    assert response.status_code == 200, response.text
    assert receipt(item).json() == response.json()
    assert publish().status_code == 401
    with Session(c.scope[0]) as db:
        run = db.get(ModelRun, c.scope[1])
        assert run.tags["candidate_phase"] == "cross_validating"
        assert db.get(TrainingCandidate, c.candidate_id).status == "running"


def test_stale_phase_is_acknowledged_without_rewriting_newer_phase(progress_api):
    c, body, publish, receipt, _ = progress_api
    newest = phase(next_request(body, sequence=3), c.name)
    assert publish(newest).json()["changed"]
    stale = next_request(newest, sequence=1, phase="preparing_data")
    response = publish(stale)
    assert response.status_code == 200 and response.json()["changed"] is False
    assert receipt(stale).json() == response.json()
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags["candidate_phase"] == "cross_validating"


@pytest.mark.parametrize("fault", ["nonce", "sequence", "client", "pod", "receipt_pod"])
def test_progress_stream_has_one_pod_client_and_request_identity(progress_api, fault):
    c, body, publish, receipt, _ = progress_api
    item = phase(body, c.name)
    assert publish(item).status_code == 200
    changed = next_request(item)
    if fault == "nonce":
        changed = {**item, "phase": "evaluating"}
    elif fault == "sequence":
        changed["sequence"] = item["sequence"]
    elif fault == "client":
        changed["client_id"] = str(uuid.uuid4())
    else:
        review, pod, _ = c.identity
        pod.metadata.uid = "another-valid-pod"
        review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    response = receipt(item) if fault == "receipt_pod" else publish(changed)
    assert response.status_code == 401


@pytest.mark.parametrize(
    "fault",
    [
        "unknown_candidate",
        "duplicate",
        "partial",
        "primary",
        "foreign_run",
        "mixed_arguments",
        "nonfinite",
        "bool_metric",
        "huge_metric",
        "negative_count",
        "bool_count",
        "negative_duration",
        "bad_resume",
        "nonterminal_artifact",
        "extra",
        "extra_entry",
        "sequence_zero",
        "sequence_bool",
        "invalid_phase",
        "long_phase",
    ],
)
def test_invalid_progress_never_mutates_the_run(progress_api, fault):
    c, body, publish, _, _ = progress_api
    with Session(c.scope[0]) as db:
        before = copy.deepcopy(db.get(ModelRun, c.scope[1]).tags)
    item = copy.deepcopy(body)
    if fault == "unknown_candidate":
        item = phase(body, "NeverSelected")
    elif fault == "duplicate":
        item["entries"][1] = dict(item["entries"][0])
    elif fault == "partial":
        item["entries"].pop()
    elif fault == "primary":
        item["primary_metric"] = "unknown_metric"
    elif fault == "foreign_run":
        item["run_id"] = str(uuid.uuid4())
    elif fault == "mixed_arguments":
        item["candidate_name"] = c.name
    elif fault in {"nonfinite", "bool_metric", "huge_metric"}:
        item["entries"][0]["metrics"] = {
            body["primary_metric"]: {
                "nonfinite": float("nan"),
                "bool_metric": True,
                "huge_metric": 10**500,
            }[fault]
        }
    elif fault in {"negative_count", "bool_count"}:
        item["entries"][0]["training_rows"] = -1 if fault == "negative_count" else True
    elif fault == "negative_duration":
        item["entries"][0]["duration_seconds"] = -1
    elif fault == "bad_resume":
        item["entries"][0]["resumed"] = "yes"
    elif fault == "nonterminal_artifact":
        item["entries"][0]["status"] = "running"
    elif fault == "extra":
        item["DATABASE_URL"] = "never-used"
    elif fault == "extra_entry":
        item["entries"][0]["DATABASE_URL"] = "never-used"
    elif fault.startswith("sequence_"):
        item["sequence"] = 0 if fault == "sequence_zero" else True
    else:
        item = phase(body, c.name)
        item["phase"] = "not_a_phase" if fault == "invalid_phase" else "x" * 513
    if fault == "nonfinite":
        import json

        response = c.client.post(
            f"/internal/attempts/{c.scope[2]}/training/progress",
            content=json.dumps(item),
            headers={**c.headers, "content-type": "application/json"},
        )
    else:
        response = publish(item)
    assert response.status_code in {401, 422}, response.text
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags == before


@pytest.mark.parametrize(
    "field",
    ["metrics", "error", "training_rows", "duration_seconds", "model_artifact_sha256", "status"],
)
def test_terminal_candidate_cannot_change_in_partial_or_final_metadata(progress_api, field):
    c, body, publish, _, final = progress_api
    assert publish().status_code == 200
    changed = next_request(body)
    entry = changed["entries"][0]
    entry[field] = {
        "metrics": {body["primary_metric"]: 0.9},
        "error": "changed",
        "training_rows": 100,
        "duration_seconds": 20,
        "model_artifact_sha256": "f" * 64,
        "status": "failed",
    }[field]
    if field == "status":
        entry.pop("model_artifact_uri")
        entry.pop("model_artifact_sha256")
    assert publish(changed).status_code == 401
    original_result = copy.deepcopy(body)
    # Final uses the same shared mutation guard; changing a terminal failure is refused too.
    if field == "error":
        from automl_api.schemas.training_result import TrainingResultRequest
        from automl_api.services.training_result import control_training_result

        result = TrainingResultRequest(
            run_id=body["run_id"],
            metrics=body["entries"][0]["metrics"],
            params={"deduplicated_rows": 0},
            leaderboard=changed["entries"],
            primary_metric=body["primary_metric"],
            mlflow_run_id=c.tracking.parent_id,
        )
        from automl_api.api.routes.attempt_control import KubernetesTrainingClient
        from automl_api.services.workflow_state import StaleFence
        from sqlalchemy.orm import sessionmaker

        with pytest.raises(StaleFence, match="Terminal candidate metadata"):
            control_training_result(
                sessionmaker(c.scope[0], expire_on_commit=False),
                c.scope[2],
                c.headers["Authorization"].split(" ")[1],
                KubernetesTrainingClient(),
                result,
            )
    assert final().status_code == 200
    assert original_result == body


def test_pending_snapshot_and_late_phase_preserve_terminal_candidate(progress_api):
    c, body, publish, _, _ = progress_api
    assert publish().status_code == 200
    pending = next_request(body)
    pending["entries"] = [
        {"model": item["model"], "status": "pending", "metrics": {}} for item in body["entries"]
    ]
    assert publish(pending).status_code == 200
    assert publish(phase(next_request(body), c.name)).json()["changed"] is False
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags["winner"] == c.name


@pytest.mark.parametrize(
    "change", ["cancel", "revoke", "lease", "source", "candidate", "routing", "policy"]
)
def test_authority_change_during_token_review_prevents_progress(progress_api, change):
    from automl_api.models.enums import AttemptStatus
    from automl_api.models.workflows import OutboxEntry
    from automl_api.services.evaluation_publication import _append

    c, _, publish, _, _ = progress_api
    review, _, auth = c.identity

    def verify(*args, **kwargs):
        assert c.scope[0].pool.checkedout() == 0
        with Session(c.scope[0]) as db, db.begin():
            attempt = db.scalar(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == c.scope[2])
                .with_for_update(nowait=True)
            )
            run = db.scalar(
                select(ModelRun).where(ModelRun.id == c.scope[1]).with_for_update(nowait=True)
            )
            if change == "cancel":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "revoke":
                _append(db, attempt, "attempt.credentials.revoked", {})
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "source":
                run.params = {**run.params, "sample_tier_rows": 123}
            elif change == "candidate":
                db.get(TrainingCandidate, c.candidate_id).params = {"alpha": 123}
            elif change == "routing":
                run.tags = {**run.tags, "leaderboard_parent_run_id": str(uuid.uuid4())}
            else:
                row = db.scalar(
                    select(OutboxEntry).where(
                        OutboxEntry.aggregate_id == run.id,
                        OutboxEntry.topic == "ray.training.submit",
                    )
                )
                row.payload = {**row.payload, "credential_protocol": "ray-training-result-v6"}
        return review

    auth.create_token_review.side_effect = verify
    assert publish().status_code == 401


def test_lost_ack_uses_durable_progress_receipt(progress_api, monkeypatch):
    from automl_api.services import training_progress

    _, _, publish, receipt, _ = progress_api
    original = training_progress.control_training_progress

    def lost(*args, **kwargs):
        result = original(*args, **kwargs)
        if not kwargs.get("receipt_only"):
            raise OSError("owned committed acknowledgement loss")
        return result

    monkeypatch.setattr(training_progress, "control_training_progress", lost)
    assert publish().status_code == 503
    assert receipt().status_code == 200


def test_sql_connection_loss_rolls_back_progress_and_receipt(progress_api):
    from automl_api.services.training_progress import PROGRESS_KEY
    from sqlalchemy import event, text

    c, _, publish, receipt, _ = progress_api
    killed = []
    with Session(c.scope[0]) as db:
        before = copy.deepcopy(db.get(ModelRun, c.scope[1]).tags)

    def before_flush(db, *_):
        if any(
            isinstance(row, WorkflowEvent)
            and row.event_key.startswith(PROGRESS_KEY)
            and ":" not in row.event_key
            for row in db.new
        ):
            db.info["kill_progress_commit"] = True

    def before_commit(db):
        db.flush()
        if db.info.get("kill_progress_commit"):
            pid = db.scalar(text("SELECT pg_backend_pid()"))
            with c.scope[0].connect().execution_options(isolation_level="AUTOCOMMIT") as other:
                assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "before_commit", before_commit)
    try:
        assert publish().status_code == 503
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "before_commit", before_commit)
    assert len(killed) == 1 and receipt().status_code == 401
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags == before


def test_competing_same_sequence_commits_only_one_request(progress_api):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    c, body, publish, _, _ = progress_api
    barrier = Barrier(2)

    def compete(item):
        barrier.wait(timeout=10)
        return publish(item)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(compete, [body, next_request(body, sequence=body["sequence"])]))
    assert sorted(item.status_code for item in outcomes) == [200, 401]


def test_old_sequence_cannot_be_reused_after_newer_progress(progress_api):
    c, body, publish, _, _ = progress_api
    first = phase(body, c.name)
    assert publish(first).status_code == 200
    assert publish(next_request(first, sequence=3)).status_code == 200
    assert publish(next_request(first, sequence=1, phase="evaluating")).status_code == 401


@pytest.mark.parametrize("label", ["automl.platform/project-id", "automl.platform/attempt-id"])
def test_pod_from_another_project_or_attempt_cannot_publish_progress(progress_api, label):
    c, _, publish, _, _ = progress_api
    c.identity[1].metadata.labels[label] = str(uuid.uuid4())
    assert publish().status_code == 401


def test_late_progress_and_receipts_survive_lease_expiry_without_new_work(progress_api):
    c, body, publish, receipt, final = progress_api
    assert publish().status_code == 200
    assert final().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        before = copy.deepcopy(db.get(ModelRun, c.scope[1]).tags)
        db.get(WorkflowAttempt, c.scope[2]).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    late = next_request(body)
    response = publish(late)
    assert response.status_code == 200 and response.json()["changed"] is False
    assert receipt().status_code == 200
    assert receipt(late).json() == response.json()
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags == before


def test_progress_transport_rejects_metadata_over_budget(progress_api):
    c, _, _, _, _ = progress_api
    from automl_api.schemas.training_progress import TRAINING_PROGRESS_MAX_BYTES

    response = c.client.post(
        f"/internal/attempts/{c.scope[2]}/training/progress",
        content=b" " * (TRAINING_PROGRESS_MAX_BYTES + 1),
        headers=c.headers,
    )
    assert response.status_code == 413 and response.headers["cache-control"] == "no-store"
