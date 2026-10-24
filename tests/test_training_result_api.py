"""Complete finite result publication uses owned SQL and fresh Pod identity."""

import copy
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.runs import Metric, ModelRun
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.workflow_state import canonical_request_hash
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_training_model_output_api import bound_run as bound_run
from test_training_model_output_api import capacity_client as capacity_client
from test_training_model_output_api import committed_tenant as committed_tenant
from test_training_model_output_api import context_api as context_api
from test_training_model_output_api import model_api as model_api
from test_training_model_output_api import phase1_engine as phase1_engine
from test_training_model_output_api import selected_scope as selected_scope


@pytest.fixture
def result_api(model_api, monkeypatch):
    c = model_api
    response = c.upload()
    assert response.status_code == 200, response.text
    artifact = response.json()
    with Session(c.scope[0]) as db:
        run = db.get(ModelRun, c.scope[1])
        primary = run.params["primary_metric"]
        other = next(name for name in run.params["candidate_models"] if name != c.name)
    payload = {
        "run_id": str(c.scope[1]),
        "metrics": {primary: 0.2},
        "params": {"deduplicated_rows": 0},
        "primary_metric": primary,
        "mlflow_run_id": c.tracking.parent_id,
        "leaderboard": [
            {
                "model": c.name,
                "status": "succeeded",
                "metrics": {primary: 0.2},
                "model_artifact_uri": artifact["uri"],
                "model_artifact_sha256": artifact["sha256"],
                "mlflow_run_id": c.ensure_tracking(),
            },
            {"model": other, "status": "failed", "metrics": {}, "error": "owned candidate failure"},
        ],
    }
    for key in ("AUTOML_ATTEMPT_ID", "AUTOML_FENCING_TOKEN", "AUTOML_PROJECT_ID"):
        monkeypatch.delenv(key, raising=False)
    from automl_api.training import pipeline

    def forbidden_worker_sql():
        raise AssertionError("result RPC opened worker SQL")

    monkeypatch.setattr(pipeline, "get_session_factory", forbidden_worker_sql)
    base = f"/internal/attempts/{c.scope[2]}/training/result"

    def publish(body=None):
        return c.client.post(base, json=payload if body is None else body, headers=c.headers)

    def receipt(body=None):
        digest = canonical_request_hash(payload if body is None else body)
        return c.client.get(f"{base}/{c.scope[1]}/receipts/{digest}", headers=c.headers)

    return c, payload, publish, receipt


def test_complete_result_and_receipt_replay_commit_one_metric_without_api_environment(result_api):
    c, body, publish, receipt = result_api
    first = publish()
    assert first.status_code == 200, first.text
    assert first.headers["cache-control"] == "no-store"
    assert first.json()["request_digest"] == canonical_request_hash(body)
    assert publish().json() == receipt().json() == first.json()
    with Session(c.scope[0]) as db:
        run = db.get(ModelRun, c.scope[1])
        assert run.tags["winner"] == c.name
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run.id))
            == 1
        )
        assert db.get(WorkflowAttempt, c.scope[2]).terminal_cas_version == 0


@pytest.mark.parametrize(
    "fault",
    [
        "partial",
        "duplicate",
        "pending",
        "extra",
        "params",
        "metrics",
        "primary",
        "uri",
        "hash",
        "foreign_run",
        "nonfinite",
    ],
)
def test_invalid_final_tournament_has_no_partial_publication(result_api, fault):
    c, original, publish, _ = result_api
    body = copy.deepcopy(original)
    if fault == "partial":
        body["leaderboard"].pop()
    elif fault == "duplicate":
        body["leaderboard"][1] = dict(body["leaderboard"][0])
    elif fault == "pending":
        body["leaderboard"][1]["status"] = "pending"
    elif fault == "extra":
        body["database_url"] = "never-accepted"
    elif fault == "params":
        body["params"]["DATABASE_URL"] = "never-accepted"
    elif fault == "metrics":
        body["metrics"][body["primary_metric"]] = 0.9
    elif fault == "primary":
        body["primary_metric"] = "unapproved_metric"
    elif fault == "uri":
        body["leaderboard"][0]["model_artifact_uri"] += ".other"
    elif fault == "hash":
        body["leaderboard"][0]["model_artifact_sha256"] = "f" * 64
    elif fault == "foreign_run":
        body["run_id"] = str(uuid.uuid4())
    else:
        import json

        body["metrics"][body["primary_metric"]] = float("nan")
        response = c.client.post(
            f"/internal/attempts/{c.scope[2]}/training/result",
            content=json.dumps(body),
            headers={**c.headers, "content-type": "application/json"},
        )
        assert response.status_code == 422
        return
    assert publish(body).status_code in {401, 422}
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags.get("training_result_receipt") is None
        assert (
            db.scalar(
                select(func.count()).select_from(Metric).where(Metric.model_run_id == c.scope[1])
            )
            == 0
        )


def test_committed_receipt_survives_parent_lease_expiry_without_new_work(result_api):
    c, _, publish, receipt = result_api
    assert publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        db.get(WorkflowAttempt, c.scope[2]).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    assert receipt().status_code == 200
    assert publish().status_code == 200


def test_another_valid_pod_cannot_publish_parent_result(result_api):
    c, _, publish, _ = result_api
    review, pod, _ = c.identity
    pod.metadata.uid = "different-valid-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    assert publish().status_code == 401


def test_native_identity_io_has_no_sql_connection_or_locks(result_api):
    c, _, publish, _ = result_api
    review, _, auth = c.identity

    def verify(*args, **kwargs):
        assert c.scope[0].pool.checkedout() == 0
        with Session(c.scope[0]) as db, db.begin():
            db.execute(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == c.scope[2])
                .with_for_update(nowait=True)
            )
            db.execute(
                select(ModelRun).where(ModelRun.id == c.scope[1]).with_for_update(nowait=True)
            )
        return review

    auth.create_token_review.side_effect = verify
    response = publish()
    assert response.status_code == 200, response.text


@pytest.mark.parametrize("change", ["cancel", "revoke", "lease", "source", "candidate"])
def test_authority_changed_during_token_review_cannot_commit_result(result_api, change):
    from automl_api.models.enums import AttemptStatus
    from automl_api.models.workflows import TrainingCandidate
    from automl_api.services.evaluation_publication import _append

    c, _, publish, _ = result_api
    review, _, auth = c.identity

    def verify(*args, **kwargs):
        with Session(c.scope[0]) as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.scope[2])
            if change == "cancel":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "revoke":
                _append(db, attempt, "attempt.credentials.revoked", {})
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "source":
                run = db.get(ModelRun, c.scope[1])
                run.params = {**run.params, "sample_tier_rows": 123}
            else:
                db.get(TrainingCandidate, c.candidate_id).params = {"alpha": 123}
        return review

    auth.create_token_review.side_effect = verify
    assert publish().status_code == 401
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags.get("training_result_receipt") is None


def test_lost_server_ack_recovers_committed_result_only(result_api, monkeypatch):
    from automl_api.services import training_result

    c, _, publish, receipt = result_api
    original = training_result.control_training_result

    def lost(*args, **kwargs):
        reply = original(*args, **kwargs)
        if not kwargs.get("receipt_only"):
            raise OSError("owned committed response loss")
        return reply

    monkeypatch.setattr(training_result, "control_training_result", lost)
    assert publish().status_code == 503
    assert receipt().status_code == 200
    with Session(c.scope[0]) as db:
        assert (
            db.scalar(
                select(func.count()).select_from(Metric).where(Metric.model_run_id == c.scope[1])
            )
            == 1
        )


def test_real_sql_loss_cannot_leave_partial_result_or_ack(result_api):
    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services.training_result import RESULT_EVENT
    from sqlalchemy import event, text

    c, _, publish, receipt = result_api
    killed = []

    def before_flush(db, *_):
        if any(isinstance(row, WorkflowEvent) and row.event_key == RESULT_EVENT for row in db.new):
            db.info["kill_result_commit"] = True

    def before_commit(db):
        db.flush()
        if db.info.get("kill_result_commit"):
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
        assert db.get(ModelRun, c.scope[1]).tags.get("training_result_receipt") is None
        assert (
            db.scalar(
                select(func.count()).select_from(Metric).where(Metric.model_run_id == c.scope[1])
            )
            == 0
        )
    assert publish().status_code == 200


@pytest.mark.parametrize(
    "change", ["metric", "direction", "step", "derived_params", "leaderboard", "event"]
)
def test_changed_stored_result_cannot_be_acknowledged(result_api, change):
    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services.training_result import RESULT_EVENT

    c, _, publish, receipt = result_api
    assert publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        run = db.get(ModelRun, c.scope[1])
        metric = db.scalars(select(Metric).where(Metric.model_run_id == run.id)).one()
        if change == "metric":
            metric.value = 0.9
        elif change == "direction":
            metric.higher_is_better = not metric.higher_is_better
        elif change == "step":
            metric.step = 1
        elif change == "derived_params":
            run.params = {**run.params, "deduplicated_rows": 999}
        elif change == "leaderboard":
            run.tags = {
                **run.tags,
                "leaderboard": [
                    {**run.tags["leaderboard"][0], "diagnostics": {"changed": True}},
                    *run.tags["leaderboard"][1:],
                ],
            }
        else:
            event = db.scalars(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.scope[2], WorkflowEvent.event_key == RESULT_EVENT
                )
            ).one()
            event.result_digest = "f" * 64
    assert receipt().status_code == 401


def test_current_protocol_cannot_complete_an_unmarked_sql_result(result_api, monkeypatch):
    from automl_api.training import pipeline

    c, body, _, _ = result_api
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(c.scope[2]))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", c.scope[3])
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(c.scope[0]))
    result = pipeline.TournamentResult(
        metrics=body["metrics"],
        model=None,
        params=body["params"],
        leaderboard=body["leaderboard"],
        primary_metric=body["primary_metric"],
    )
    assert pipeline._persist_training_success(c.scope[1], result, body["mlflow_run_id"])
    response = c.client.post(
        f"/internal/attempts/{c.scope[2]}/parent/complete",
        json={"run_id": str(c.scope[1]), "worker_id": str(uuid.uuid4())},
        headers=c.headers,
    )
    assert response.status_code == 401


def test_conflicting_publication_and_foreign_receipt_never_change_stored_result(result_api):
    c, original, publish, receipt = result_api
    assert publish().status_code == 200
    changed = copy.deepcopy(original)
    changed["mlflow_run_id"] = "different-tracking"
    assert publish(changed).status_code == receipt(changed).status_code == 401
    assert receipt().status_code == 200


def test_competing_identical_requests_have_one_commit_and_recoverable_receipt(result_api):
    from concurrent.futures import ThreadPoolExecutor

    c, _, publish, receipt = result_api
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(publish) for _ in range(2)]
        statuses = [future.result(timeout=30).status_code for future in futures]
    assert 200 in statuses and all(status in {200, 401} for status in statuses)
    assert receipt().status_code == 200
    with Session(c.scope[0]) as db:
        assert (
            db.scalar(
                select(func.count()).select_from(Metric).where(Metric.model_run_id == c.scope[1])
            )
            == 1
        )


def test_over_budget_result_never_reaches_publication(result_api):
    from automl_api.schemas.training_result import TRAINING_RESULT_MAX_BYTES

    c, _, _, _ = result_api
    response = c.client.post(
        f"/internal/attempts/{c.scope[2]}/training/result",
        content=b"x" * (TRAINING_RESULT_MAX_BYTES + 1),
        headers=c.headers,
    )
    assert response.status_code == 413 and response.headers["cache-control"] == "no-store"


def test_positive_label_type_changes_cannot_replay_receipt(result_api):
    c, body, publish, receipt = result_api
    body["params"]["positive_label"] = 0
    assert publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        run = db.get(ModelRun, c.scope[1])
        from sqlalchemy import update

        db.execute(
            update(ModelRun)
            .where(ModelRun.id == run.id)
            .values(params={**run.params, "positive_label": False})
        )
    assert receipt().status_code == 401


def test_derived_duplicate_count_cannot_exceed_training_partition(result_api):
    _, body, publish, _ = result_api
    body["params"]["deduplicated_rows"] = 999999
    assert publish().status_code == 401


def test_server_ranks_a_complete_tournament_and_refuses_a_losing_winner(result_api):
    from automl_api.training.evaluation import metric_direction

    c, body, publish, _ = result_api
    other = body["leaderboard"][1]["model"]
    response = c.upload(candidate_name=other, request_id=uuid.uuid4())
    assert response.status_code == 200
    artifact = response.json()
    score = 0.1 if metric_direction(body["primary_metric"]) == "minimize" else 0.9
    body["leaderboard"][1] = {
        "model": other,
        "status": "succeeded",
        "mlflow_run_id": c.ensure_tracking(other),
        "metrics": {body["primary_metric"]: score},
        "model_artifact_uri": artifact["uri"],
        "model_artifact_sha256": artifact["sha256"],
    }
    assert publish().status_code == 401
    body["leaderboard"].reverse()
    body["metrics"] = {body["primary_metric"]: score}
    assert publish().status_code == 200


@pytest.mark.parametrize(
    "field,value",
    [
        ("best_params", []),
        ("diagnostics", "invalid"),
        ("duration_seconds", "1.2"),
        ("duration_seconds", -1),
        ("training_rows", True),
        ("training_rows", -1),
        ("error", {}),
        ("cost_tier", {}),
        ("worker_pod", {}),
        ("resumed", "yes"),
    ],
)
def test_candidate_metadata_cannot_break_public_leaderboard_contract(result_api, field, value):
    _, body, publish, _ = result_api
    body["leaderboard"][0][field] = value
    assert publish().status_code == 422


def test_failed_candidate_cannot_smuggle_a_model_reference(result_api):
    _, body, publish, _ = result_api
    body["leaderboard"][1]["model_artifact_uri"] = "s3://another-project/model"
    assert publish().status_code == 422


def test_stored_winner_tracking_pointer_is_part_of_receipt_integrity(result_api):
    c, _, publish, receipt = result_api
    assert publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        run = db.get(ModelRun, c.scope[1])
        run.tags = {**run.tags, "winner_mlflow_run_id": "changed-tracking"}
    assert receipt().status_code == 401


def test_existing_foreign_run_cannot_be_substituted(result_api):
    c, body, publish, _ = result_api
    with Session(c.scope[0]) as db, db.begin():
        original = db.get(ModelRun, c.scope[1])
        other = ModelRun(
            project_id=original.project_id,
            dataset_version_id=original.dataset_version_id,
            created_by_id=original.created_by_id,
            run_kind=original.run_kind,
            status=original.status,
            task_type=original.task_type,
            target_column=original.target_column,
            params=dict(original.params),
            tags={},
        )
        db.add(other)
        db.flush()
        other_id = other.id
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, other_id) is not None
    body["run_id"] = str(other_id)
    assert publish().status_code == 401


def test_invalid_stored_winner_shape_cannot_be_acknowledged(result_api):
    c, _, publish, receipt = result_api
    assert publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        run = db.get(ModelRun, c.scope[1])
        run.tags = {**run.tags, "leaderboard": ["invalid"]}
    assert receipt().status_code == 401


def test_legacy_worker_completion_cannot_bypass_current_result_rpc(result_api, monkeypatch):
    from automl_api.services.training_parent import complete_parent
    from automl_api.services.workflow_state import StaleFence
    from automl_api.training import pipeline

    c, body, _, _ = result_api
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(c.scope[2]))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", c.scope[3])
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(c.scope[0]))
    result = pipeline.TournamentResult(
        metrics=body["metrics"],
        model=None,
        params=body["params"],
        leaderboard=body["leaderboard"],
        primary_metric=body["primary_metric"],
    )
    assert pipeline._persist_training_success(c.scope[1], result, body["mlflow_run_id"])
    with Session(c.scope[0]) as db, db.begin():
        with pytest.raises(StaleFence):
            complete_parent(db, c.scope[1], (c.scope[2], c.scope[3]))
        assert db.get(WorkflowAttempt, c.scope[2]).terminal_cas_version == 0


@pytest.mark.parametrize("valid", [True, False])
def test_observer_completion_requires_the_same_protocol_receipt(result_api, monkeypatch, valid):
    from automl_api.services import reconciler
    from automl_api.services.workflow_state import StaleFence
    from automl_api.training import pipeline

    c, body, publish, receipt = result_api
    if valid:
        assert publish().status_code == 200
    else:
        monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(c.scope[2]))
        monkeypatch.setenv("AUTOML_FENCING_TOKEN", c.scope[3])
        monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(c.scope[0]))
        result = pipeline.TournamentResult(
            metrics=body["metrics"],
            model=None,
            params=body["params"],
            leaderboard=body["leaderboard"],
            primary_metric=body["primary_metric"],
        )
        assert pipeline._persist_training_success(c.scope[1], result, body["mlflow_run_id"])

    def no_native_read(*args):
        raise AssertionError("stored result recovery opened a native job read")

    with Session(c.scope[0]) as db, db.begin():
        if valid:
            assert (
                reconciler.observe_training_ray_jobs(db, read=no_native_read, attempt_id=c.scope[2])
                == 1
            )
            db.flush()
            assert db.get(WorkflowAttempt, c.scope[2]).terminal_cas_version == 1
        else:
            with pytest.raises(StaleFence):
                reconciler.observe_training_ray_jobs(db, read=no_native_read, attempt_id=c.scope[2])
            assert db.get(WorkflowAttempt, c.scope[2]).terminal_cas_version == 0
    if valid:
        assert receipt().status_code == 200
