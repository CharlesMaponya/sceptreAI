"""A readable model URI alone cannot authorize declared training publication."""

import pytest
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline
from sqlalchemy.orm import Session
from test_training_model_output_api import bound_run as bound_run
from test_training_model_output_api import capacity_client as capacity_client
from test_training_model_output_api import committed_tenant as committed_tenant
from test_training_model_output_api import context_api as context_api
from test_training_model_output_api import model_api as model_api
from test_training_model_output_api import phase1_engine as phase1_engine
from test_training_model_output_api import selected_scope as selected_scope


@pytest.fixture
def publication(model_api, monkeypatch):
    c = model_api
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(c.scope[2]))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", c.scope[3])
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(c.scope[0]))
    entry = {
        "model": c.name,
        "status": "succeeded",
        "metrics": {"rmse": 0.2},
        "model_artifact_uri": "s3://owned/readable-but-never-authorized",
        "model_artifact_sha256": "a" * 64,
    }
    return c, entry


@pytest.mark.parametrize("kind", ["partial", "final"])
def test_declared_publication_rejects_model_without_verified_completion(publication, kind):
    c, entry = publication
    with pytest.raises(StaleFence, match="verified|custody|completion"):
        if kind == "partial":
            pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")
        else:
            result = pipeline.TournamentResult(
                metrics=entry["metrics"],
                model=None,
                params={},
                leaderboard=[entry],
                primary_metric="rmse",
            )
            pipeline._persist_training_success(c.scope[1], result, c.tracking.parent_id)


def authorized_entry(c):
    response = c.upload()
    assert response.status_code == 200, response.text
    data = response.json()
    return {
        "model": c.name,
        "status": "succeeded",
        "metrics": {"rmse": 0.2},
        "model_artifact_uri": data["uri"],
        "model_artifact_sha256": data["sha256"],
        "mlflow_run_id": c.ensure_tracking(),
    }


@pytest.mark.parametrize("kind", ["partial", "final"])
def test_only_verified_model_can_publish_and_replay(publication, kind):
    from automl_api.models.runs import Metric, ModelRun
    from sqlalchemy import func, select

    c, _ = publication
    entry = authorized_entry(c)
    for _ in range(2):
        if kind == "partial":
            pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")
        else:
            result = pipeline.TournamentResult(
                metrics=entry["metrics"],
                model=None,
                params={},
                leaderboard=[entry],
                primary_metric="rmse",
            )
            assert pipeline._persist_training_success(c.scope[1], result, c.tracking.parent_id)
    with Session(c.scope[0]) as db:
        run = db.get(ModelRun, c.scope[1])
        assert run.tags["leaderboard"][0]["model_artifact_uri"] == entry["model_artifact_uri"]
        assert db.scalar(
            select(func.count()).select_from(Metric).where(Metric.model_run_id == run.id)
        ) == (1 if kind == "final" else 0)


@pytest.mark.parametrize(
    "fault",
    ["uri", "hash", "candidate", "completion", "intent", "source", "duplicate", "environment"],
)
def test_publication_rejects_changed_native_or_source_authority(publication, fault, monkeypatch):
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import WorkflowAttemptCredentialGrant, WorkflowEvent
    from sqlalchemy import delete, select

    c, _ = publication
    entry = authorized_entry(c)
    entries = [entry]
    if fault == "uri":
        entry["model_artifact_uri"] += ".other"
    elif fault == "hash":
        entry["model_artifact_sha256"] = "0" * 64
    elif fault == "candidate":
        entry["model"] = "RandomForestRegressor"
    elif fault == "duplicate":
        entries.append(dict(entry))
    elif fault == "environment":
        monkeypatch.delenv("AUTOML_ATTEMPT_ID")
        monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    else:
        with Session(c.scope[0]) as db, db.begin():
            if fault == "source":
                run = db.get(ModelRun, c.scope[1])
                run.params = {**run.params, "sample_tier_rows": 123}
            else:
                row = db.scalars(
                    select(WorkflowAttemptCredentialGrant).where(
                        WorkflowAttemptCredentialGrant.attempt_id == c.scope[2]
                    )
                ).one()
                key = (
                    f"attempt.output.completed.{row.id}"
                    if fault == "completion"
                    else f"training.model.output.{c.candidate_id}"
                )
                db.execute(
                    delete(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == c.scope[2], WorkflowEvent.event_key == key
                    )
                )
    with pytest.raises((StaleFence, ValueError)):
        pipeline._persist_partial_leaderboard(c.scope[1], entries, "rmse")
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags.get("winner_model_artifact_uri") is None


def test_failed_candidates_need_no_model_and_legacy_policy_is_preserved(publication):
    from automl_api.models.workflows import OutboxEntry
    from sqlalchemy import select

    c, entry = publication
    pipeline._persist_partial_leaderboard(
        c.scope[1], [{"model": c.name, "status": "failed"}], "rmse"
    )
    with Session(c.scope[0]) as db, db.begin():
        row = db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == c.scope[1], OutboxEntry.topic == "ray.training.submit"
            )
        ).one()
        row.payload = {**row.payload, "credential_protocol": "ray-training-context-v4"}
    entry["status"] = "failed"
    pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")


@pytest.mark.parametrize("fault", [None, "allowlist", "source", "lineage", "legacy_slot"])
def test_replacement_reuses_only_explicit_input_bound_model(publication, monkeypatch, fault):
    import uuid

    from automl_api.models.enums import AttemptStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
    from automl_api.services import reconciler
    from automl_api.services.workflow_state import canonical_request_hash
    from sqlalchemy import select

    c, _ = publication
    entry = authorized_entry(c)
    pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")
    with Session(c.scope[0]) as db, db.begin():
        old = db.get(WorkflowAttempt, c.scope[2])
        new = reconciler._replace_lost_ray_attempt(db, old, reason="owned recovery fixture")
        assert new is not None and old.status == AttemptStatus.SUPERSEDED
        references = [
            ref
            for ref in new.predecessor_checkpoint_allowlist
            if isinstance(ref, dict) and ref.get("kind") == "training_model"
        ]
        assert len(references) == 1 and references[0]["candidate_id"] == str(c.candidate_id)
        new.status, new.ray_job_name = AttemptStatus.SUBMITTED, "owned-replacement"
        new_id, fence, generation = new.id, new.fencing_token, new.generation
    review, pod, _ = c.identity
    pod.metadata.labels["automl.platform/attempt-id"] = str(new_id)
    pod.metadata.labels["automl.platform/generation"] = str(generation)
    from automl_api.services.attempt_identity import attempt_audience

    review.status.audiences = [attempt_audience(new_id, fence)]
    response = c.client.post(
        f"/internal/attempts/{new_id}/parent/start",
        json={"run_id": str(c.scope[1]), "worker_id": str(uuid.uuid4()), "run_kind": "training"},
        headers=c.headers,
    )
    assert response.status_code == 200, response.text
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(new_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    if fault is not None:
        with Session(c.scope[0]) as db, db.begin():
            new = db.get(WorkflowAttempt, new_id)
            if fault == "allowlist":
                new.predecessor_checkpoint_allowlist = []
            elif fault == "source":
                run = db.get(ModelRun, c.scope[1])
                run.params = {**run.params, "sample_tier_rows": 123}
            elif fault == "lineage":
                new.predecessor_attempt_id = None
            else:
                event = db.scalars(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == c.scope[2],
                        WorkflowEvent.event_key == f"training.model.grant.{c.candidate_id}",
                    )
                ).one()
                event.payload = {
                    key: value for key, value in event.payload.items() if key != "input_digest"
                }
                event.result_digest = canonical_request_hash(event.payload)
        with pytest.raises((StaleFence, ValueError)):
            pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")
    else:
        pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")
        with Session(c.scope[0]) as db:
            run = db.get(ModelRun, c.scope[1])
            assert run.tags["leaderboard"][0]["model_artifact_uri"] == entry["model_artifact_uri"]


def test_sql_loss_rolls_back_candidate_and_metric_publication(publication):
    from automl_api.models.runs import Metric, ModelRun
    from automl_api.models.workflows import TrainingCandidate
    from sqlalchemy import event, func, select, text
    from sqlalchemy.exc import SQLAlchemyError

    c, _ = publication
    entry = authorized_entry(c)
    killed = []

    def before_commit(db):
        if any(isinstance(row, Metric) for row in db.new):
            pid = db.scalar(text("SELECT pg_backend_pid()"))
            with c.scope[0].connect().execution_options(isolation_level="AUTOCOMMIT") as other:
                assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(Session, "before_commit", before_commit)
    result = pipeline.TournamentResult(
        metrics=entry["metrics"], model=None, params={}, leaderboard=[entry], primary_metric="rmse"
    )
    try:
        with pytest.raises(SQLAlchemyError):
            pipeline._persist_training_success(c.scope[1], result, c.tracking.parent_id)
    finally:
        event.remove(Session, "before_commit", before_commit)
    assert len(killed) == 1
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).tags.get("training_result_receipt") is None
        assert db.get(TrainingCandidate, c.candidate_id).status != "succeeded"
        assert (
            db.scalar(
                select(func.count()).select_from(Metric).where(Metric.model_run_id == c.scope[1])
            )
            == 0
        )
    assert pipeline._persist_training_success(c.scope[1], result, c.tracking.parent_id)


def test_competing_identical_final_publications_commit_one_metric(publication):
    from concurrent.futures import ThreadPoolExecutor

    from automl_api.models.runs import Metric
    from sqlalchemy import func, select

    c, _ = publication
    entry = authorized_entry(c)

    def publish():
        result = pipeline.TournamentResult(
            metrics=entry["metrics"],
            model=None,
            params={},
            leaderboard=[dict(entry)],
            primary_metric="rmse",
        )
        return pipeline._persist_training_success(c.scope[1], result, c.tracking.parent_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [pool.submit(publish) for _ in range(2)]
        assert all(result.result(timeout=20) for result in results)
    with Session(c.scope[0]) as db:
        assert (
            db.scalar(
                select(func.count()).select_from(Metric).where(Metric.model_run_id == c.scope[1])
            )
            == 1
        )


def test_unknown_native_outcome_cannot_publish_its_readable_model(publication, monkeypatch):
    from automl_api.services import attempt_output_grants as outputs

    c, _ = publication

    def lose_native_ack(*args, **kwargs):
        c.native(*args, **kwargs)
        raise OSError("accepted native response lost")

    monkeypatch.setattr(outputs, "write_attempt_output", lose_native_ack)
    assert c.upload().status_code == 503
    row = c.saved()[0]
    assert row.status == "unknown" and row.object_uri in c.objects
    entry = {
        "model": c.name,
        "status": "succeeded",
        "metrics": {"rmse": 0.2},
        "model_artifact_uri": row.object_uri,
        "model_artifact_sha256": row.content_sha256,
    }
    with pytest.raises(StaleFence):
        pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")


def test_current_slot_input_digest_is_revalidated(publication):
    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services.workflow_state import canonical_request_hash
    from sqlalchemy import select

    c, _ = publication
    entry = authorized_entry(c)
    with Session(c.scope[0]) as db, db.begin():
        event = db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == f"training.model.grant.{c.candidate_id}",
            )
        ).one()
        event.payload = {**event.payload, "input_digest": "f" * 64}
        event.result_digest = canonical_request_hash(event.payload)
    with pytest.raises(StaleFence, match="input binding"):
        pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")


@pytest.mark.parametrize("state", ["cancelled", "superseded"])
def test_missing_environment_cannot_bypass_a_closed_declared_parent(
    publication, monkeypatch, state
):
    from automl_api.models.enums import AttemptStatus
    from automl_api.models.workflows import WorkflowAttempt

    c, entry = publication
    with Session(c.scope[0]) as db, db.begin():
        db.get(WorkflowAttempt, c.scope[2]).status = AttemptStatus(state)
    monkeypatch.delenv("AUTOML_ATTEMPT_ID")
    monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    with pytest.raises(StaleFence):
        pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")


@pytest.mark.parametrize("fault", ["fence", "unknown", "duplicate"])
def test_invalid_saved_declaration_never_downgrades_to_legacy(publication, monkeypatch, fault):
    import uuid

    from automl_api.models.workflows import OutboxEntry
    from sqlalchemy import select

    c, entry = publication
    with Session(c.scope[0]) as db, db.begin():
        row = db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == c.scope[1], OutboxEntry.topic == "ray.training.submit"
            )
        ).one()
        if fault == "fence":
            row.payload = {**row.payload, "fencing_token": "other-fence"}
        elif fault == "unknown":
            row.payload = {**row.payload, "credential_protocol": "unknown"}
        else:
            db.add(
                OutboxEntry(
                    project_id=row.project_id,
                    command_id=row.command_id,
                    event_key="duplicate-" + uuid.uuid4().hex,
                    topic=row.topic,
                    aggregate_type=row.aggregate_type,
                    aggregate_id=row.aggregate_id,
                    payload=dict(row.payload),
                )
            )
    monkeypatch.delenv("AUTOML_ATTEMPT_ID")
    monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    with pytest.raises(StaleFence):
        pipeline._persist_partial_leaderboard(c.scope[1], [entry], "rmse")
