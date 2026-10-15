"""Candidate read projection and atomic/fenced status persistence on PostgreSQL."""

import copy
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from automl_api.api.routes import contracts as routes
from automl_api.core.redaction import redact_mapping
from automl_api.models.enums import GlobalRole
from automl_api.models.iam import User
from automl_api.models.runs import Metric, ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    ExperimentSpecRevision,
    FeatureContractRevision,
    FeatureRecipeRevision,
    FeatureSearchSpaceRevision,
    SearchObjectiveRevision,
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowCheckpoint,
    WorkflowEvent,
)
from automl_api.services.candidate_details import get_candidate_detail
from automl_api.services.training_trials import finish_trial, start_trial
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training import pipeline
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import delete, event, func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import enroll, trial_scope  # noqa: F401


@pytest.fixture
def detail_scope(trial_scope):  # noqa: F811
    engine, run_id, parent_id, fence, candidate_id = trial_scope
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        candidate = db.get(TrainingCandidate, candidate_id)
        common = dict(
            project_id=run.project_id,
            revision=1,
            digest_scope="candidate-detail-v1",
            content_digest="e" * 64,
        )
        split = DatasetSplitRevision(
            **common,
            dataset_version_id=run.dataset_version_id,
            train_digest="1" * 64,
            validation_digest="2" * 64,
            final_test_digest="3" * 64,
            sealed_at=datetime.now(UTC),
        )
        contract = FeatureContractRevision(
            **common, name="detail-contract", task_type="regression", target_column="target"
        )
        search = FeatureSearchSpaceRevision(
            **common, name="detail-search", metric_name="rmse", metric_direction="minimize"
        )
        objective = SearchObjectiveRevision(
            **common, name="detail-objective", metric_name="rmse", metric_direction="minimize"
        )
        db.add_all([split, contract, search, objective])
        db.flush()
        spec = ExperimentSpecRevision(
            **common,
            name="detail-experiment",
            dataset_version_id=run.dataset_version_id,
            split_revision_id=split.id,
            feature_contract_revision_id=contract.id,
            feature_search_space_revision_id=search.id,
            search_objective_revision_id=objective.id,
            catalog_revision_id=candidate.catalog_revision_id,
            task_type="regression",
            target_column="target",
            primary_metric="rmse",
        )
        db.add(spec)
        db.flush()
        run.params = {
            "experiment_spec_revision_id": str(spec.id),
            "split_revision_id": str(split.id),
            "feature_contract_revision_id": str(contract.id),
            "feature_registry_revision_id": str(
                db.get(
                    FeatureRecipeRevision, candidate.feature_recipe_revision_id
                ).registry_revision_id
            ),
            "feature_search_space_revision_id": str(search.id),
            "sample_tier_rows": 100,
            "validation_sample_rows": 20,
            "sample_tier_policy": "seeded",
        }
        run.cpu_request_cores = 1
        run.cpu_limit_cores = 2
        run.memory_request_mb = 512
        run.memory_limit_mb = 1024
        run.tags = {
            "leaderboard": [{"model": "Ridge", "status": "pending", "metrics": {}, "error": None}]
        }
        candidate.params = {
            "resource_class": "low",
            "password": "candidate-password",
            "nested": [[{"api_key": "candidate-key"}]],
        }
        parent = db.get(WorkflowAttempt, parent_id)
        parent.ray_job_name = "owned-ray-job"
        parent.ray_cluster_name = "owned-ray-cluster"
        attempt = enroll(db, trial_scope, parameters={"alpha": 0.1, "password": "trial-password"})
        start_trial(
            db,
            attempt_id=attempt.id,
            fencing_token=attempt.fencing_token,
            run_fencing_token=fence,
            worker_id="detail-worker",
        )
        finish_trial(
            db,
            attempt_id=attempt.id,
            fencing_token=attempt.fencing_token,
            run_fencing_token=fence,
            worker_id="detail-worker",
            result={
                "scores": [0.5, 0.6],
                "nested": [
                    [
                        {
                            "password": "result-password",
                            "url": "https://user:pass@storage.test/model?token=private",
                        }
                    ]
                ],
            },
        )
        dispatch = {"ray_trial_id": "physical-ray-trial", "suggestion_id": "suggestion-0"}
        db.add(
            WorkflowEvent(
                project_id=run.project_id,
                attempt_id=attempt.id,
                event_key="trial.dispatch:physical-ray-trial",
                event_type="trial.dispatch",
                sequence=2,
                payload=dispatch,
                result_digest=canonical_request_hash(dispatch),
            )
        )
        db.add(
            WorkflowCheckpoint(
                project_id=run.project_id,
                attempt_id=attempt.id,
                sequence=1,
                event_sequence=1,
                object_uri="https://user:pass@storage.test/checkpoint?token=checkpoint-secret",
                content_digest="4" * 64,
            )
        )
        child_id = attempt.id
    try:
        yield (*trial_scope, child_id)
    finally:
        with Session(engine) as db, db.begin():
            project_id = db.get(ModelRun, run_id).project_id
            db.execute(
                delete(ExperimentSpecRevision).where(
                    ExperimentSpecRevision.project_id == project_id
                )
            )
            db.execute(
                delete(DatasetSplitRevision).where(DatasetSplitRevision.project_id == project_id)
            )


def read(scope):
    engine, run_id, _, _, candidate_id, _ = scope
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        return get_candidate_detail(
            db, db.get(User, run.created_by_id), run.project_id, run_id, candidate_id
        )


def test_detail_returns_complete_recorded_graph_and_no_credentials(detail_scope):
    result = read(detail_scope)
    assert result.lineage_state == "recorded"
    assert result.split_digests == {
        "train": "1" * 64,
        "validation": "2" * 64,
        "final_test": "3" * 64,
    }
    assert result.resources["run_cpu_limit_cores"] == 2
    assert result.resources["resource_class"] == "low"
    assert result.sample_tier["sample_tier_rows"] == 100
    assert result.run_attempts[0].ray_job_name == "owned-ray-job"
    assert len(result.trials) == 1 and len(result.trials[0].attempts) == 1
    attempt = result.trials[0].attempts[0]
    assert attempt.ray_trial_ids == ["physical-ray-trial"]
    assert attempt.receipt.state == "verified" and attempt.receipt.result["scores"] == [0.5, 0.6]
    assert attempt.checkpoints[0].content_digest == "4" * 64
    assert "fencing_token" not in attempt.model_dump()
    assert "workload_identity" not in attempt.model_dump()
    body = result.model_dump_json()
    for value in (
        "candidate-password",
        "candidate-key",
        "trial-password",
        "result-password",
        "user:pass",
        "token=private",
        "checkpoint-secret",
    ):
        assert value not in body


@pytest.mark.parametrize("change", ["digest", "type", "parameters", "cas", "parent"])
def test_inconsistent_trial_receipts_never_publish_result(detail_scope, change):
    engine, _, parent_id, _, _, child_id = detail_scope
    with Session(engine) as db, db.begin():
        terminal = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == child_id, WorkflowEvent.event_key == "trial.terminal"
            )
        )
        child = db.get(WorkflowAttempt, child_id)
        if change == "digest":
            terminal.result_digest = "f" * 64
        if change == "type":
            terminal.event_type = "unrelated"
        if change == "parameters":
            db.get(TrainingTrial, child.trial_id).parameters = {"alpha": 99}
        if change == "cas":
            child.terminal_cas_version = 0
        if change == "parent":
            db.get(WorkflowAttempt, parent_id).workload_identity = "wrong-parent-identity"
    receipt = read(detail_scope).trials[0].attempts[0].receipt
    assert receipt.state == "inconsistent"
    assert receipt.result is None and receipt.result_digest is None


def test_missing_receipt_is_reported_without_repair(detail_scope):
    engine, _, _, _, _, child_id = detail_scope
    with Session(engine) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == child_id, WorkflowEvent.event_key == "trial.terminal"
            )
        )
    assert read(detail_scope).trials[0].attempts[0].receipt.state == "missing"


def test_foreign_candidate_trials_and_split_are_not_adopted(detail_scope):
    engine, run_id, _, _, candidate_id, _ = detail_scope
    with Session(engine) as db, db.begin():
        candidate = db.get(TrainingCandidate, candidate_id)
        other = TrainingCandidate(
            project_id=candidate.project_id,
            model_run_id=run_id,
            candidate_key="1:Other",
            estimator_key="Other",
            catalog_revision_id=candidate.catalog_revision_id,
            feature_recipe_revision_id=candidate.feature_recipe_revision_id,
        )
        db.add(other)
        db.flush()
        enroll(db, detail_scope[:5], candidate_id=other.id, suggestion_id="other-suggestion")
        run = db.get(ModelRun, run_id)
        run.params = {**run.params, "split_revision_id": str(uuid.uuid4())}
    result = read(detail_scope)
    assert len(result.trials) == 1 and result.trials[0].suggestion_id == "suggestion-0"
    assert result.lineage_state == "inconsistent" and result.split_digests == {}


@pytest.mark.parametrize("writer", ["phase", "partial", "final"])
def test_status_is_atomic_with_fenced_candidate_publication(detail_scope, publication, writer):  # noqa: F811
    engine, run_id, _, _, candidate_id, _ = detail_scope
    if writer == "phase":
        pipeline._persist_candidate_phase(run_id, "Ridge", "cross_validating")
    if writer == "partial":
        pipeline._persist_partial_leaderboard(
            run_id,
            [
                {
                    "model": "Ridge",
                    "status": "succeeded",
                    "metrics": {"rmse": 1.25},
                    "model_artifact_sha256": "b" * 64,
                }
            ],
            "rmse",
        )
    if writer == "final":
        result = publication[-1]
        result.leaderboard = [
            {
                "model": "Ridge",
                "status": "succeeded",
                "metrics": {"r2": 0.9},
                "model_artifact_sha256": "b" * 64,
            }
        ]
        assert pipeline._persist_training_success(run_id, result, "candidate-mlflow")
    expected = "running" if writer == "phase" else "succeeded"
    with Session(engine) as db:
        assert db.get(TrainingCandidate, candidate_id).status == expected
        assert db.get(ModelRun, run_id).tags["leaderboard"][0]["status"] == expected
    result = read(detail_scope)
    assert result.status == result.recorded_status == expected
    if writer != "phase":
        assert result.artifact_digests == {"model_sha256": "b" * 64}


@pytest.mark.parametrize("writer", ["phase", "partial", "final"])
def test_stale_candidate_writer_changes_nothing(detail_scope, publication, monkeypatch, writer):  # noqa: F811
    engine, run_id, _, _, candidate_id, _ = detail_scope
    with Session(engine) as db:
        before = copy.deepcopy(db.get(ModelRun, run_id).tags)
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", "stale-candidate-fence")
    with pytest.raises(StaleFence):
        if writer == "phase":
            pipeline._persist_candidate_phase(run_id, "Ridge", "fit")
        if writer == "partial":
            pipeline._persist_partial_leaderboard(
                run_id,
                [{"model": "Ridge", "status": "succeeded", "metrics": {"rmse": 1.25}}],
                "rmse",
            )
        if writer == "final":
            pipeline._persist_training_success(run_id, publication[-1], "late-mlflow")
    with Session(engine) as db:
        assert db.get(TrainingCandidate, candidate_id).status == "pending"
        assert db.get(ModelRun, run_id).tags == before
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run_id))
            == 0
        )


def test_terminal_candidate_does_not_regress(detail_scope):
    engine, run_id, _, _, candidate_id, _ = detail_scope
    pipeline._persist_partial_leaderboard(
        run_id, [{"model": "Ridge", "status": "succeeded", "metrics": {"rmse": 1.25}}], "rmse"
    )
    pipeline._persist_candidate_phase(run_id, "Ridge", "fit-again")
    pipeline._persist_partial_leaderboard(
        run_id, [{"model": "Ridge", "status": "pending", "metrics": {}}], "rmse"
    )
    with Session(engine) as db:
        assert db.get(TrainingCandidate, candidate_id).status == "succeeded"
        assert db.get(ModelRun, run_id).tags["leaderboard"][0]["metrics"] == {"rmse": 1.25}


def test_candidate_http_reads_are_scoped_redacted_and_read_only(detail_scope):
    engine, run_id, _, _, candidate_id, _ = detail_scope
    writes = []

    def observe(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE"}:
            writes.append(statement)

    event.listen(engine, "before_cursor_execute", observe)
    try:
        with Session(engine) as db:
            run = db.get(ModelRun, run_id)
            owner = db.get(User, run.created_by_id)
            app = FastAPI()
            app.include_router(routes.router)
            app.dependency_overrides[routes.get_db] = lambda: db
            app.dependency_overrides[routes.get_current_user] = lambda: owner
            with TestClient(app) as client:
                base = f"/projects/{run.project_id}/training/runs/{run_id}/candidates"
                response = client.get(f"{base}/{candidate_id}")
                assert response.status_code == 200, response.text
                assert response.json()["trials"][0]["attempts"][0]["receipt"]["state"] == "verified"
                listed = client.get(base)
                assert listed.status_code == 200, listed.text
                for text in (response.text, listed.text):
                    assert "candidate-password" not in text and "candidate-key" not in text
                assert (
                    client.get(
                        f"/projects/{run.project_id}/training/runs/{uuid.uuid4()}/candidates/{candidate_id}"
                    ).status_code
                    == 404
                )
                app.dependency_overrides[routes.get_current_user] = lambda: SimpleNamespace(
                    id=uuid.uuid4(), global_role=GlobalRole.MEMBER
                )
                assert client.get(f"{base}/{candidate_id}").status_code in {403, 404}
        assert not writes
    finally:
        event.remove(engine, "before_cursor_execute", observe)


def test_recursive_read_redaction_removes_credentials_at_every_array_depth():
    value = {
        "nested": [
            [
                [
                    {
                        "password": "secret-pass",
                        "api_key": "secret-key",
                        "message": "password=plain api_key=plain",
                    }
                ]
            ]
        ],
        "safe": [[1, 2]],
    }
    redacted = redact_mapping(value)
    assert "secret-pass" not in json.dumps(redacted) and "secret-key" not in json.dumps(redacted)
    assert "=plain" not in json.dumps(redacted)
    assert redacted["safe"] == [[1, 2]]


def test_existing_candidate_id_cannot_cross_project_boundary(detail_scope):
    from automl_api.models.enums import ProjectRole
    from automl_api.models.projects import Project, ProjectMembership

    engine, run_id, _, _, candidate_id, _ = detail_scope
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        user = db.get(User, run.created_by_id)
        neighbor = Project(
            owner_id=user.id, created_by_id=user.id, name="candidate-boundary-neighbor"
        )
        db.add(neighbor)
        db.flush()
        db.add(
            ProjectMembership(
                project_id=neighbor.id,
                user_id=user.id,
                role=ProjectRole.OWNER,
                accepted_at=datetime.now(UTC),
            )
        )
        db.flush()
        with pytest.raises(HTTPException) as error:
            get_candidate_detail(db, user, neighbor.id, run_id, candidate_id)
        assert error.value.status_code == 404
        db.rollback()


@pytest.mark.parametrize("ambiguous", ["row", "summary", "extension"])
def test_candidate_never_claims_ambiguous_or_extension_metrics(detail_scope, ambiguous):
    engine, run_id, _, _, candidate_id, _ = detail_scope
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        candidate = db.get(TrainingCandidate, candidate_id)
        entry = {
            "model": "Ridge",
            "status": "succeeded",
            "metrics": {"rmse": 1.25},
            "model_artifact_sha256": "b" * 64,
        }
        if ambiguous == "row":
            db.add(
                TrainingCandidate(
                    project_id=run.project_id,
                    model_run_id=run_id,
                    candidate_key="duplicate:Ridge",
                    estimator_key="Ridge",
                    catalog_revision_id=candidate.catalog_revision_id,
                    feature_recipe_revision_id=candidate.feature_recipe_revision_id,
                )
            )
        if ambiguous == "extension":
            entry["extension_run_id"] = str(uuid.uuid4())
        run.tags = {"leaderboard": [entry, dict(entry)] if ambiguous == "summary" else [entry]}
    result = read(detail_scope)
    assert result.summary_state == ("missing" if ambiguous == "extension" else "ambiguous")
    assert not result.metrics and not result.artifact_digests


def test_terminal_conflict_rolls_back_candidate_run_and_metrics(detail_scope, publication):  # noqa: F811
    engine, run_id, _, _, candidate_id, _ = detail_scope
    pipeline._persist_partial_leaderboard(
        run_id,
        [{"model": "Ridge", "status": "failed", "metrics": {}, "error": "deterministic failure"}],
        "rmse",
    )
    with Session(engine) as db:
        before = copy.deepcopy(db.get(ModelRun, run_id).tags)
    result = publication[-1]
    result.leaderboard = [{"model": "Ridge", "status": "succeeded", "metrics": {"r2": 0.9}}]
    with pytest.raises(StaleFence, match="terminal candidate"):
        pipeline._persist_training_success(run_id, result, "conflicting-success")
    with Session(engine) as db:
        assert db.get(TrainingCandidate, candidate_id).status == "failed"
        assert db.get(ModelRun, run_id).tags == before
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run_id))
            == 0
        )


def test_candidate_history_uses_constant_query_count(detail_scope):
    engine = detail_scope[0]
    queries = []

    def observe(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            queries.append(statement)

    event.listen(engine, "before_cursor_execute", observe)
    try:
        first = read(detail_scope)
        first_count = len(queries)
        with Session(engine) as db, db.begin():
            for index in range(20):
                enroll(db, detail_scope[:5], suggestion_id=f"additional-{index}")
        queries.clear()
        second = read(detail_scope)
        second_count = len(queries)
        assert len(first.trials) == 1 and len(second.trials) == 21
        assert first_count == second_count
        assert first_count < 20
    finally:
        event.remove(engine, "before_cursor_execute", observe)


def test_invalid_dispatch_digest_does_not_expose_ray_trial_identity(detail_scope):
    engine, _, _, _, _, child_id = detail_scope
    with Session(engine) as db, db.begin():
        row = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == child_id, WorkflowEvent.event_type == "trial.dispatch"
            )
        )
        row.result_digest = "bad-digest"
    assert not read(detail_scope).trials[0].attempts[0].ray_trial_ids


@pytest.mark.parametrize(
    "value",
    [
        '{"password":"quoted-password","safe":"diagnostic"}',
        "{'api_key': 'quoted-key'}",
        "Authorization: Bearer header-credential",
        "authorization=Basic basic-credential",
        '{"password":"escaped\\"credential"}',
    ],
)
def test_string_credentials_and_authorization_schemes_are_redacted(value):
    from automl_api.core.redaction import redact_text

    result = redact_text(value)
    for secret in (
        "quoted-password",
        "quoted-key",
        "header-credential",
        "basic-credential",
        "escaped",
        "credential",
    ):
        assert secret not in result
    if '"safe"' in value:
        assert json.loads(result) == {"password": "<redacted>", "safe": "diagnostic"}


def test_irrelevant_progress_payloads_are_filtered_in_the_database(detail_scope):
    engine, run_id, parent_id, _, _, _ = detail_scope
    with Session(engine) as db, db.begin():
        project_id = db.get(ModelRun, run_id).project_id
        for index in range(100):
            db.add(
                WorkflowEvent(
                    project_id=project_id,
                    attempt_id=parent_id,
                    sequence=100 + index,
                    event_key=f"progress-{index}",
                    event_type="training.progress",
                    payload={"private": "parent-progress-secret"},
                )
            )
    returned = []

    def observe(_conn, cursor, statement, _parameters, _context, _many):
        if "FROM workflow_events" in statement:
            returned.append(cursor.rowcount)

    event.listen(engine, "after_cursor_execute", observe)
    try:
        result = read(detail_scope)
        assert returned == [2]
        assert "parent-progress-secret" not in result.model_dump_json()
    finally:
        event.remove(engine, "after_cursor_execute", observe)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "status": "succeeded",
            "result": [],
        },
        {"status": "succeeded", "result": {}, "failure": 17},
        {"status": "succeeded", "result": {}, "extra": "unexpected"},
    ],
)
def test_well_hashed_malformed_receipt_is_not_trusted(detail_scope, payload):
    engine, _, _, _, _, child_id = detail_scope
    with Session(engine) as db, db.begin():
        child = db.get(WorkflowAttempt, child_id)
        terminal = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == child_id, WorkflowEvent.event_key == "trial.terminal"
            )
        )
        terminal.payload = payload
        terminal.result_digest = canonical_request_hash(payload)
        db.get(TrainingTrial, child.trial_id).result_digest = terminal.result_digest
    receipt = read(detail_scope).trials[0].attempts[0].receipt
    assert receipt.state == "inconsistent" and receipt.result is None


@pytest.mark.parametrize("key", ["private_key", "aws_access_key_id", "serviceAccountPrivateKey"])
def test_provider_key_credentials_are_redacted_in_maps_and_json_strings(key):
    source = {
        "deep": [[{key: "SENSITIVE-TEST-VALUE"}]],
        "encoded": json.dumps({key: "SENSITIVE-TEST-VALUE"}),
    }
    assert "SENSITIVE-TEST-VALUE" not in json.dumps(redact_mapping(source))


def test_failed_trial_receipt_retains_a_redacted_terminal_reason(detail_scope):
    engine, _, _, fence, _, _ = detail_scope
    with Session(engine) as db, db.begin():
        attempt = enroll(db, detail_scope[:5], suggestion_id="failed-suggestion")
        start_trial(
            db,
            attempt_id=attempt.id,
            fencing_token=attempt.fencing_token,
            run_fencing_token=fence,
            worker_id="failure-worker",
        )
        finish_trial(
            db,
            attempt_id=attempt.id,
            fencing_token=attempt.fencing_token,
            run_fencing_token=fence,
            worker_id="failure-worker",
            failure='backend failed {"password":"SENSITIVE-FAILURE"}',
        )
    trial = next(t for t in read(detail_scope).trials if t.suggestion_id == "failed-suggestion")
    assert trial.status == "failed"
    assert trial.attempts[0].receipt.state == "verified"
    assert trial.attempts[0].receipt.result is None
    assert "SENSITIVE-FAILURE" not in trial.attempts[0].receipt.failure
