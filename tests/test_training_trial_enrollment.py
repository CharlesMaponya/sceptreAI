import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    EstimatorCatalogRevision,
    FeatureRecipeRevision,
    FeatureRegistryRevision,
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
)
from automl_api.services.training_trials import enroll_trial
from automl_api.services.workflow_state import StaleFence, transition_attempt
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401


@pytest.fixture
def trial_scope(publication):  # noqa: F811
    engine, run_id, parent_id, fence, _ = publication
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        catalog = EstimatorCatalogRevision(
            project_id=run.project_id, name="trial-catalog", revision=1,
            digest_scope="catalog", content_digest="b" * 64, release_version="fixture",
        )
        registry = FeatureRegistryRevision(
            project_id=run.project_id, name="trial-registry", revision=1,
            digest_scope="feature_registry", content_digest="c" * 64,
        )
        db.add_all([catalog, registry])
        db.flush()
        recipe = FeatureRecipeRevision(
            project_id=run.project_id, name="trial-recipe", revision=1,
            digest_scope="feature_recipe", content_digest="d" * 64,
            registry_revision_id=registry.id,
        )
        db.add(recipe)
        db.flush()
        candidate = TrainingCandidate(
            project_id=run.project_id, model_run_id=run.id,
            candidate_key="0:Ridge", estimator_key="Ridge",
            catalog_revision_id=catalog.id, feature_recipe_revision_id=recipe.id,
        )
        db.add(candidate)
        db.flush()
        return engine, run_id, parent_id, fence, candidate.id


def enroll(db, scope, **changes):
    _, _, parent_id, fence, candidate_id = scope
    arguments = dict(
        run_attempt_id=parent_id, run_fencing_token=fence, candidate_id=candidate_id,
        suggestion_id="suggestion-0", parameters={"alpha": 0.1, "seed": 42},
        execution_id="ray-trial-0",
    )
    arguments.update(changes)
    return enroll_trial(db, **arguments)


def test_enrollment_commits_one_typed_trial_and_replays_identity(trial_scope):
    engine, run_id, parent_id, _, candidate_id = trial_scope
    ids = []
    for _ in range(3):
        with Session(engine) as db, db.begin():
            attempt = enroll(db, trial_scope)
            ids.append(attempt.id)
            assert attempt.run_attempt_id == parent_id and attempt.model_run_id == run_id
            assert attempt.stage.value == "training_trial" and attempt.generation == 1
            assert attempt.status == AttemptStatus.PENDING
            assert attempt.ray_submission_id == "ray-trial-0"
    assert len(set(ids)) == 1
    with Session(engine) as db:
        assert db.scalar(select(func.count()).select_from(TrainingTrial).where(
            TrainingTrial.candidate_id == candidate_id
        )) == 1


@pytest.mark.parametrize("changed", [False, True])
def test_concurrent_trial_enrollment_converges_or_rejects_conflict(trial_scope, changed):
    engine = trial_scope[0]
    barrier = Barrier(2)

    def submit(index):
        barrier.wait(timeout=10)
        try:
            with Session(engine) as db, db.begin():
                row = enroll(db, trial_scope, parameters={"alpha": index if changed else 1})
                return row.id
        except ValueError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, range(2)))
    if changed:
        assert results.count("conflict") == 1
    else:
        assert results[0] == results[1]


@pytest.mark.parametrize("invalid", ["fence", "candidate", "parent", "run", "receipt", "lease"])
def test_invalid_enrollment_creates_no_trial(trial_scope, invalid):
    engine, run_id, parent_id, _, _ = trial_scope
    changes = {}
    with Session(engine) as db, db.begin():
        if invalid == "parent":
            db.get(WorkflowAttempt, parent_id).status = AttemptStatus.SUPERSEDED
        elif invalid == "lease":
            db.get(WorkflowAttempt, parent_id).lease_expires_at = (
                datetime.now(UTC) - timedelta(seconds=1)
            )
        elif invalid == "run":
            db.get(ModelRun, run_id).status = RunStatus.CANCELLED
        elif invalid == "receipt":
            db.get(ModelRun, run_id).tags = {
                "training_result_receipt": {"attempt_id": str(parent_id)}
            }
        elif invalid == "fence":
            changes["run_fencing_token"] = "wrong"
        else:
            original = db.get(TrainingCandidate, trial_scope[4])
            other_run = ModelRun(
                project_id=original.project_id,
                dataset_version_id=db.get(ModelRun, run_id).dataset_version_id,
                created_by_id=db.get(ModelRun, run_id).created_by_id,
            )
            db.add(other_run)
            db.flush()
            other_candidate = TrainingCandidate(
                project_id=original.project_id, model_run_id=other_run.id,
                candidate_key="other", estimator_key=original.estimator_key,
                catalog_revision_id=original.catalog_revision_id,
                feature_recipe_revision_id=original.feature_recipe_revision_id,
            )
            db.add(other_candidate)
            db.flush()
            changes["candidate_id"] = other_candidate.id
    with pytest.raises(StaleFence), Session(engine) as db, db.begin():
        enroll(db, trial_scope, **changes)
    with Session(engine) as db:
        assert db.scalar(select(func.count()).select_from(TrainingTrial).where(
            TrainingTrial.candidate_id == trial_scope[4]
        )) == 0


def test_enrollment_rejects_changed_execution_and_corrupted_parameters(trial_scope):
    engine = trial_scope[0]
    with Session(engine) as db, db.begin():
        trial_id = enroll(db, trial_scope).trial_id
    with pytest.raises(StaleFence), Session(engine) as db, db.begin():
        enroll(db, trial_scope, execution_id="untracked-second-dispatch")
    with Session(engine) as db, db.begin():
        db.get(TrainingTrial, trial_id).parameters = {"alpha": 99}
    with pytest.raises(ValueError, match="parameters conflict"), Session(engine) as db, db.begin():
        enroll(db, trial_scope)


def test_enrollment_rolls_back_with_dispatch_transaction(trial_scope):
    engine = trial_scope[0]
    with pytest.raises(ConnectionError), Session(engine) as db, db.begin():
        enroll(db, trial_scope)
        raise ConnectionError("before commit")
    with Session(engine) as db:
        assert db.scalar(select(func.count()).select_from(TrainingTrial).where(
            TrainingTrial.candidate_id == trial_scope[4]
        )) == 0


@pytest.mark.parametrize("blocked", ["active", "completed", "budget", None])
def test_parent_replacement_retains_logical_trial_and_requires_supersession(trial_scope, blocked):
    engine, run_id, parent_id, fence, candidate_id = trial_scope
    with Session(engine) as db, db.begin():
        old = enroll(db, trial_scope)
        old_id, trial_id, old_token = old.id, old.trial_id, old.fencing_token
        old.retry_budget = 1 if blocked == "budget" else 5
        parent = db.get(WorkflowAttempt, parent_id)
        transition_attempt(parent, AttemptStatus.SUPERSEDED, fencing_token=fence)
        if blocked != "active":
            transition_attempt(old, AttemptStatus.SUPERSEDED, fencing_token=old.fencing_token)
        if blocked == "completed":
            db.get(TrainingTrial, trial_id).result_digest = "e" * 64
        replacement = WorkflowAttempt(
            project_id=parent.project_id, model_run_id=run_id, stage="training_run",
            logical_key=parent.logical_key, workload_identity=parent.workload_identity,
            generation=2, fencing_token=uuid.uuid4().hex, status=AttemptStatus.RUNNING,
        )
        db.flush()
        db.add(replacement)
        db.flush()
        updated = (engine, run_id, replacement.id, replacement.fencing_token, candidate_id)
    if blocked:
        with pytest.raises(StaleFence), Session(engine) as db, db.begin():
            enroll(db, updated, execution_id="replacement-trial")
    else:
        with Session(engine) as db, db.begin():
            new = enroll(db, updated, execution_id="replacement-trial")
            assert new.trial_id == trial_id and new.generation == 2
            assert new.id != old_id and new.fencing_token != old_token
            assert new.predecessor_attempt_id == old_id
            assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.SUPERSEDED


@pytest.mark.parametrize("changes", [
    {"suggestion_id": " "}, {"execution_id": "x" * 256}, {"parameters": {"alpha": float("nan")}},
])
def test_enrollment_rejects_invalid_identity_or_nonfinite_parameters(trial_scope, changes):
    with pytest.raises(ValueError), Session(trial_scope[0]) as db, db.begin():
        enroll(db, trial_scope, **changes)


def test_lost_commit_ack_replays_same_trial_attempt(trial_scope):
    engine = trial_scope[0]

    class LostCommit(Session):
        def commit(self):
            super().commit()
            raise ConnectionError("commit acknowledgement lost")

    with LostCommit(engine) as db:
        original_id = enroll(db, trial_scope).id
        with pytest.raises(ConnectionError, match="acknowledgement lost"):
            db.commit()
    for _ in range(2):
        with Session(engine) as db, db.begin():
            assert enroll(db, trial_scope).id == original_id


def test_cached_parent_cannot_enroll_after_supersession(trial_scope):
    engine, _, parent_id, _, _ = trial_scope
    with Session(engine, expire_on_commit=False) as cached:
        parent = cached.get(WorkflowAttempt, parent_id)
        cached.commit()
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, parent_id).status = AttemptStatus.SUPERSEDED
        assert parent.status == AttemptStatus.RUNNING
        with pytest.raises(StaleFence):
            enroll(cached, trial_scope)
        cached.rollback()
