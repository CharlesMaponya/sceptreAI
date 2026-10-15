"""Legacy selected requests become durable, project-scoped experiment identities."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import Dataset, DatasetVersion, ProfilingJob
from automl_api.models.enums import DatasetFormat, DatasetStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.workflows import (
    DatasetSplitRevision,
    ExperimentSpecRevision,
    PreparedArtifact,
    SearchObjectiveRevision,
)
from automl_api.preparation_worker import _feature_launch_revisions
from automl_api.schemas.contracts import ExperimentSpecCreate
from automl_api.schemas.training import TrainingEstimateRequest, TrainingLaunchRequest
from automl_api.services import contracts, training
from automl_api.services.experiment_identity import SPEC_SCOPE, resolve_training_identity
from fastapi import HTTPException
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_service_runtime import _estimate


def publish_graph(db, user_id, project_id, task, version=None):
    if version is None:
        dataset = Dataset(project_id=project_id, created_by_id=user_id, name="selected-identity")
        db.add(dataset)
        db.flush()
        version = DatasetVersion(
            project_id=project_id,
            dataset_id=dataset.id,
            created_by_id=user_id,
            version_number=1,
            status=DatasetStatus.READY,
            format=DatasetFormat.CSV,
            object_uri="s3://identity-fixture/raw.csv",
            content_hash="a" * 64,
            byte_size=300,
            row_count=12,
            column_count=3,
            schema_json={"columns": [{"name": "x"}, {"name": "target"}, {"name": "time"}]},
        )
        db.add(version)
        db.flush()
    revision = (
        db.scalar(
            select(func.max(DatasetSplitRevision.revision)).where(
                DatasetSplitRevision.dataset_version_id == version.id
            )
        )
        or 0
    ) + 1
    split = DatasetSplitRevision(
        project_id=project_id,
        dataset_version_id=version.id,
        revision=revision,
        digest_scope="split-v1",
        content_digest="b" * 64,
        train_digest="1" * 64,
        validation_digest="2" * 64,
        final_test_digest="3" * 64,
        sealed_at=datetime.now(UTC),
        specification={
            "split_counts": {"train": 8, "validation": 2, "final_test": 2},
            "split_strategy": "temporal" if task == TaskType.TIME_SERIES else "hash",
            "time_column": "time" if task == TaskType.TIME_SERIES else None,
            "uris": {
                "train": "s3://identity-fixture/train",
                "validation": "s3://identity-fixture/validation",
                "final_input": "s3://identity-fixture/final",
            },
        },
    )
    db.add(split)
    db.flush()
    target = None if task == TaskType.CLUSTERING else "target"
    profile = ProfilingJob(
        project_id=project_id,
        dataset_id=version.dataset_id,
        dataset_version_id=version.id,
        created_by_id=user_id,
        target_column=target,
        status="succeeded",
        finished_at=datetime.now(UTC),
        overview_json={
            "split_revision_id": str(split.id),
            "task_inference": {"task_type": task.value},
        },
        feature_profiles_json={"target": {"distinct_count": 2, "sample_values": ["no", "yes"]}},
    )
    artifact = PreparedArtifact(
        project_id=project_id,
        dataset_version_id=version.id,
        revision=revision,
        digest_scope="prepared-v1",
        content_digest="c" * 64,
        object_uri="s3://identity-fixture/prepared",
        byte_size=200,
        row_count=12,
        specification={
            "source_digest": version.content_hash,
            "identity": {"split_digests": {"train": split.train_digest}},
        },
    )
    db.add_all([profile, artifact])
    db.flush()
    bindings = _feature_launch_revisions(
        db,
        profile,
        artifact,
        {
            "task_inference": {"task_type": task.value},
            "leakage_analysis": {"excluded_columns": []},
            "columns": [
                {"name": name, "semantic_type": "numerical_continuous", "quality_flags": []}
                for name in ("x", "target", "time")
            ],
        },
    )
    profile.overview_json = {**profile.overview_json, "launch_bindings": bindings}
    return version, profile, bindings


@pytest.fixture
def selected_scope(phase1_engine, committed_tenant, request):  # noqa: F811
    task = getattr(request, "param", TaskType.REGRESSION)
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        version, profile, bindings = publish_graph(db, user_id, project_id, task)
        version_id = version.id
    body = {
        "dataset_version_id": version_id,
        "task_type": task,
        "target_column": None if task == TaskType.CLUSTERING else "target",
        "candidate_models": [
            {TaskType.CLASSIFICATION: "LogisticRegression", TaskType.CLUSTERING: "KMeans"}.get(
                task, "Ridge"
            )
        ],
        "prefer_gpu": False,
    }
    try:
        yield phase1_engine, user_id, project_id, body, bindings
    finally:
        with Session(phase1_engine) as db, db.begin():
            db.execute(
                delete(ExperimentSpecRevision).where(
                    ExperimentSpecRevision.project_id == project_id
                )
            )
            db.execute(
                delete(DatasetSplitRevision).where(DatasetSplitRevision.project_id == project_id)
            )


@pytest.mark.parametrize(
    "selected_scope",
    [TaskType.REGRESSION, TaskType.CLASSIFICATION, TaskType.CLUSTERING, TaskType.TIME_SERIES],
    indirect=True,
)
def test_legacy_selected_resolves_and_replays_one_complete_identity(selected_scope):
    engine, _, project_id, body, bindings = selected_scope
    with Session(engine) as db, db.begin():
        resolved = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        identity = resolved.experiment_spec_revision_id
        assert identity is not None
        assert str(resolved.split_revision_id) == bindings["split_revision_id"]
        assert resolved.feature_registry_revision_id and resolved.feature_recipe_revision_id
        row = db.get(ExperimentSpecRevision, identity)
        assert row.digest_scope == SPEC_SCOPE
    with Session(engine) as db, db.begin():
        replay = resolve_training_identity(db, project_id, TrainingLaunchRequest(**body))
        assert replay.experiment_spec_revision_id == identity
        assert replay.estimator_catalog_revision_id == replay.catalog_revision_id
        assert (
            db.scalar(
                select(func.count())
                .select_from(ExperimentSpecRevision)
                .where(ExperimentSpecRevision.project_id == project_id)
            )
            == 1
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(SearchObjectiveRevision)
                .where(SearchObjectiveRevision.project_id == project_id)
            )
            == 1
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_version_id", uuid.uuid4()),
        ("split_revision_id", uuid.uuid4()),
        ("catalog_revision_id", uuid.uuid4()),
        ("feature_recipe_revision_id", uuid.uuid4()),
        ("target_column", None),
        ("positive_label", "yes"),
        ("evaluation_column", "x"),
        ("entity_column", "x"),
        ("event_time_column", "time"),
        ("prediction_time_column", "time"),
        ("excluded_columns", ["x"]),
        ("cv_folds", 4),
        ("optimization_iterations", 6),
    ],
)
def test_explicit_spec_cannot_be_overridden(selected_scope, field, value):
    engine, _, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        resolved = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db,
                project_id,
                TrainingEstimateRequest(
                    experiment_spec_revision_id=resolved.experiment_spec_revision_id,
                    **{field: value},
                ),
            )
        assert error.value.status_code == 422
        assert error.value.detail == {"code": "experiment_spec_mismatch", "field": field}


def test_profile_changes_do_not_silently_change_a_legacy_identity(selected_scope):
    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        old = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
    with Session(engine) as db, db.begin():
        version = db.get(DatasetVersion, body["dataset_version_id"])
        _, _, new_bindings = publish_graph(db, user_id, project_id, body["task_type"], version)
    with Session(engine) as db, db.begin():
        same = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        assert same.experiment_spec_revision_id == old.experiment_spec_revision_id
        assert str(same.split_revision_id) != new_bindings["split_revision_id"]
        explicit = resolve_training_identity(
            db,
            project_id,
            TrainingEstimateRequest(**body, split_revision_id=new_bindings["split_revision_id"]),
        )
        assert explicit.experiment_spec_revision_id != old.experiment_spec_revision_id


def test_concurrent_selected_requests_share_one_identity(selected_scope):
    engine, _, project_id, body, _ = selected_scope
    barrier = Barrier(4)

    def issue(_):
        with Session(engine) as db, db.begin():
            barrier.wait(timeout=5)
            return resolve_training_identity(
                db, project_id, TrainingEstimateRequest(**body)
            ).experiment_spec_revision_id

    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(issue, range(4)))
    assert len(set(ids)) == 1
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(ExperimentSpecRevision)
                .where(ExperimentSpecRevision.project_id == project_id)
            )
            == 1
        )


@pytest.mark.parametrize("change", ["body", "column", "binding"])
def test_modified_spec_or_graph_is_rejected(selected_scope, change):
    engine, _, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        resolved = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
    with Session(engine) as db, db.begin():
        spec = db.get(ExperimentSpecRevision, resolved.experiment_spec_revision_id)
        if change == "body":
            spec.specification = {**spec.specification, "tampered": True}
        if change == "column":
            spec.target_column = "time"
        if change == "binding":
            db.get(DatasetSplitRevision, resolved.split_revision_id).content_digest = "d" * 64
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db,
                project_id,
                TrainingEstimateRequest(
                    experiment_spec_revision_id=resolved.experiment_spec_revision_id
                ),
            )
        assert error.value.status_code == 409


def test_all_catalog_does_not_create_an_implicit_experiment(selected_scope):
    engine, _, project_id, body, _ = selected_scope
    body.pop("candidate_models")
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db, project_id, TrainingEstimateRequest(**body, catalog_mode="all")
            )
        assert error.value.detail["code"] == "experiment_spec_required"
        assert (
            db.scalar(
                select(func.count())
                .select_from(ExperimentSpecRevision)
                .where(ExperimentSpecRevision.project_id == project_id)
            )
            == 0
        )


def test_public_spec_metadata_does_not_override_validated_identity(selected_scope):
    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        frozen = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        payload = ExperimentSpecCreate(
            name="named-spec",
            specification={
                "identity": {"target_column": "wrong"},
                "runtime_env": {"pip": ["untrusted"]},
            },
            **frozen.model_dump(
                include=set(ExperimentSpecCreate.model_fields)
                & set(TrainingEstimateRequest.model_fields),
                exclude_none=True,
            ),
        )
        named = contracts.create_experiment_spec(db, db.get(User, user_id), project_id, payload)
        named_id = named.id
    with Session(engine) as db:
        actual = resolve_training_identity(
            db, project_id, TrainingEstimateRequest(experiment_spec_revision_id=named_id)
        )
        assert (
            actual.target_column == "target"
            and actual.dataset_version_id == body["dataset_version_id"]
        )


def test_reservation_replays_before_mutable_profile_or_capacity(selected_scope, monkeypatch):
    engine, user_id, project_id, body, _ = selected_scope
    client = MagicMock()
    client.estimate.side_effect = lambda **_: _estimate()
    monkeypatch.setattr(training, "_reconcile_active_runs", lambda *_: None)
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_: (None, {}))
    monkeypatch.setattr(
        training, "get_object_store", lambda: SimpleNamespace(exists=lambda _: True)
    )
    payload = TrainingEstimateRequest(**body, reserve_capacity=True)
    with Session(engine) as db, db.begin():
        first = training.estimate_training_run(
            db,
            db.get(User, user_id),
            project_id,
            payload,
            client,
            idempotency_key="reserve-identity",
        )
    assert first.experiment_spec_revision_id and first.capacity_reservation
    client.estimate.side_effect = AssertionError("capacity must not be probed on replay")
    monkeypatch.setattr(
        training,
        "_resolve_estimate_identity",
        MagicMock(side_effect=AssertionError("identity must not be resolved on replay")),
    )
    with Session(engine) as db, db.begin():
        replay = training.estimate_training_run(
            db,
            db.get(User, user_id),
            project_id,
            payload,
            client,
            idempotency_key="reserve-identity",
        )
        assert replay == first
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            training.estimate_training_run(
                db,
                db.get(User, user_id),
                project_id,
                payload.model_copy(update={"expected_minutes": 11}),
                client,
                idempotency_key="reserve-identity",
            )
        assert error.value.detail["code"] == "idempotency_key_reused"
