from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import CommandStatus, RunKind, RunStatus, ScopeStatus, TaskType
from automl_api.schemas.training import (
    ClusterCapacityRead,
    TrainingAddModelsRequest,
    TrainingEstimateRead,
    TrainingEstimateRequest,
    TrainingLaunchRequest,
)
from automl_api.services import training
from fastapi import HTTPException
from kubernetes.client import ApiException


class _ListResult:
    def __init__(self, values: list[object]) -> None:
        self.values = values

    def all(self) -> list[object]:
        return self.values


class _Session:
    def __init__(
        self,
        *,
        scalar_values: list[object | None] | None = None,
        list_values: list[list[object]] | None = None,
    ) -> None:
        self.scalar_values = list(scalar_values or [])
        self.list_values = list(list_values or [])
        self.flushes = 0
        self.refreshes = 0
        self.added: list[object] = []
        self.executed: list[tuple[object, object | None]] = []
        self.bind = None

    def scalar(self, _statement: object) -> object | None:
        return self.scalar_values.pop(0) if self.scalar_values else None

    def scalars(self, _statement: object) -> _ListResult:
        return _ListResult(self.list_values.pop(0) if self.list_values else [])

    def flush(self) -> None:
        self.flushes += 1
        now = datetime.now(UTC)
        for instance in self.added:
            if getattr(instance, "id", None) is None:
                instance.id = uuid.uuid4()
            if getattr(instance, "created_at", None) is None:
                instance.created_at = now
            if getattr(instance, "updated_at", None) is None:
                instance.updated_at = now

    def add(self, instance: object) -> None:
        self.added.append(instance)

    def execute(self, statement: object, params: object | None = None) -> None:
        self.executed.append((statement, params))

    def get(self, _model: object, identifier: object) -> object | None:
        return next(
            (instance for instance in self.added if getattr(instance, "id", None) == identifier),
            None,
        )

    def refresh(self, _run: object, *, with_for_update: bool = False) -> None:
        assert with_for_update
        self.refreshes += 1


def _capacity() -> ClusterCapacityRead:
    return ClusterCapacityRead(
        connected=True,
        source="kubernetes",
        total_cpu_cores=8,
        requested_cpu_cores=2,
        available_cpu_cores=6,
        total_memory_mb=16384,
        requested_memory_mb=2048,
        available_memory_mb=14336,
        ready_nodes=1,
        gpu_available=True,
        active_training_jobs=0,
    )


def _estimate() -> TrainingEstimateRead:
    return TrainingEstimateRead(
        capacity=_capacity(),
        estimated_working_set_mb=1024,
        cpu_request_cores=1,
        cpu_limit_cores=2,
        memory_request_mb=1024,
        memory_limit_mb=2048,
        gpu_requested=False,
        expected_minutes=10,
        active_deadline_seconds=3600,
        estimated_core_hours=0.2,
        max_concurrent_jobs=2,
        can_launch=True,
    )


def _version() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        object_uri="minio://datasets/train.csv",
        byte_size=4096,
        row_count=100,
        column_count=3,
        schema_json={
            "columns": [
                {"name": "feature"},
                {"name": "target"},
                {"name": "cluster_label"},
            ]
        },
    )


def _request(**updates: object) -> TrainingEstimateRequest:
    values = {
        "dataset_version_id": uuid.uuid4(),
        "target_column": "target",
        "task_type": TaskType.REGRESSION,
        "primary_metric": "rmse",
        "prefer_gpu": False,
        "candidate_limit": 2,
        "candidate_models": ["Ridge"],
        "optimization_iterations": 3,
        "cv_folds": 3,
    }
    values.update(updates)
    return TrainingEstimateRequest(**values)


def _run(status: RunStatus = RunStatus.RUNNING) -> SimpleNamespace:
    now = datetime.now(UTC) - timedelta(minutes=2)
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        run_kind=RunKind.TRAINING,
        status=status,
        task_type=TaskType.REGRESSION,
        target_column="target",
        k8s_job_name="training-job",
        tags={"completed_candidates": 1, "candidate_phase": "evaluating"},
        params={"candidate_limit": 3, "gpu_vendor": "nvidia", "gpu_resource": "gpu"},
        started_at=now,
        queued_at=now,
        created_at=now,
        finished_at=None,
        cpu_request_cores=1,
        cpu_limit_cores=2,
        memory_request_mb=1024,
        memory_limit_mb=2048,
        gpu_requested=True,
        failure_code=None,
        failure_message=None,
        plain_english_failure=None,
    )


def test_training_estimate_uses_catalog_cost_and_leakage_evidence(monkeypatch) -> None:
    version = _version()
    estimate = _estimate()
    client = MagicMock()
    client.estimate.return_value = estimate
    db = _Session(scalar_values=[version, 0, 0])
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(
        training,
        "get_object_store",
        lambda: SimpleNamespace(exists=lambda _uri: True),
    )
    monkeypatch.setattr(training, "_reconcile_active_runs", MagicMock())
    monkeypatch.setattr(
        training,
        "_latest_leakage_analysis",
        lambda *_args: (
            SimpleNamespace(id=uuid.uuid4()),
            {"excluded_columns": ["target_copy"]},
        ),
    )
    payload = _request(dataset_version_id=version.id)

    result = training.estimate_training_run(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        version.project_id,
        payload,
        client,
    )

    assert result.can_launch is True
    assert "target_copy" in result.warnings[0]
    kwargs = client.estimate.call_args.kwargs
    assert kwargs["dataset_bytes"] == 4096
    assert kwargs["candidate_limit"] == 1
    assert kwargs["optimization_iterations"] == 3


def test_training_estimate_accepts_completed_clear_leakage_profile(monkeypatch) -> None:
    version = _version()
    client = MagicMock()
    client.estimate.return_value = _estimate()
    db = _Session(scalar_values=[version, 0, 0])
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(
        training,
        "get_object_store",
        lambda: SimpleNamespace(exists=lambda _uri: True),
    )
    monkeypatch.setattr(training, "_reconcile_active_runs", lambda *_args: None)
    monkeypatch.setattr(
        training,
        "_latest_leakage_analysis",
        lambda *_args: (SimpleNamespace(id=uuid.uuid4()), {"excluded_columns": []}),
    )
    result = training.estimate_training_run(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        version.project_id,
        _request(dataset_version_id=version.id),
        client,
    )
    assert result.warnings == []


def test_training_estimate_blocks_missing_objects_and_concurrency(monkeypatch) -> None:
    version = _version()
    estimate = _estimate()
    client = MagicMock()
    client.estimate.return_value = estimate
    db = _Session(scalar_values=[version, 2, 1])
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(
        training,
        "get_object_store",
        lambda: SimpleNamespace(exists=lambda _uri: False),
    )
    monkeypatch.setattr(training, "_reconcile_active_runs", MagicMock())
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_args: (None, {}))

    result = training.estimate_training_run(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        version.project_id,
        _request(dataset_version_id=version.id),
        client,
    )

    assert result.can_launch is False
    blockers = " ".join(result.blockers)
    assert "object is missing" in blockers
    assert "Database concurrency limit" in blockers
    assert "already has an active" in blockers
    assert "No completed leakage profile" in " ".join(result.warnings)


def test_training_estimate_translates_invalid_metric_and_store_configuration(monkeypatch) -> None:
    version = _version()
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    invalid = _request(dataset_version_id=version.id, primary_metric="not-a-metric")
    with pytest.raises(HTTPException, match="metric") as metric_error:
        training.estimate_training_run(
            _Session(scalar_values=[version]),
            SimpleNamespace(),
            version.project_id,
            invalid,
            MagicMock(),
        )
    assert metric_error.value.status_code == 422

    client = MagicMock()
    client.estimate.return_value = _estimate()
    monkeypatch.setattr(
        training,
        "get_object_store",
        lambda: SimpleNamespace(exists=MagicMock(side_effect=ValueError("credentials missing"))),
    )
    with pytest.raises(HTTPException, match="credentials missing") as store_error:
        training.estimate_training_run(
            _Session(scalar_values=[version]),
            SimpleNamespace(),
            version.project_id,
            _request(dataset_version_id=version.id),
            client,
        )
    assert store_error.value.status_code == 503


def _launch_request(version_id: uuid.UUID) -> TrainingLaunchRequest:
    return TrainingLaunchRequest(
        dataset_version_id=version_id,
        target_column="target",
        task_type=TaskType.REGRESSION,
        primary_metric="rmse",
        prefer_gpu=False,
        candidate_limit=1,
        candidate_models=["Ridge"],
        optimization_iterations=3,
        cv_folds=3,
        run_name="qualified run",
        split_revision_id=uuid.uuid4(),
        feature_contract_revision_id=uuid.uuid4(),
        feature_registry_revision_id=uuid.uuid4(),
        feature_recipe_revision_id=uuid.uuid4(),
        feature_search_space_revision_id=uuid.uuid4(),
        estimator_catalog_revision_id=uuid.uuid4(),
    )


def _mock_durable_launch(monkeypatch, payload: TrainingLaunchRequest) -> None:
    command = SimpleNamespace(
        id=uuid.uuid4(),
        status=CommandStatus.PENDING,
        response_payload={},
        project_id=uuid.uuid4(),
    )
    revisions = {
        "catalog": SimpleNamespace(id=payload.estimator_catalog_revision_id),
        "recipe": SimpleNamespace(id=payload.feature_recipe_revision_id),
    }
    monkeypatch.setattr(training, "begin_command", lambda *_args, **_kwargs: (command, False))
    monkeypatch.setattr(training, "_resolve_launch_revisions", lambda *_args: revisions)
    monkeypatch.setattr(training, "enqueue_outbox", MagicMock())


def test_training_launch_builds_durable_pending_run_and_outbox_intent(monkeypatch) -> None:
    version = _version()
    estimate = _estimate()
    db = _Session()
    client = MagicMock()
    client.settings = SimpleNamespace(training_namespace="sceptre")
    client.build_job_manifest.side_effect = lambda **kwargs: {
        "metadata": {"name": f"training-{kwargs['run_id']}"}
    }
    monkeypatch.setattr(training, "_lock_training_admission", MagicMock())
    monkeypatch.setattr(training, "estimate_training_run", lambda *_args: estimate)
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_args: (None, {}))
    payload = _launch_request(version.id)
    _mock_durable_launch(monkeypatch, payload)

    result = training.launch_training_run(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        version.project_id,
        payload,
        client,
        idempotency_key="launch-one",
    )

    persisted = db.added[0]
    assert persisted.status == RunStatus.QUEUED
    assert persisted.run_kind == RunKind.TRAINING
    assert persisted.params["optimization_iterations"] == 3
    assert persisted.tags["leaderboard"][0]["model"] == "Ridge"
    assert persisted.tags["leaderboard"][0]["status"] == "pending"
    assert result.run.id == persisted.id
    assert result.manifest["executor"] == "kuberay"
    assert result.manifest["desiredState"] == "ray_submission_pending"
    client.create_job.assert_not_called()
    training.enqueue_outbox.assert_called_once()


def test_training_launch_does_not_call_cluster_in_request_transaction(monkeypatch) -> None:
    version = _version()
    db = _Session()
    client = MagicMock()
    client.settings = SimpleNamespace(training_namespace="sceptre")
    client.build_job_manifest.return_value = {"metadata": {"name": "training-failed"}}
    client.create_job.side_effect = RuntimeError("admission denied")
    monkeypatch.setattr(training, "_lock_training_admission", MagicMock())
    monkeypatch.setattr(training, "estimate_training_run", lambda *_args: _estimate())
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_args: (None, {}))
    payload = _launch_request(version.id)
    _mock_durable_launch(monkeypatch, payload)

    result = training.launch_training_run(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        version.project_id,
        payload,
        client,
        idempotency_key="launch-no-side-effect",
    )

    assert result.run.status == RunStatus.QUEUED
    client.create_job.assert_not_called()


def test_training_launch_rejects_failed_precheck(monkeypatch) -> None:
    estimate = _estimate().model_copy(
        update={"can_launch": False, "blockers": ["capacity exhausted"]}
    )
    monkeypatch.setattr(training, "_lock_training_admission", MagicMock())
    monkeypatch.setattr(training, "estimate_training_run", lambda *_args: estimate)
    with pytest.raises(HTTPException, match="precheck failed") as error:
        training.launch_training_run(
            _Session(),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            _launch_request(uuid.uuid4()),
            MagicMock(),
            idempotency_key="blocked",
        )
    assert error.value.status_code == 409


def test_training_validation_helpers_reject_incoherent_specs(monkeypatch) -> None:
    version = _version()
    training._validate_target(version, None)
    with pytest.raises(HTTPException, match="Target column"):
        training._validate_target(version, "missing")

    training._validate_evaluation_column(
        version,
        _request(
            task_type=TaskType.CLUSTERING,
            target_column=None,
            evaluation_column="cluster_label",
            primary_metric="silhouette",
        ),
    )
    with pytest.raises(HTTPException, match="only supported for clustering"):
        training._validate_evaluation_column(
            version,
            _request(evaluation_column="cluster_label"),
        )
    with pytest.raises(HTTPException, match="was not found"):
        training._validate_evaluation_column(
            version,
            _request(
                task_type=TaskType.CLUSTERING,
                target_column=None,
                evaluation_column="missing",
                primary_metric="silhouette",
            ),
        )

    monkeypatch.setattr(
        training,
        "estimator_catalog_payload",
        lambda _task: [{"name": "Ridge"}],
    )
    with pytest.raises(HTTPException, match="Unsupported estimators"):
        training._validate_candidate_models(
            _request(candidate_models=["Unknown"]),
        )


@pytest.mark.parametrize(
    ("updates", "code"),
    [
        ({"candidate_models": ["Ridge"]}, "invalid_catalog_selection"),
        ({"execution_mode_hint": "incremental"}, "invalid_catalog_selection"),
        ({"candidate_limit": 2}, "invalid_catalog_selection"),
        ({"deadline_seconds": 7100}, "qualification_deadline_required"),
        ({"optimization_iterations": 4}, "qualification_strength_required"),
        ({"cv_folds": 4}, "qualification_strength_required"),
    ],
)
def test_all_catalog_contract_rejects_client_weakening(updates, code: str) -> None:
    payload = TrainingEstimateRequest(
        dataset_version_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        catalog_mode="all",
        **updates,
    )
    with pytest.raises(HTTPException) as error:
        training._validate_catalog_selection(payload)
    assert error.value.detail["code"] == code


def test_all_catalog_contract_omitted_candidate_limit_expands_catalog() -> None:
    payload = TrainingEstimateRequest(
        dataset_version_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        catalog_mode="all",
    )
    training._validate_catalog_selection(payload)
    assert "candidate_limit" not in payload.model_fields_set


def test_experiment_spec_is_authoritative_and_compatibility_fields_are_assertions() -> None:
    spec = SimpleNamespace(
        id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION.value,
        target_column="target",
        primary_metric="rmse",
        catalog_revision_id=uuid.uuid4(),
    )
    project_id = uuid.uuid4()
    resolved = training._resolve_estimate_identity(
        _Session(scalar_values=[spec]),
        project_id,
        TrainingEstimateRequest(experiment_spec_revision_id=spec.id, catalog_mode="all"),
    )
    assert resolved.dataset_version_id == spec.dataset_version_id
    assert resolved.catalog_revision_id == spec.catalog_revision_id
    with pytest.raises(HTTPException) as error:
        training._resolve_estimate_identity(
            _Session(scalar_values=[spec]),
            project_id,
            TrainingEstimateRequest(
                experiment_spec_revision_id=spec.id,
                dataset_version_id=uuid.uuid4(),
                catalog_mode="all",
            ),
        )
    assert error.value.detail["code"] == "experiment_spec_mismatch"


def test_reservation_validation_is_atomic_and_digest_bound() -> None:
    project_id = uuid.uuid4()
    reservation = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=project_id,
        command_id=uuid.uuid4(),
        status="held",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    digest = "a" * 64
    command = SimpleNamespace(
        response_payload={
            "estimate_digest": digest,
            "capacity_profile_revision": "local-capacity-v1",
        }
    )
    request = _launch_request(uuid.uuid4()).model_copy(
        update={
            "capacity_reservation_id": reservation.id,
            "estimate_digest": digest,
            "capacity_profile_revision": "local-capacity-v1",
        }
    )
    estimate = _estimate().model_copy(update={"estimate_digest": digest})
    db = _Session(scalar_values=[reservation])
    db.get = lambda _model, _identifier: command
    assert training._validate_launch_reservation(db, project_id, request, estimate) is reservation
    request = request.model_copy(update={"estimate_digest": "b" * 64})
    with pytest.raises(HTTPException) as error:
        bad_db = _Session(scalar_values=[reservation])
        bad_db.get = lambda _model, _identifier: command
        training._validate_launch_reservation(bad_db, project_id, request, estimate)
    assert error.value.detail["code"] == "capacity_profile_changed"


def _coherent_revisions(payload: TrainingLaunchRequest) -> list[object]:
    return [
        SimpleNamespace(
            id=payload.split_revision_id,
            dataset_version_id=payload.dataset_version_id,
        ),
        SimpleNamespace(
            id=payload.feature_contract_revision_id,
            task_type=payload.task_type.value,
            target_column=payload.target_column,
        ),
        SimpleNamespace(id=payload.feature_registry_revision_id),
        SimpleNamespace(
            id=payload.feature_recipe_revision_id,
            registry_revision_id=payload.feature_registry_revision_id,
        ),
        SimpleNamespace(
            id=payload.feature_search_space_revision_id,
            metric_name=payload.primary_metric,
        ),
        SimpleNamespace(id=payload.estimator_catalog_revision_id),
    ]


@pytest.mark.parametrize(
    ("index", "message"),
    [
        (0, "selected split revision"),
        (1, "selected contract revision"),
        (2, "selected registry revision"),
        (3, "selected recipe revision"),
        (4, "selected search revision"),
        (5, "selected catalog revision"),
    ],
)
def test_launch_revision_resolution_rejects_each_missing_binding(index: int, message: str) -> None:
    payload = _launch_request(uuid.uuid4())
    revisions = _coherent_revisions(payload)
    revisions[index] = None
    with pytest.raises(HTTPException, match=message):
        training._resolve_launch_revisions(_Session(scalar_values=revisions), uuid.uuid4(), payload)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda rows: setattr(rows[0], "dataset_version_id", uuid.uuid4()), "split revision"),
        (lambda rows: setattr(rows[1], "task_type", "classification"), "feature contract"),
        (lambda rows: setattr(rows[1], "target_column", "other"), "feature contract"),
        (lambda rows: setattr(rows[3], "registry_revision_id", uuid.uuid4()), "recipe"),
        (lambda rows: setattr(rows[4], "metric_name", "mae"), "search-space"),
    ],
)
def test_launch_revision_resolution_rejects_incoherent_lineage(mutate, message: str) -> None:
    payload = _launch_request(uuid.uuid4())
    revisions = _coherent_revisions(payload)
    mutate(revisions)
    with pytest.raises(HTTPException, match=message):
        training._resolve_launch_revisions(_Session(scalar_values=revisions), uuid.uuid4(), payload)


def test_launch_revision_resolution_accepts_scope_and_rejects_scope_state() -> None:
    payload = _launch_request(uuid.uuid4()).model_copy(
        update={"promotional_scope_id": uuid.uuid4()}
    )
    revisions = _coherent_revisions(payload)
    scope = SimpleNamespace(
        id=payload.promotional_scope_id,
        split_revision_id=payload.split_revision_id,
        status=ScopeStatus.OPEN,
    )
    resolved = training._resolve_launch_revisions(
        _Session(scalar_values=[*revisions, scope]), uuid.uuid4(), payload
    )
    assert resolved["scope"] is scope
    with pytest.raises(HTTPException, match="missing or bound"):
        training._resolve_launch_revisions(
            _Session(scalar_values=[*revisions, None]), uuid.uuid4(), payload
        )
    scope.status = ScopeStatus.SEALED
    with pytest.raises(HTTPException, match="already sealed"):
        training._resolve_launch_revisions(
            _Session(scalar_values=[*_coherent_revisions(payload), scope]),
            uuid.uuid4(),
            payload,
        )


def test_estimate_identity_and_catalog_resolution_failure_paths() -> None:
    project_id = uuid.uuid4()
    with pytest.raises(HTTPException) as error:
        training._resolve_estimate_identity(
            _Session(), project_id, TrainingEstimateRequest(catalog_mode="all")
        )
    assert error.value.detail["code"] == "experiment_spec_required"
    missing_spec = TrainingEstimateRequest(
        experiment_spec_revision_id=uuid.uuid4(), catalog_mode="all"
    )
    with pytest.raises(HTTPException) as error:
        training._resolve_estimate_identity(_Session(), project_id, missing_spec)
    assert error.value.detail["code"] == "experiment_spec_changed"

    catalog_id = uuid.uuid4()
    current = SimpleNamespace(id=uuid.uuid4())
    request = _request(catalog_revision_id=catalog_id)
    with pytest.raises(HTTPException) as error:
        training._resolved_catalog_revision(
            _Session(scalar_values=[None, current]), project_id, request
        )
    assert error.value.detail["current_revision"] == str(current.id)
    request = _request(catalog_revision_id=None)
    active = SimpleNamespace(id=uuid.uuid4())
    assert (
        training._resolved_catalog_revision(_Session(scalar_values=[active]), project_id, request)
        is active
    )


def test_reservation_validation_rejects_required_expired_and_profile_mismatch() -> None:
    project_id = uuid.uuid4()
    required = _launch_request(uuid.uuid4()).model_copy(
        update={"catalog_mode": "all", "reserve_capacity": True, "candidate_limit": None}
    )
    with pytest.raises(HTTPException) as error:
        training._validate_launch_reservation(_Session(), project_id, required, _estimate())
    assert error.value.detail["code"] == "capacity_reservation_required"

    reservation = SimpleNamespace(
        id=uuid.uuid4(),
        command_id=uuid.uuid4(),
        status="held",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    supplied = _launch_request(uuid.uuid4()).model_copy(
        update={"capacity_reservation_id": reservation.id}
    )
    with pytest.raises(HTTPException) as error:
        training._validate_launch_reservation(
            _Session(scalar_values=[reservation]), project_id, supplied, _estimate()
        )
    assert error.value.detail["code"] == "capacity_reservation_expired"

    reservation.expires_at = datetime.now(UTC) + timedelta(minutes=1)
    supplied = supplied.model_copy(
        update={"estimate_digest": "a" * 64, "capacity_profile_revision": "wrong"}
    )
    estimate = _estimate().model_copy(update={"estimate_digest": "a" * 64})
    command = SimpleNamespace(
        response_payload={
            "estimate_digest": "a" * 64,
            "capacity_profile_revision": "local-capacity-v1",
        }
    )
    db = _Session(scalar_values=[reservation])
    db.get = lambda *_args: command
    with pytest.raises(HTTPException) as error:
        training._validate_launch_reservation(db, project_id, supplied, estimate)
    assert error.value.detail["code"] == "capacity_profile_changed"


def test_dataset_and_leakage_lookup_helpers() -> None:
    version = _version()
    assert (
        training._get_dataset_version(
            _Session(scalar_values=[version]), version.project_id, version.id
        )
        is version
    )
    with pytest.raises(HTTPException, match="Dataset version not found"):
        training._get_dataset_version(_Session(scalar_values=[None]), uuid.uuid4(), uuid.uuid4())

    assert training._latest_leakage_analysis(_Session(), version.id, None) == (None, {})
    profile = SimpleNamespace(overview_json={"leakage_analysis": {"excluded_columns": ["proxy"]}})
    assert training._latest_leakage_analysis(
        _Session(scalar_values=[profile]), version.id, "target"
    ) == (profile, {"excluded_columns": ["proxy"]})
    malformed = SimpleNamespace(overview_json={"leakage_analysis": "invalid"})
    assert training._latest_leakage_analysis(
        _Session(scalar_values=[malformed]), version.id, "target"
    ) == (malformed, {})


def test_training_queries_logs_and_telemetry_fail_softly(monkeypatch) -> None:
    run = _run()
    user = SimpleNamespace(id=uuid.uuid4())
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    assert (
        training.get_training_run(
            _Session(scalar_values=[run]), user, run.project_id, run.id, sync=False
        )
        is run
    )
    with pytest.raises(HTTPException, match="Training run not found"):
        training.get_training_run(
            _Session(scalar_values=[None]), user, run.project_id, run.id, sync=False
        )

    client = MagicMock()
    client.job_logs.return_value = ["starting", "complete"]
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: run)
    logs = training.training_logs(_Session(), user, run.project_id, run.id, client)
    assert logs.lines == ["starting", "complete"]

    client.job_logs.side_effect = ApiException(status=404)
    assert training.training_logs(_Session(), user, run.project_id, run.id, client).lines == []
    client.job_logs.side_effect = ApiException(status=500)
    with pytest.raises(ApiException):
        training.training_logs(_Session(), user, run.project_id, run.id, client)


def test_training_resources_tracks_peaks_progress_and_degraded_telemetry(monkeypatch) -> None:
    run = _run()
    run.tags["resource_usage"] = {
        "cpu_usage_cores": 0.5,
        "memory_usage_mb": 900,
        "peak_cpu_usage_cores": 1.5,
        "peak_memory_usage_mb": 1200,
    }
    db = _Session()
    client = MagicMock()
    client.training_resource_usage.return_value = {
        "telemetry_available": True,
        "pod_name": "trainer-1",
        "pod_phase": "Running",
        "cpu_usage_cores": 1.0,
        "memory_usage_mb": 1000,
        "restart_count": 1,
    }
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: run)

    usage = training.training_resources(db, SimpleNamespace(), run.project_id, run.id, client)

    assert usage.progress == pytest.approx(1 / 3)
    assert usage.estimated_remaining_seconds is not None
    assert usage.peak_cpu_usage_cores == 1.5
    assert usage.peak_memory_usage_mb == 1200
    assert usage.gpu_count == 1
    assert usage.telemetry_available is True
    assert db.refreshes == 1

    client.training_resource_usage.side_effect = ApiException(status=503, reason="metrics down")
    degraded = training.training_resources(db, SimpleNamespace(), run.project_id, run.id, client)
    assert degraded.telemetry_available is False
    assert "metrics down" in (degraded.status_reason or "")


def test_timestamp_parent_and_estimator_helpers(monkeypatch) -> None:
    assert training._timestamp_from_tag(None) is None
    assert training._timestamp_from_tag("not-a-date") is None
    naive = training._timestamp_from_tag("2026-08-12T12:00:00")
    assert naive is not None and naive.tzinfo is UTC
    aware = training._timestamp_from_tag("2026-08-12T12:00:00+02:00")
    assert aware is not None and aware.hour == 10

    run = _run()
    run.tags = {}
    assert training._leaderboard_parent(_Session(), run) is run
    run.tags = {"leaderboard_parent_run_id": "invalid"}
    assert training._leaderboard_parent(_Session(), run) is run
    parent = _run(RunStatus.SUCCEEDED)
    parent.project_id = run.project_id
    run.tags = {"leaderboard_parent_run_id": str(parent.id)}
    db = MagicMock()
    db.get.return_value = parent
    assert training._leaderboard_parent(db, run) is parent

    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    with pytest.raises(HTTPException, match="concrete task type"):
        training.list_training_estimators(
            _Session(), SimpleNamespace(), uuid.uuid4(), TaskType.UNSPECIFIED
        )


def test_training_admission_lock_is_postgres_only() -> None:
    db = _Session()
    training._lock_training_admission(db)
    assert db.executed == []

    db.bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    training._lock_training_admission(db)
    assert len(db.executed) == 1
    assert db.executed[0][1] == {"lock_id": 7_301_247_011}


def test_cancel_training_run_is_idempotent_and_tolerates_missing_job(monkeypatch) -> None:
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    user = SimpleNamespace(id=uuid.uuid4())

    terminal = _run(RunStatus.SUCCEEDED)
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: terminal)
    assert (
        training.cancel_training_run(
            _Session(), user, terminal.project_id, terminal.id, MagicMock()
        )
        is terminal
    )

    cancelled = _run(RunStatus.CANCELLED)
    cancelled.finished_at = None
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: cancelled)
    result = training.cancel_training_run(
        _Session(), user, cancelled.project_id, cancelled.id, MagicMock()
    )
    assert result.status == RunStatus.CANCELLED
    assert result.finished_at is not None
    assert result.tags["candidate_phase"] == "cancelled"

    active = _run(RunStatus.RUNNING)
    active.tags["leaderboard"] = [
        {"model": "Ridge", "status": "running"},
        {"model": "RandomForest", "status": "pending"},
    ]
    client = MagicMock()
    client.delete_job.side_effect = ApiException(status=404)
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: active)
    result = training.cancel_training_run(_Session(), user, active.project_id, active.id, client)
    assert result.status == RunStatus.CANCELLED
    assert result.tags["cancelled_candidate"] == "Ridge"
    assert result.tags["leaderboard"][0]["status"] == "cancelled"

    active = _run(RunStatus.RUNNING)
    client.delete_job.side_effect = ApiException(status=500)
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: active)
    with pytest.raises(ApiException):
        training.cancel_training_run(_Session(), user, active.project_id, active.id, client)


def test_restart_and_add_models_reject_invalid_source_states(monkeypatch) -> None:
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    user = SimpleNamespace(id=uuid.uuid4())
    source = _run(RunStatus.RUNNING)
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: source)
    with pytest.raises(HTTPException, match="can be restarted"):
        training.restart_training_run(_Session(), user, source.project_id, source.id)

    source.status = RunStatus.FAILED
    monkeypatch.setattr(training, "_get_dataset_version", lambda *_args: _version())
    monkeypatch.setattr(
        training, "get_object_store", lambda: SimpleNamespace(exists=lambda _uri: False)
    )
    with pytest.raises(HTTPException, match="dataset object is missing"):
        training.restart_training_run(_Session(), user, source.project_id, source.id)

    request = SimpleNamespace(candidate_models=["Ridge"])
    source.status = RunStatus.RUNNING
    with pytest.raises(HTTPException, match="only be added"):
        training.add_models_to_training_run(_Session(), user, source.project_id, source.id, request)


@pytest.mark.parametrize(
    ("failure_code", "expected"),
    [
        ("POD_OOM_KILLED", "memory limit"),
        ("JOB_DEADLINE_EXCEEDED", "runtime safety deadline"),
        ("POD_EVICTED", "evicted"),
        ("TRAINING_IMAGE_NOT_PRESENT", "not available"),
        ("TRAINING_IMAGE_PULL_FAILED", "could not download"),
        ("TRAINING_IMAGE_INVALID", "image name is invalid"),
        ("TRAINING_CONTAINER_CONFIG_INVALID", "assemble"),
        ("TRAINING_CONTAINER_START_FAILED", "start the training container"),
        ("UNCLASSIFIED", "failed or disappeared"),
    ],
)
def test_sync_run_status_maps_terminal_failure_codes(
    failure_code: str,
    expected: str,
) -> None:
    run = _run(RunStatus.RUNNING)
    client = MagicMock()
    client.job_state.return_value = "failed"
    client.job_failure_details.return_value = (failure_code, "details")

    training._sync_run_status(_Session(), run, client)

    assert run.status == RunStatus.FAILED
    assert run.failure_code == failure_code
    assert expected in run.plain_english_failure
    assert run.finished_at is not None


def test_sync_run_status_handles_success_missing_and_stale_observation() -> None:
    for state, expected in (
        ("running", RunStatus.RUNNING),
        ("succeeded", RunStatus.SUCCEEDED),
        ("missing", RunStatus.FAILED),
    ):
        run = _run(RunStatus.QUEUED)
        client = MagicMock()
        client.job_state.return_value = state
        training._sync_run_status(_Session(), run, client)
        assert run.status == expected
        if state == "missing":
            assert run.failure_code == "KUBERNETES_JOB_MISSING"

    stale = _run(RunStatus.CANCELLED)
    client = MagicMock()
    client.job_state.return_value = "succeeded"
    training._sync_run_status(_Session(), stale, client)
    assert stale.status == RunStatus.CANCELLED


def test_sync_run_status_applies_image_pull_grace_and_terminal_cleanup() -> None:
    run = _run(RunStatus.QUEUED)
    client = MagicMock()
    client.job_state.return_value = "image_pull_backoff"
    db = _Session()
    training._sync_run_status(db, run, client)
    assert training._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG in run.tags
    assert run.status == RunStatus.QUEUED

    run.tags[training._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG] = datetime.now(UTC).isoformat()
    training._sync_run_status(db, run, client)
    assert run.status == RunStatus.QUEUED

    run.tags[training._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG] = (
        datetime.now(UTC) - training._IMAGE_PULL_BACKOFF_GRACE - timedelta(seconds=1)
    ).isoformat()
    client.job_failure_details.return_value = ("TRAINING_IMAGE_PULL_FAILED", "denied")
    client.delete_job.side_effect = ApiException(status=404)
    training._sync_run_status(db, run, client)
    assert run.status == RunStatus.FAILED
    assert run.failure_code == "TRAINING_IMAGE_PULL_FAILED"

    run = _run(RunStatus.RUNNING)
    run.tags[training._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG] = datetime.now(UTC).isoformat()
    client = MagicMock()
    client.job_state.return_value = "running"
    training._sync_run_status(_Session(), run, client)
    assert training._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG not in run.tags


def test_reconcile_active_runs_skips_unsubmitted_and_stops_on_api_error(monkeypatch) -> None:
    unsubmitted = _run(RunStatus.QUEUED)
    unsubmitted.k8s_job_name = None
    submitted = _run(RunStatus.RUNNING)
    later = _run(RunStatus.RUNNING)
    db = _Session(list_values=[[unsubmitted, submitted, later]])
    sync = MagicMock(side_effect=ApiException(status=503))
    monkeypatch.setattr(training, "_sync_run_status", sync)

    training._reconcile_active_runs(db, MagicMock(), [RunStatus.QUEUED, RunStatus.RUNNING])

    sync.assert_called_once()


def test_add_models_validates_parent_request_and_catalog(monkeypatch) -> None:
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    user = SimpleNamespace(id=uuid.uuid4())
    selected = _run(RunStatus.SUCCEEDED)
    selected.tags = {"leaderboard": []}
    selected.params = {}
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: selected)

    incomplete_parent = _run(RunStatus.RUNNING)
    monkeypatch.setattr(training, "_leaderboard_parent", lambda *_args: incomplete_parent)
    request = TrainingAddModelsRequest(candidate_models=["Ridge"])
    with pytest.raises(HTTPException, match="original training run"):
        training.add_models_to_training_run(
            _Session(), user, selected.project_id, selected.id, request
        )

    parent = _run(RunStatus.SUCCEEDED)
    parent.tags = {"leaderboard": []}
    parent.params = {}
    monkeypatch.setattr(training, "_leaderboard_parent", lambda *_args: parent)
    duplicate = TrainingAddModelsRequest(candidate_models=["Ridge", "Ridge"])
    with pytest.raises(HTTPException, match="selected only once"):
        training.add_models_to_training_run(
            _Session(), user, selected.project_id, selected.id, duplicate
        )

    monkeypatch.setattr(training, "candidate_catalog", lambda _task: [])
    with pytest.raises(HTTPException, match="Unsupported estimators"):
        training.add_models_to_training_run(
            _Session(), user, selected.project_id, selected.id, request
        )

    monkeypatch.setattr(
        training,
        "candidate_catalog",
        lambda _task: [SimpleNamespace(name="Ridge")],
    )
    parent.tags = {"leaderboard": [{"model": "Ridge", "status": "succeeded"}]}
    with pytest.raises(HTTPException, match="already completed"):
        training.add_models_to_training_run(
            _Session(), user, selected.project_id, selected.id, request
        )


def test_get_run_syncs_active_job_and_logs_without_job(monkeypatch) -> None:
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    run = _run(RunStatus.QUEUED)
    sync = MagicMock()
    monkeypatch.setattr(training, "_sync_run_status", sync)
    client = MagicMock()
    assert (
        training.get_training_run(
            _Session(scalar_values=[run]),
            SimpleNamespace(),
            run.project_id,
            run.id,
            client=client,
        )
        is run
    )
    sync.assert_called_once()

    run.k8s_job_name = None
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: run)
    result = training.training_logs(_Session(), SimpleNamespace(), run.project_id, run.id, client)
    assert result.lines == []
    client.job_logs.assert_not_called()


def test_resource_usage_propagates_unexpected_api_error(monkeypatch) -> None:
    run = _run()
    client = MagicMock()
    client.training_resource_usage.side_effect = ApiException(status=500)
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: run)
    with pytest.raises(ApiException):
        training.training_resources(_Session(), SimpleNamespace(), run.project_id, run.id, client)


def test_leaderboard_combines_parent_extension_and_cancelled_entries(monkeypatch) -> None:
    parent = _run(RunStatus.SUCCEEDED)
    parent.tags = {
        "leaderboard_primary_metric": "rmse",
        "leaderboard": [
            {
                "model": "Ridge",
                "status": "succeeded",
                "metrics": {"rmse": 1.5},
                "duration_seconds": 1.0,
                "error": None,
            },
            {
                "model": "NoMetric",
                "status": "succeeded",
                "metrics": {},
                "duration_seconds": 1.0,
                "error": None,
            },
        ],
    }
    parent.params = {"excluded_leakage_columns": ["proxy"]}
    run = _run(RunStatus.CANCELLED)
    run.tags = {
        "leaderboard_parent_run_id": str(parent.id),
        "current_candidate": "RandomForest",
        "candidate_phase": "fitting",
        "leaderboard": [
            {
                "model": "RandomForest",
                "status": "running",
                "metrics": {},
                "duration_seconds": None,
                "error": None,
            },
        ],
    }
    run.params = {"candidate_models": ["Ridge", "RandomForest"], "candidate_limit": 2}
    run.finished_at = datetime.now(UTC)
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: run)
    monkeypatch.setattr(training, "_leaderboard_parent", lambda *_args: parent)
    monkeypatch.setattr(
        training,
        "select_candidates",
        lambda *_args: [
            SimpleNamespace(name="Ridge", cost_tier="low"),
            SimpleNamespace(name="RandomForest", cost_tier="medium"),
        ],
    )
    result = training.training_leaderboard(_Session(), SimpleNamespace(), run.project_id, run.id)

    assert result.winner == "Ridge"
    assert result.entries[0].rank == 1
    cancelled = next(entry for entry in result.entries if entry.model == "RandomForest")
    assert cancelled.status == "cancelled"
    assert cancelled.pipeline.model_name == "RandomForest"


def test_rank_leaderboard_supports_minimize_unranked_and_pending() -> None:
    entries = [
        {"model": "worse", "status": "succeeded", "metrics": {"rmse": 2.0}},
        {"model": "best", "status": "succeeded", "metrics": {"rmse": 1.0}},
        {"model": "unranked", "status": "succeeded", "metrics": {}},
        {"model": "pending", "status": "pending", "metrics": {"rmse": 0.1}},
    ]
    ranked = training._rank_combined_leaderboard(entries, "rmse")
    assert [entry["model"] for entry in ranked] == [
        "best",
        "worse",
        "unranked",
        "pending",
    ]
    assert ranked[0]["rank"] == 1
    assert ranked[-1]["primary_score"] is None


def test_parent_validation_and_noop_reconcile(monkeypatch) -> None:
    run = _run()
    parent = _run(RunStatus.SUCCEEDED)
    run.tags = {"leaderboard_parent_run_id": str(parent.id)}
    db = MagicMock()
    db.get.return_value = None
    assert training._leaderboard_parent(db, run) is run
    parent.project_id = uuid.uuid4()
    db.get.return_value = parent
    assert training._leaderboard_parent(db, run) is run

    submitted = _run(RunStatus.RUNNING)
    db = _Session(list_values=[[submitted]])
    sync = MagicMock()
    monkeypatch.setattr(training, "_sync_run_status", sync)
    training._reconcile_active_runs(db, MagicMock(), [RunStatus.RUNNING])
    sync.assert_called_once()


def test_remaining_training_noop_and_nonterminal_branches(monkeypatch) -> None:
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    user = SimpleNamespace(id=uuid.uuid4())

    active = _run(RunStatus.RUNNING)
    active.k8s_job_name = None
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: active)
    result = training.cancel_training_run(
        _Session(), user, active.project_id, active.id, MagicMock()
    )
    assert result.status == RunStatus.CANCELLED

    concrete = training.list_training_estimators(
        _Session(), user, uuid.uuid4(), TaskType.REGRESSION
    )
    assert concrete
    training._validate_candidate_models(_request(candidate_models=[]))

    profile, result = training._latest_leakage_analysis(
        _Session(scalar_values=[None]), uuid.uuid4(), "target"
    )
    assert profile is None and result == {}

    run = _run(RunStatus.RUNNING)
    client = MagicMock()
    client.job_state.return_value = "unknown"
    training._sync_run_status(_Session(), run, client)
    assert run.status == RunStatus.RUNNING


def test_standalone_leaderboard_without_metric_remains_unranked(monkeypatch) -> None:
    run = _run(RunStatus.RUNNING)
    run.tags = {"leaderboard": [], "current_candidate": "Ridge"}
    run.params = {"candidate_models": ["Ridge"], "candidate_limit": 1}
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: run)
    monkeypatch.setattr(
        training,
        "select_candidates",
        lambda *_args: [SimpleNamespace(name="Ridge", cost_tier="low")],
    )
    result = training.training_leaderboard(_Session(), SimpleNamespace(), run.project_id, run.id)
    assert result.primary_metric is None
    assert result.entries[0].status == "running"


def _successful_submission_client() -> MagicMock:
    client = MagicMock()
    client.settings = SimpleNamespace(training_namespace="sceptre")
    client.build_job_manifest.side_effect = lambda **kwargs: {
        "metadata": {"name": f"training-{kwargs['run_id']}"}
    }
    return client


def test_restart_legacy_run_requires_immutable_revision_bindings(monkeypatch) -> None:
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    source = _run(RunStatus.FAILED)
    source.params.update(
        {
            "candidate_models": ["Ridge"],
            "primary_metric": "rmse",
            "prefer_gpu": False,
        }
    )
    source.tags = {}
    source.run_name = "source"
    source.gpu_requested = False
    source.dataset_version_id = uuid.uuid4()
    version = _version()
    version.id = source.dataset_version_id
    version.project_id = source.project_id
    db = _Session()
    client = _successful_submission_client()
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: source)
    monkeypatch.setattr(training, "_get_dataset_version", lambda *_args: version)
    monkeypatch.setattr(
        training, "get_object_store", lambda: SimpleNamespace(exists=lambda _uri: True)
    )
    monkeypatch.setattr(training, "_lock_training_admission", lambda *_args: None)
    monkeypatch.setattr(training, "estimate_training_run", lambda *_args: _estimate())
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_args: (None, {}))

    with pytest.raises(HTTPException, match="predates immutable revision bindings") as error:
        training.restart_training_run(
            db, SimpleNamespace(id=uuid.uuid4()), source.project_id, source.id, client
        )
    assert error.value.status_code == 409


def test_add_models_to_legacy_run_requires_immutable_revision_bindings(monkeypatch) -> None:
    monkeypatch.setattr(training, "require_project_role", lambda *_args: None)
    parent = _run(RunStatus.SUCCEEDED)
    parent.params = {"primary_metric": "rmse", "excluded_leakage_columns": []}
    parent.tags = {"leaderboard": [], "extension_run_ids": []}
    parent.run_name = "source"
    db = _Session()
    client = _successful_submission_client()
    monkeypatch.setattr(training, "get_training_run", lambda *_args, **_kwargs: parent)
    monkeypatch.setattr(training, "_leaderboard_parent", lambda *_args: parent)
    monkeypatch.setattr(training, "_lock_training_admission", lambda *_args: None)
    monkeypatch.setattr(training, "estimate_training_run", lambda *_args: _estimate())
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_args: (None, {}))

    request = TrainingAddModelsRequest(candidate_models=["Ridge"])
    with pytest.raises(HTTPException, match="predates immutable revision bindings") as error:
        training.add_models_to_training_run(
            db, SimpleNamespace(id=uuid.uuid4()), parent.project_id, parent.id, request, client
        )
    assert error.value.status_code == 409
