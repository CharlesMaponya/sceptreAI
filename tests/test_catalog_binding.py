"""Persisted catalog identity, task isolation and pre-execution drift rejection."""

import copy
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.api.routes import contracts as routes
from automl_api.models.enums import ProjectRole, TaskType
from automl_api.models.projects import ProjectMembership
from automl_api.models.workflows import EstimatorCatalogRevision
from automl_api.services import training
from automl_api.training import catalog_contract as catalog
from automl_api.training import pipeline
from fastapi import HTTPException
from test_phase1_postgres_contracts import phase1_db, tenant  # noqa: F401

TASKS = [TaskType.CLASSIFICATION, TaskType.REGRESSION, TaskType.TIME_SERIES, TaskType.CLUSTERING]


def revision(task=TaskType.REGRESSION, **updates):
    spec = catalog.catalog_specification(task)
    values = dict(id=uuid.uuid4(), specification=spec, content_digest=catalog.catalog_digest(spec))
    values.update(updates)
    return SimpleNamespace(**values)


@pytest.mark.parametrize("task", TASKS)
def test_runtime_catalog_definition_is_stable_and_json_safe(task):
    spec = catalog.catalog_specification(task)
    assert spec == catalog.catalog_specification(task)
    assert len(spec["estimators"]) > 0
    assert len({r["name"] for r in spec["estimators"]}) == len(spec["estimators"])
    assert catalog.validate_catalog_revision(revision(task), task) == spec
    assert spec["task_type"] == task.value
    assert len(spec["runtime_lock_digest"]) == 64
    assert all(
        r["serializable"] is False and r["sampling_policy"] == "unqualified"
        for r in spec["estimators"]
    )


@pytest.mark.parametrize("change", ["digest", "task", "runtime", "metadata", "legacy"])
def test_catalog_rejects_modified_or_legacy_definition(change):
    row = revision()
    row.specification = copy.deepcopy(row.specification)
    if change == "digest":
        row.content_digest = "0" * 64
    if change == "task":
        row.specification["task_type"] = "classification"
    if change == "runtime":
        row.specification["runtime_lock_digest"] = "f" * 64
    if change == "metadata":
        row.specification["estimators"][0]["resource_class"] = "invented"
    if change == "legacy":
        row.specification = {"release": "old", "task_type": "regression"}
    if change != "digest":
        row.content_digest = catalog.catalog_digest(row.specification)
    with pytest.raises(ValueError):
        catalog.validate_catalog_revision(row, TaskType.REGRESSION)


@pytest.mark.parametrize("task", TASKS)
def test_catalog_api_reads_task_scoped_stored_revision(phase1_db, tenant, task):  # noqa: F811
    user, project = tenant
    phase1_db.add(
        ProjectMembership(
            project_id=project.id,
            user_id=user.id,
            role=ProjectRole.OWNER,
            accepted_at=datetime.now(UTC),
        )
    )
    other = TaskType.CLASSIFICATION if task != TaskType.CLASSIFICATION else TaskType.REGRESSION
    rows = []
    for index, t in enumerate((task, task, other)):
        spec = catalog.catalog_specification(t)
        row = EstimatorCatalogRevision(
            project_id=project.id,
            name=f"catalog-{index}",
            revision=1,
            release_version="test",
            digest_scope="catalog-test",
            specification=spec,
            content_digest=catalog.catalog_digest(spec),
            created_at=datetime.now(UTC) + timedelta(seconds=index),
        )
        phase1_db.add(row)
        rows.append(row)
    phase1_db.flush()
    latest = routes.catalog(project.id, task, phase1_db, user)
    assert latest.catalog_revision_id == rows[1].id
    assert latest.estimators[0].model_dump() == rows[1].specification["estimators"][0]
    assert not latest.signature_verified
    pinned = routes.catalog(project.id, task, phase1_db, user, rows[0].id)
    assert pinned.catalog_revision_id == rows[0].id
    payload = training.TrainingEstimateRequest(task_type=task, catalog_revision_id=rows[0].id)
    assert training._resolved_catalog_revision(phase1_db, project.id, payload).id == rows[0].id
    with pytest.raises(HTTPException) as error:
        routes.catalog(project.id, task, phase1_db, user, rows[2].id)
    assert error.value.status_code == 409
    assert error.value.detail["current_revision"] == str(rows[1].id)
    rows[0].specification = {**rows[0].specification, "runtime_lock_digest": "e" * 64}
    phase1_db.flush()
    with pytest.raises(HTTPException):
        routes.catalog(project.id, task, phase1_db, user, rows[0].id)


def test_catalog_api_never_fabricates_unpublished_revision(monkeypatch):
    monkeypatch.setattr(routes, "require_project_role", lambda *_: None)
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(HTTPException) as error:
        routes.catalog(uuid.uuid4(), TaskType.REGRESSION, db, SimpleNamespace())
    assert error.value.status_code == 404


@pytest.mark.parametrize(
    "change", [None, "missing", "foreign", "digest", "runtime", "selection", "duplicate", "count"]
)
def test_worker_checks_catalog_identity_before_execution(change):
    row = revision()
    spec = row.specification
    names = [spec["estimators"][0]["name"]]
    run = SimpleNamespace(
        project_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        params={
            "estimator_catalog_revision_id": str(row.id),
            "catalog_content_digest": row.content_digest,
            "catalog_runtime_digest": spec["runtime_lock_digest"],
            "candidate_models": names,
            "candidate_limit": 1,
        },
    )
    if change == "digest":
        run.params["catalog_content_digest"] = "0" * 64
    if change == "runtime":
        run.params["catalog_runtime_digest"] = "0" * 64
    if change == "selection":
        run.params["candidate_models"] = ["Unknown"]
    if change == "duplicate":
        run.params["candidate_models"] *= 2
    if change == "count":
        run.params["candidate_limit"] = 2
    db = MagicMock()
    db.scalar.return_value = None if change in {"missing", "foreign"} else row
    if change is None:
        pipeline._validate_bound_catalog(db, run)
    else:
        with pytest.raises(ValueError):
            pipeline._validate_bound_catalog(db, run)
    statement = db.scalar.call_args.args[0]
    assert run.project_id in statement.compile().params.values()
    db.add.assert_not_called()
    db.commit.assert_not_called()


def test_fenced_worker_cannot_skip_missing_catalog_pointer(monkeypatch):
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(uuid.uuid4()))
    with pytest.raises(ValueError, match="bound catalog"):
        pipeline._validate_bound_catalog(MagicMock(), SimpleNamespace(params={}))


@pytest.mark.parametrize("change", ["package", "default", "dimension"])
def test_runtime_drift_invalidates_an_unchanged_catalog(change, monkeypatch):
    from dataclasses import replace

    from sklearn.base import clone

    row = revision()
    if change == "package":
        changed = copy.deepcopy(catalog._RUNTIME_DEPENDENCIES)
        changed["packages"].append(("changed-runtime-package", "1"))
        monkeypatch.setattr(catalog, "_RUNTIME_DEPENDENCIES", changed)
    else:
        original = catalog.model_catalog.candidate_catalog
        entries = list(original(TaskType.REGRESSION))
        index = next(i for i, c in enumerate(entries) if c.name == "Ridge")
        candidate = entries[index]
        if change == "default":
            estimator = clone(candidate.estimator).set_params(alpha=2.0)
            entries[index] = replace(candidate, estimator=estimator)
        else:
            space = copy.deepcopy(candidate.search_space)
            next(iter(space.values())).low *= 2
            entries[index] = replace(candidate, search_space=space)
        monkeypatch.setattr(catalog.model_catalog, "candidate_catalog", lambda _: tuple(entries))
    with pytest.raises(ValueError, match="runtime differs"):
        catalog.validate_catalog_revision(row, TaskType.REGRESSION)


@pytest.mark.parametrize("valid", [False, True])
def test_actual_training_startup_validates_before_data_or_tracking(monkeypatch, valid):
    from automl_api.models.enums import RunStatus

    row = revision()
    run = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        status=RunStatus.QUEUED,
        tags={},
        started_at=None,
        params={
            "estimator_catalog_revision_id": str(row.id),
            "catalog_content_digest": row.content_digest,
            "catalog_runtime_digest": row.specification["runtime_lock_digest"],
            "candidate_models": [row.specification["estimators"][0]["name"]],
            "candidate_limit": 1,
        },
    )
    db = MagicMock()
    db.__enter__.return_value = db
    db.scalar.return_value = row if valid else None
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: db)
    monkeypatch.setattr(pipeline, "_locked_run", lambda *_: run)
    monkeypatch.setattr(pipeline, "_bound_split_revision", lambda *_: SimpleNamespace())
    data = MagicMock(side_effect=RuntimeError("stop after catalog admission"))
    monkeypatch.setattr(pipeline, "_load_prepared_training_frames", data)
    tracking = MagicMock()
    monkeypatch.setattr(pipeline, "_tracked_mlflow_run", tracking)
    monkeypatch.setattr(pipeline, "_mark_failed", MagicMock())
    if valid:
        with pytest.raises(RuntimeError, match="after catalog admission"):
            pipeline.execute_training_run(run.id)
        data.assert_called_once()
        db.commit.assert_called_once()
    else:
        with pytest.raises(ValueError, match="catalog revision is unavailable"):
            pipeline.execute_training_run(run.id)
        data.assert_not_called()
        db.get.assert_not_called()
        db.commit.assert_not_called()
    tracking.assert_not_called()


def test_catalog_project_isolation_is_enforced_by_postgres(phase1_db, tenant):  # noqa: F811
    from automl_api.models.projects import Project

    user, project = tenant
    foreign = Project(owner_id=user.id, created_by_id=user.id, name="other-catalog-project")
    phase1_db.add(foreign)
    phase1_db.flush()
    spec = catalog.catalog_specification(TaskType.REGRESSION)
    row = EstimatorCatalogRevision(
        project_id=foreign.id,
        name="foreign",
        revision=1,
        release_version="test",
        digest_scope="test",
        specification=spec,
        content_digest=catalog.catalog_digest(spec),
    )
    phase1_db.add(row)
    phase1_db.flush()
    with pytest.raises(HTTPException) as error:
        training._resolved_catalog_revision(
            phase1_db,
            project.id,
            training.TrainingEstimateRequest(
                task_type=TaskType.REGRESSION, catalog_revision_id=row.id
            ),
        )
    assert error.value.detail["current_revision"] is None
    run = SimpleNamespace(
        project_id=project.id,
        task_type=TaskType.REGRESSION,
        params={"estimator_catalog_revision_id": str(row.id)},
    )
    with pytest.raises(ValueError, match="unavailable"):
        pipeline._validate_bound_catalog(phase1_db, run)


def test_admission_rejects_stale_content_but_public_read_keeps_revision_identity(phase1_db, tenant):  # noqa: F811
    user, project = tenant
    phase1_db.add(
        ProjectMembership(
            project_id=project.id,
            user_id=user.id,
            role=ProjectRole.OWNER,
            accepted_at=datetime.now(UTC),
        )
    )
    rows = []
    for index in range(2):
        spec = {**catalog.catalog_specification(TaskType.REGRESSION), "release": f"release-{index}"}
        row = EstimatorCatalogRevision(
            project_id=project.id,
            name=f"release-{index}",
            revision=1,
            release_version=f"v{index}",
            digest_scope="test",
            specification=spec,
            content_digest=catalog.catalog_digest(spec),
            created_at=datetime.now(UTC) + timedelta(seconds=index),
        )
        phase1_db.add(row)
        rows.append(row)
    phase1_db.flush()
    assert (
        routes.catalog(
            project.id, TaskType.REGRESSION, phase1_db, user, rows[0].id
        ).catalog_revision
        == rows[0].content_digest
    )
    with pytest.raises(HTTPException) as error:
        training._resolved_catalog_revision(
            phase1_db,
            project.id,
            training.TrainingEstimateRequest(
                task_type=TaskType.REGRESSION, catalog_revision_id=rows[0].id
            ),
        )
    assert error.value.detail["code"] == "catalog_revision_changed"
    assert error.value.detail["requested_revision"] == str(rows[0].id)
    assert error.value.detail["current_revision"] == str(rows[1].id)


def test_catalog_identity_matches_an_independent_worker_process():
    import json
    import os
    import subprocess
    import sys

    expected = {
        task.value: catalog.catalog_digest(catalog.catalog_specification(task)) for task in TASKS
    }
    code = """import json
from automl_api.training.catalog_contract import catalog_digest,catalog_specification
from automl_api.models.enums import TaskType
tasks=['classification','regression','time_series','clustering']
print(json.dumps({task:catalog_digest(catalog_specification(TaskType(task))) for task in tasks}))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "PYTHONPATH": "apps/api:packages"},
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert json.loads(result.stdout) == expected


@pytest.mark.parametrize(
    "kind,protocol",
    [
        ("training", None),
        ("training", "analysis-v1"),
        ("validation", "training-catalog-v1"),
        ("deployment", "analysis-v1"),
        ("training", "training-catalog-v1"),
    ],
)
def test_worker_protocol_matches_durable_kind_before_admission(monkeypatch, kind, protocol):
    import sys

    from automl_api.models.enums import RunKind
    from automl_api.services.workflow_state import StaleFence
    from automl_api.training import worker

    run_id = uuid.uuid4()
    db = MagicMock()
    db.__enter__.return_value = db
    db.get.return_value = SimpleNamespace(run_kind=RunKind(kind))
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: db)
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(worker, "_fenced_attempt_context", lambda: (uuid.uuid4(), "fence"))
    begin = MagicMock(side_effect=RuntimeError("admission boundary"))
    monkeypatch.setattr(worker, "_begin_fenced_attempt", begin)
    argv = ["worker", "--run-id", str(run_id)]
    if protocol:
        argv += ["--attempt-protocol", protocol]
    monkeypatch.setattr(sys, "argv", argv)
    if kind == "training" and protocol == "training-catalog-v1":
        with pytest.raises(RuntimeError, match="admission boundary"):
            worker.main()
        begin.assert_called_once()
    else:
        with pytest.raises(StaleFence, match="protocol"):
            worker.main()
        begin.assert_not_called()
    db.commit.assert_not_called()


def test_ray_training_command_declares_catalog_protocol(monkeypatch):
    import shlex

    from automl_api.core.config import Settings
    from automl_api.services import reconciler
    from test_phase1_reconciler import _attempt, _run

    monkeypatch.setattr(reconciler, "get_settings", lambda: Settings())
    run = _run()
    manifest = reconciler.build_ray_job_manifest(run, _attempt(run), name="bound-catalog-worker")
    assert shlex.split(manifest["spec"]["entrypoint"]) == [
        "python",
        "-m",
        "automl_api.training.worker",
        "--run-id",
        str(run.id),
        "--attempt-protocol",
        "training-catalog-v1",
    ]
