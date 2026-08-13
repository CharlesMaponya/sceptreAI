from __future__ import annotations

import uuid
from concurrent.futures import Future
from contextlib import AbstractContextManager
from types import SimpleNamespace
from unittest.mock import MagicMock

import automl_api.services.profiling_jobs as jobs
import pytest
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import DatasetFormat, DatasetStatus, TaskType
from automl_api.models.iam import User
from automl_api.schemas.profiling import (
    ColumnProfileRead,
    DatasetProfileRead,
    LeakageAnalysisRead,
    TaskInferenceRead,
)
from automl_api.schemas.profiling_jobs import ProfilingJobCreate
from fastapi import HTTPException


class _Session(AbstractContextManager):
    def __init__(self, job: object, user: object, version: object) -> None:
        self.job = job
        self.user = user
        self.version = version
        self.commits = 0

    def __enter__(self) -> _Session:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def get(self, model: object, _identifier: object) -> object | None:
        return {
            ProfilingJob: self.job,
            User: self.user,
            DatasetVersion: self.version,
        }.get(model)

    def scalar(self, _statement: object) -> object | None:
        return getattr(self.job, "status", None)

    def refresh(self, _instance: object) -> None:
        return None

    def commit(self) -> None:
        self.commits += 1


def _profile(job: object) -> DatasetProfileRead:
    column = ColumnProfileRead(
        name="amount",
        semantic_type="numerical_continuous",
        missing_count=0,
        missing_ratio=0,
        distinct_count=3,
        sample_values=["1", "2", "3"],
        statistics={"min": 1, "max": 3},
        distribution_type="histogram",
        distribution=[],
        quality_flags=[],
    )
    return DatasetProfileRead(
        project_id=job.project_id,
        dataset_id=job.dataset_id,
        dataset_version_id=job.dataset_version_id,
        row_count_analyzed=3,
        column_count=1,
        target_column=None,
        task_inference=TaskInferenceRead(
            task_type=TaskType.CLUSTERING,
            confidence=0.8,
            rationale="No target",
        ),
        columns=[column],
        relationships=[],
        preparation_plan=[],
        leakage_analysis=LeakageAnalysisRead(status="not_applicable"),
        warnings=["fixture"],
    )


def _job() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        created_by_id=uuid.uuid4(),
        target_column=None,
        status="running",
        current_stage="features",
        progress=0.1,
        total_columns=0,
        completed_columns=0,
        row_count=None,
        overview_json={
            "stages": {
                "features": "running",
                "relationships": "queued",
                "preparation": "queued",
            }
        },
        feature_profiles_json={},
        relationships_json=[],
        preparation_json=[],
        warnings_json=[],
        artifact_uris_json={},
        failure_message=None,
        started_at=None,
        finished_at=None,
        heartbeat_at=None,
    )


def test_monolithic_profile_job_persists_each_stage_and_marks_dataset_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _job()
    version = SimpleNamespace(status=DatasetStatus.PROFILING, profile_artifact_uri=None)
    session = _Session(job, SimpleNamespace(id=job.created_by_id), version)
    profile = _profile(job)
    stored_stages: list[str] = []

    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(jobs, "build_dataset_profile", lambda *_args, **_kwargs: profile)

    def store_stage(current_job: object, stage: str, _payload: object) -> dict[str, str]:
        stored_stages.append(stage)
        return {**current_job.artifact_uris_json, stage: f"memory://{stage}.json"}

    monkeypatch.setattr(jobs, "_store_stage_artifact", store_stage)

    jobs._run_monolithic_fallback(job.id)

    assert stored_stages == ["features", "relationships", "preparation", "complete"]
    assert job.status == "succeeded"
    assert job.current_stage == "complete"
    assert job.progress == 1.0
    assert job.feature_profiles_json["amount"]["distinct_count"] == 3
    assert job.warnings_json == ["fixture"]
    assert version.status == DatasetStatus.READY
    assert version.profile_artifact_uri == "memory://complete.json"
    assert session.commits == 1


def test_profile_job_stage_helpers_preserve_existing_state() -> None:
    job = _job()
    job.overview_json = {"other": "value", "stages": {"features": "queued"}}

    jobs._set_stage(job, "features", "completed")

    assert job.overview_json == {
        "other": "value",
        "stages": {"features": "completed"},
    }
    version = SimpleNamespace(
        schema_json={"columns": [{"name": "a"}, {"name": ""}, {}]},
    )
    assert jobs._version_columns(version) == ["a"]


@pytest.mark.parametrize(
    ("dataset_format", "filename", "expected"),
    [
        (DatasetFormat.CSV, "records.csv", True),
        (DatasetFormat.JSON, "records.jsonl", True),
        (DatasetFormat.JSON, "records.ndjson", True),
        (DatasetFormat.JSON, "records.json", False),
        (DatasetFormat.PARQUET, "records.parquet", False),
    ],
)
def test_partitioned_stage_support_is_explicit(
    dataset_format: DatasetFormat,
    filename: str,
    expected: bool,
) -> None:
    version = SimpleNamespace(format=dataset_format, original_filename=filename)
    assert jobs._supports_partitioned_stages(version) is expected


def test_store_stage_artifact_uses_attempt_scoped_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _job()
    store = MagicMock()
    store.put_bytes.return_value = SimpleNamespace(uri="s3://bucket/profile.json")
    monkeypatch.setattr(jobs, "get_object_store", lambda: store)

    result = jobs._store_stage_artifact(job, "features", {"count": 3})

    key, content = store.put_bytes.call_args.args
    assert key.endswith(f"profiles/{job.id}/features.json")
    assert content == b'{"count":3}'
    assert result == {"features": "s3://bucket/profile.json"}


def test_get_version_rejects_unknown_dataset_version() -> None:
    db = MagicMock()
    db.scalar.return_value = None

    with pytest.raises(HTTPException) as error:
        jobs._get_version(db, uuid.uuid4(), uuid.uuid4(), uuid.uuid4())

    assert error.value.status_code == 404


def test_job_finished_releases_schedule_slot_and_swallows_worker_error() -> None:
    job_id = uuid.uuid4()
    future: Future[None] = Future()
    future.set_exception(RuntimeError("persisted worker failure"))
    jobs._scheduled_jobs.add(job_id)

    jobs._job_finished(job_id, future)

    assert job_id not in jobs._scheduled_jobs


class _ListResult:
    def __init__(self, values: list[object]) -> None:
        self.values = values

    def all(self) -> list[object]:
        return self.values


class _CreateSession(_Session):
    def __init__(
        self,
        version: object,
        scalar_values: list[object | None],
        active_jobs: list[object] | None = None,
    ) -> None:
        super().__init__(SimpleNamespace(), SimpleNamespace(), version)
        self.scalar_values = scalar_values
        self.active_jobs = active_jobs or []
        self.added: list[object] = []

    def scalar(self, _statement: object) -> object | None:
        return self.scalar_values.pop(0) if self.scalar_values else None

    def scalars(self, _statement: object) -> _ListResult:
        return _ListResult(self.active_jobs)

    def add(self, instance: object) -> None:
        self.added.append(instance)

    def flush(self) -> None:
        for instance in self.added:
            if getattr(instance, "id", None) is None:
                instance.id = uuid.uuid4()


def _version() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_id=uuid.uuid4(),
        row_count=50,
        format=DatasetFormat.CSV,
        original_filename="records.csv",
        schema_json={"columns": [{"name": "amount"}, {"name": "segment"}]},
        status=DatasetStatus.READY,
        profile_artifact_uri=None,
    )


def test_create_profile_job_reuses_matching_current_algorithm(monkeypatch) -> None:
    version = _version()
    latest = _job()
    latest.target_column = "amount"
    latest.status = "succeeded"
    latest.overview_json = {"profile_algorithm_version": jobs.PROFILE_ALGORITHM_VERSION}
    db = _CreateSession(version, [version, latest])
    monkeypatch.setattr(jobs, "require_project_role", lambda *_args: None)

    returned, created = jobs.create_profiling_job(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        version.project_id,
        version.dataset_id,
        version.id,
        ProfilingJobCreate(target_column=" amount "),
    )

    assert returned is latest
    assert created is False
    assert db.added == []


def test_create_profile_job_cancels_active_and_reuses_completed_features(monkeypatch) -> None:
    version = _version()
    latest = _job()
    latest.target_column = None
    latest.status = "running"
    latest.overview_json = {"profile_algorithm_version": jobs.PROFILE_ALGORITHM_VERSION - 1}
    active = _job()
    active.status = "running"
    reusable = _job()
    reusable.completed_columns = 2
    reusable.total_columns = 2
    reusable.row_count = 45
    reusable.feature_profiles_json = {
        "amount": _profile(reusable).columns[0].model_dump(mode="json"),
        "segment": {
            **_profile(reusable).columns[0].model_dump(mode="json"),
            "name": "segment",
            "semantic_type": "categorical",
        },
    }
    reusable.artifact_uris_json = {"features": "s3://profiles/features.json"}
    db = _CreateSession(version, [version, latest, reusable], [active])
    monkeypatch.setattr(jobs, "require_project_role", lambda *_args: None)

    created_job, created = jobs.create_profiling_job(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        version.project_id,
        version.dataset_id,
        version.id,
        ProfilingJobCreate(force=True),
        auto_started=True,
    )

    assert created is True
    assert active.status == "cancelled"
    assert active.finished_at is not None
    assert created_job.progress == 0.7
    assert created_job.completed_columns == 2
    assert created_job.auto_started is True
    assert created_job.overview_json["stages"]["features"] == "reused"
    assert created_job.overview_json["task_inference"]["task_type"] == "clustering"
    assert created_job.artifact_uris_json == {"features": "s3://profiles/features.json"}


def test_create_profile_job_rejects_unknown_target(monkeypatch) -> None:
    version = _version()
    monkeypatch.setattr(jobs, "require_project_role", lambda *_args: None)
    with pytest.raises(HTTPException, match="not found") as error:
        jobs.create_profiling_job(
            _CreateSession(version, [version]),
            SimpleNamespace(id=uuid.uuid4()),
            version.project_id,
            version.dataset_id,
            version.id,
            ProfilingJobCreate(target_column="missing"),
        )
    assert error.value.status_code == 422


def test_profile_job_queries_enforce_scope_and_not_found(monkeypatch) -> None:
    expected = _job()
    monkeypatch.setattr(jobs, "require_project_role", lambda *_args: None)
    user = SimpleNamespace(id=uuid.uuid4())
    assert jobs.get_profiling_job(
        _CreateSession(SimpleNamespace(), [expected]), user, expected.project_id, expected.id
    ) is expected
    assert jobs.latest_profiling_job(
        _CreateSession(SimpleNamespace(), [expected]),
        user,
        expected.project_id,
        expected.dataset_version_id,
    ) is expected
    with pytest.raises(HTTPException, match="not found") as error:
        jobs.get_profiling_job(
            _CreateSession(SimpleNamespace(), [None]), user, expected.project_id, expected.id
        )
    assert error.value.status_code == 404


def test_profile_scheduler_deduplicates_submission_and_releases_slot(monkeypatch) -> None:
    job_id = uuid.uuid4()
    future: Future[None] = Future()
    executor = MagicMock()
    executor.submit.return_value = future
    monkeypatch.setattr(jobs, "_executor", executor)
    jobs._scheduled_jobs.discard(job_id)

    assert jobs.schedule_profiling_job(job_id) is True
    assert jobs.schedule_profiling_job(job_id) is False
    executor.submit.assert_called_once_with(jobs._run_profiling_job, job_id)
    future.set_result(None)
    assert job_id not in jobs._scheduled_jobs


def test_resume_incomplete_jobs_requeues_and_schedules_each_once(monkeypatch) -> None:
    job_ids = [uuid.uuid4(), uuid.uuid4()]
    query = MagicMock()
    session = MagicMock()
    session.__enter__.return_value = session
    session.scalars.return_value.all.return_value = job_ids
    session.query.return_value.filter.return_value = query
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: session)
    scheduled: list[uuid.UUID] = []
    monkeypatch.setattr(
        jobs,
        "schedule_profiling_job",
        lambda job_id: scheduled.append(job_id) or job_id == job_ids[0],
    )

    assert jobs.resume_incomplete_profiling_jobs() == 1
    assert scheduled == job_ids
    query.update.assert_called_once()
    session.commit.assert_called_once()


def test_profile_worker_dispatches_partitioned_and_monolithic_paths(monkeypatch) -> None:
    for supported, expected in ((True, "partitioned"), (False, "monolithic")):
        job = _job()
        job.status = "queued"
        job.started_at = None
        version = _version()
        session = _Session(job, SimpleNamespace(), version)
        monkeypatch.setattr(
            jobs,
            "get_session_factory",
            lambda current=session: lambda: current,
        )
        monkeypatch.setattr(
            jobs,
            "_get_version",
            lambda *_args, current=version: current,
        )
        monkeypatch.setattr(
            jobs,
            "_supports_partitioned_stages",
            lambda _version, current=supported: current,
        )
        calls: list[str] = []
        monkeypatch.setattr(
            jobs,
            "_run_partitioned_stages",
            lambda _job_id, current=calls: current.append("partitioned"),
        )
        monkeypatch.setattr(
            jobs,
            "_run_monolithic_fallback",
            lambda _job_id, current=calls: current.append("monolithic"),
        )

        jobs._run_profiling_job(job.id)

        assert calls == [expected]
        assert job.status == "running"
        assert version.status == DatasetStatus.PROFILING


def test_profile_worker_persists_failure_and_preserves_cancellation(monkeypatch) -> None:
    job = _job()
    job.status = "queued"
    version = _version()
    session = _Session(job, SimpleNamespace(), version)
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(jobs, "_get_version", lambda *_args: version)
    monkeypatch.setattr(jobs, "_supports_partitioned_stages", lambda _version: False)
    monkeypatch.setattr(
        jobs,
        "_run_monolithic_fallback",
        MagicMock(side_effect=RuntimeError("profiling failed")),
    )
    with pytest.raises(RuntimeError, match="profiling failed"):
        jobs._run_profiling_job(job.id)
    assert job.status == "failed"
    assert job.failure_message == "profiling failed"
    assert version.status == DatasetStatus.FAILED
    assert job.overview_json["stages"][job.current_stage] == "failed"

    cancelled = _job()
    cancelled.status = "cancelled"
    cancelled_session = _Session(cancelled, SimpleNamespace(), version)
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: cancelled_session)
    jobs._run_profiling_job(cancelled.id)
    assert cancelled.status == "cancelled"


def test_finish_preparation_promotes_complete_profile_atomically(monkeypatch) -> None:
    job = _job()
    job.row_count = 3
    job.target_column = None
    job.feature_profiles_json = {
        "amount": _profile(job).columns[0].model_dump(mode="json")
    }
    job.overview_json = {
        **job.overview_json,
        "leakage_analysis": LeakageAnalysisRead(status="not_applicable").model_dump(mode="json"),
    }
    version = SimpleNamespace(status=DatasetStatus.PROFILING, profile_artifact_uri=None)
    session = _Session(job, SimpleNamespace(), version)
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(jobs, "_build_preparation_plan", lambda *_args: [])

    def store(current_job: object, stage: str, _payload: object) -> dict[str, str]:
        return {**current_job.artifact_uris_json, stage: f"s3://profiles/{stage}.json"}

    monkeypatch.setattr(jobs, "_store_stage_artifact", store)

    jobs._finish_preparation(job.id)

    assert job.status == "succeeded"
    assert job.current_stage == "complete"
    assert job.progress == 1.0
    assert job.finished_at is not None
    assert job.artifact_uris_json["preparation"].endswith("preparation.json")
    assert job.artifact_uris_json["complete"].endswith("complete.json")
    assert job.overview_json["stages"]["preparation"] == "completed"
    assert version.status == DatasetStatus.READY
    assert version.profile_artifact_uri == job.artifact_uris_json["complete"]


def test_finish_preparation_honors_cancellation(monkeypatch) -> None:
    job = _job()
    job.status = "cancelled"
    version = SimpleNamespace(status=DatasetStatus.PROFILING, profile_artifact_uri=None)
    session = _Session(job, SimpleNamespace(), version)
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: session)
    store = MagicMock()
    monkeypatch.setattr(jobs, "_store_stage_artifact", store)

    jobs._finish_preparation(job.id)

    store.assert_not_called()
    assert version.status == DatasetStatus.PROFILING


def test_monolithic_fallback_reuses_features_but_recomputes_target_evidence(monkeypatch) -> None:
    job = _job()
    job.total_columns = 1
    job.completed_columns = 1
    job.row_count = 3
    job.feature_profiles_json = {
        "amount": _profile(job).columns[0].model_dump(mode="json")
    }
    job.artifact_uris_json = {"features": "s3://profiles/reused-features.json"}
    job.overview_json = {
        **job.overview_json,
        "stages": {
            "features": "reused",
            "relationships": "running",
            "preparation": "queued",
        },
    }
    version = SimpleNamespace(status=DatasetStatus.PROFILING, profile_artifact_uri=None)
    session = _Session(job, SimpleNamespace(id=job.created_by_id), version)
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(jobs, "_load_rows", lambda _version: ([{"amount": 1}], ["sampled"]))
    leakage = LeakageAnalysisRead(status="not_applicable", warnings=["checked"])
    monkeypatch.setattr(jobs, "detect_target_leakage", lambda *_args: leakage)
    monkeypatch.setattr(jobs, "_relationships_against_target", lambda *_args: [])
    monkeypatch.setattr(jobs, "_build_preparation_plan", lambda *_args: [])
    stored: list[str] = []

    def store(current_job: object, stage: str, _payload: object) -> dict[str, str]:
        stored.append(stage)
        return {**current_job.artifact_uris_json, stage: f"s3://profiles/{stage}.json"}

    monkeypatch.setattr(jobs, "_store_stage_artifact", store)

    jobs._run_monolithic_fallback(job.id)

    assert stored == ["relationships", "preparation", "complete"]
    assert job.artifact_uris_json["features"] == "s3://profiles/reused-features.json"
    assert job.overview_json["stages"]["features"] == "reused"
    assert job.warnings_json == ["sampled", "checked"]
    assert job.status == "succeeded"


def test_partitioned_ray_polars_stages_checkpoint_batches_and_complete(monkeypatch) -> None:
    import automl_api.services.ray_polars_profiling as ray_profile

    job = _job()
    job.status = "running"
    job.total_columns = 0
    job.completed_columns = 0
    job.row_count = None
    job.feature_profiles_json = {}
    job.artifact_uris_json = {}
    version = SimpleNamespace(status=DatasetStatus.PROFILING, profile_artifact_uri=None)
    session = _Session(job, SimpleNamespace(), version)
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: session)
    dataset = MagicMock()
    dataset.count.return_value = 4
    dataset.schema.return_value = SimpleNamespace(names=["amount", "segment"])
    monkeypatch.setattr(ray_profile, "_load_dataset", lambda _version: dataset)

    def profile_column(_dataset: object, column: str, _rows: int) -> ColumnProfileRead:
        base = _profile(job).columns[0]
        return base.model_copy(update={"name": column})

    monkeypatch.setattr(ray_profile, "_profile_column", profile_column)
    monkeypatch.setattr(ray_profile, "_relationships", lambda *_args: ([], ["bounded sample"]))
    monkeypatch.setattr(ray_profile, "_sample_rows", lambda *_args: [{"amount": 1}])
    leakage = LeakageAnalysisRead(status="not_applicable", warnings=["leakage checked"])
    monkeypatch.setattr(jobs, "detect_target_leakage", lambda *_args: leakage)
    monkeypatch.setattr(jobs, "_build_preparation_plan", lambda *_args: [])
    stored: list[str] = []

    def store(current_job: object, stage: str, _payload: object) -> dict[str, str]:
        stored.append(stage)
        return {**current_job.artifact_uris_json, stage: f"s3://profiles/{stage}.json"}

    monkeypatch.setattr(jobs, "_store_stage_artifact", store)

    jobs._run_partitioned_stages(job.id)

    assert job.row_count == 4
    assert job.total_columns == 2
    assert job.completed_columns == 2
    assert set(job.feature_profiles_json) == {"amount", "segment"}
    assert job.warnings_json == ["bounded sample", "leakage checked"]
    assert job.status == "succeeded"
    assert stored == ["features", "relationships", "preparation", "complete"]
    assert version.status == DatasetStatus.READY
