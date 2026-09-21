from __future__ import annotations

import hashlib
import io
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import automl_api.preparation_worker as worker
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray
from automl_api.core.config import Settings
from automl_api.models.enums import AttemptStatus, DatasetFormat, WorkflowStage
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.services import reconciler
from automl_api.storage.contracts import ObjectMetadata, RayDataSourceDescriptor
from automl_shared.data_identity import content_fingerprint, split_role, stable_row_id


def _dataset_attempt(stage: WorkflowStage = WorkflowStage.SPLITTER) -> WorkflowAttempt:
    profile_id = uuid.uuid4()
    attempt = WorkflowAttempt(
        project_id=uuid.uuid4(),
        stage=stage,
        logical_key=f"profile:{profile_id}:{stage.value}",
        dataset_version_id=uuid.uuid4(),
        workload_identity=f"sceptre-dataset-{stage.value}",
        generation=1,
        fencing_token="dataset-fence",
        status=AttemptStatus.PENDING,
        retry_budget=5,
        terminal_cas_version=0,
    )
    attempt.id = uuid.uuid4()
    return attempt


def _profile_and_version(attempt: WorkflowAttempt) -> tuple[SimpleNamespace, SimpleNamespace]:
    profile_id = uuid.UUID(attempt.logical_key.split(":")[1])
    profile = SimpleNamespace(
        id=profile_id,
        project_id=attempt.project_id,
        dataset_version_id=attempt.dataset_version_id,
        status="queued",
        current_stage="overview",
        started_at=None,
        heartbeat_at=None,
        overview_json={"execution_mode": "kuberay"},
    )
    version = SimpleNamespace(
        id=attempt.dataset_version_id,
        project_id=attempt.project_id,
        status="ready",
    )
    return profile, version


def _dataset_entry(attempt: WorkflowAttempt, profile_id: uuid.UUID) -> OutboxEntry:
    entry = OutboxEntry(
        project_id=attempt.project_id,
        command_id=uuid.uuid4(),
        event_key="dataset-submit",
        topic=(
            "ray.dataset.split.submit"
            if attempt.stage == WorkflowStage.SPLITTER
            else "ray.dataset.prepare.submit"
        ),
        aggregate_type="profiling_job",
        aggregate_id=profile_id,
        payload={
            "profiling_job_id": str(profile_id),
            "attempt_id": str(attempt.id),
            "fencing_token": attempt.fencing_token,
        },
    )
    entry.id = uuid.uuid4()
    return entry


def test_dataset_ray_manifest_is_stage_scoped_and_fenced(monkeypatch) -> None:
    attempt = _dataset_attempt()
    profile, version = _profile_and_version(attempt)
    monkeypatch.setattr(
        reconciler,
        "get_settings",
        lambda: Settings(
            training_namespace="sceptre",
            object_store_type="s3_compatible",
            object_store_endpoint="http://objects:8333",
            ray_max_workers=2,
        ),
    )

    manifest = reconciler.build_dataset_ray_job_manifest(
        profile, version, attempt, name="dataset-split"
    )

    assert manifest["metadata"]["namespace"] == "sceptre"
    assert manifest["metadata"]["labels"]["automl.platform/workflow-stage"] == "splitter"
    assert "--stage splitter" in manifest["spec"]["entrypoint"]
    assert manifest["spec"]["backoffLimit"] == 0
    cluster = manifest["spec"]["rayClusterSpec"]
    assert cluster["workerGroupSpecs"][0]["minReplicas"] == 1
    assert cluster["workerGroupSpecs"][0]["maxReplicas"] == 2
    assert cluster["headGroupSpec"]["rayStartParams"] == {
        "dashboard-host": "0.0.0.0",
        "num-cpus": "0",
        "object-store-memory": "268435456",
    }
    assert cluster["headGroupSpec"]["template"]["spec"]["containers"][0]["resources"] == {
        "requests": {"cpu": "500m", "memory": "4Gi"},
        "limits": {"cpu": "2", "memory": "6Gi"},
    }
    assert cluster["workerGroupSpecs"][0]["rayStartParams"] == {"num-cpus": "2"}
    assert cluster["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]["resources"] == {
        "requests": {"cpu": "1", "memory": "4Gi"},
        "limits": {"cpu": "2", "memory": "5Gi"},
    }
    for pod in (
        manifest["spec"]["submitterPodTemplate"]["spec"],
        cluster["headGroupSpec"]["template"]["spec"],
        cluster["workerGroupSpecs"][0]["template"]["spec"],
    ):
        if pod["containers"][0]["name"] == "ray-head":
            assert "serviceAccountName" not in pod
            assert pod["automountServiceAccountToken"] is True
        else:
            assert pod["serviceAccountName"] == attempt.workload_identity
            assert pod["automountServiceAccountToken"] is False
        env = {item["name"]: item for item in pod["containers"][0]["env"]}
        assert env["AUTOML_FENCING_TOKEN"]["value"] == "dataset-fence"
        assert env["AUTOML_PROFILE_JOB_ID"]["value"] == str(profile.id)


def test_dataset_submit_tracks_ray_identity_and_rejects_wrong_stage() -> None:
    attempt = _dataset_attempt()
    profile, version = _profile_and_version(attempt)
    entry = _dataset_entry(attempt, profile.id)
    db = MagicMock()
    db.scalar.side_effect = [attempt, profile, version]
    k8s = MagicMock()
    k8s.create_ray_job.return_value = {
        "metadata": {"name": "created-split"},
        "status": {"rayClusterName": "split-cluster", "jobId": "split-job"},
    }

    result = reconciler.submit_dataset_ray_job(db, entry, k8s)

    assert result.status == AttemptStatus.SUBMITTED
    assert result.ray_job_name == "created-split"
    assert profile.current_stage == "splitter"
    assert profile.status == "running"
    k8s.ensure_service_account.assert_called_once_with(attempt.workload_identity)

    wrong = _dataset_attempt(WorkflowStage.PREPARATION)
    wrong_entry = _dataset_entry(wrong, uuid.UUID(wrong.logical_key.split(":")[1]))
    wrong_entry.topic = "ray.dataset.split.submit"
    db.scalar.side_effect = [wrong]
    with pytest.raises(ValueError, match="does not match"):
        reconciler.submit_dataset_ray_job(db, wrong_entry, k8s)


def test_dataset_submit_is_idempotent_and_checks_scope_and_fence() -> None:
    attempt = _dataset_attempt()
    profile, _ = _profile_and_version(attempt)
    entry = _dataset_entry(attempt, profile.id)
    attempt.status = AttemptStatus.SUBMITTED
    db = MagicMock()
    db.scalar.return_value = attempt
    assert reconciler.submit_dataset_ray_job(db, entry, MagicMock()) is attempt

    entry.payload["fencing_token"] = "stale"
    with pytest.raises(ValueError, match="fence"):
        reconciler.submit_dataset_ray_job(db, entry, MagicMock())

    db.scalar.return_value = None
    with pytest.raises(LookupError, match="attempt"):
        reconciler.submit_dataset_ray_job(db, entry, MagicMock())


def test_dataset_observer_marks_running_and_replaces_lost_generation(monkeypatch) -> None:
    attempt = _dataset_attempt()
    attempt.status = AttemptStatus.SUBMITTED
    attempt.ray_job_name = "split-job"
    profile, _ = _profile_and_version(attempt)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.scalar.return_value = profile
    k8s = MagicMock()
    k8s.ray_job.return_value = {
        "status": {"jobStatus": "RUNNING", "rayClusterName": "cluster", "jobId": "job"}
    }

    assert reconciler.observe_dataset_ray_jobs(db, k8s) == 1
    assert attempt.status == AttemptStatus.RUNNING
    assert profile.heartbeat_at is not None

    command = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.scalar.side_effect = [profile, command]
    k8s.ray_job.side_effect = reconciler.ApiException(status=404)
    enqueue = MagicMock()
    monkeypatch.setattr(reconciler, "enqueue_outbox", enqueue)

    reconciler.observe_dataset_ray_jobs(db, k8s)

    assert attempt.status == AttemptStatus.SUPERSEDED
    replacement = next(
        call.args[0] for call in db.add.call_args_list if isinstance(call.args[0], WorkflowAttempt)
    )
    assert replacement.generation == 2
    assert replacement.predecessor_attempt_id == attempt.id
    assert enqueue.call_args.kwargs["topic"] == "ray.dataset.split.submit"


def test_dataset_observer_fails_profile_closed_at_retry_budget() -> None:
    attempt = _dataset_attempt()
    attempt.status = AttemptStatus.RUNNING
    attempt.ray_job_name = "split-job"
    attempt.retry_budget = 1
    attempt.terminal_reason = "Unsupported canonical value type: time"
    profile, version = _profile_and_version(attempt)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.scalar.return_value = profile
    db.get.return_value = version
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobStatus": "FAILED"}}

    reconciler.observe_dataset_ray_jobs(db, k8s)

    assert attempt.status == AttemptStatus.FAILED
    assert profile.status == "failed"
    assert profile.failure_message == "Unsupported canonical value type: time"
    assert version.status == "ready"


def test_dataset_observer_retries_nested_image_pull_failure_after_grace(monkeypatch) -> None:
    attempt = _dataset_attempt()
    attempt.status = AttemptStatus.SUBMITTED
    attempt.ray_job_name = "split-job"
    attempt.updated_at = datetime.now(UTC) - timedelta(minutes=10)
    profile, _ = _profile_and_version(attempt)
    command = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.scalar.side_effect = [profile, command]
    k8s = MagicMock()
    k8s.ray_job.return_value = {
        "status": {
            "jobDeploymentStatus": "Initializing",
            "rayClusterStatus": {"conditions": [{"reason": "ImagePullBackOff", "status": "False"}]},
        }
    }
    enqueue = MagicMock()
    monkeypatch.setattr(reconciler, "enqueue_outbox", enqueue)

    reconciler.observe_dataset_ray_jobs(db, k8s)

    assert attempt.status == AttemptStatus.SUPERSEDED
    replacement = next(
        call.args[0] for call in db.add.call_args_list if isinstance(call.args[0], WorkflowAttempt)
    )
    assert replacement.generation == 2
    assert "ImagePullBackOff" in attempt.terminal_reason
    enqueue.assert_called_once()


def test_identity_batch_is_bounded_stable_and_duplicate_safe(monkeypatch) -> None:
    digest = hashlib.sha256(b"source").hexdigest()
    table = pa.table(
        {
            "id": [0, 1, 2],
            "feature": ["same", "same", "different"],
            "target": [1, 1, 0],
        }
    )
    result = worker._attach_identity_batch(
        table,
        columns=["feature", "target"],
        source_digest=digest,
        split_seed="seed",
    )
    rows = result.to_pylist()
    assert len({row["row_id"] for row in rows}) == 3
    assert rows[0]["content_fingerprint"] == rows[1]["content_fingerprint"]
    assert rows[0]["split_role"] == rows[1]["split_role"]
    assert rows[0]["content_fingerprint"] == content_fingerprint(
        {"feature": "same", "target": 1}, ["feature", "target"]
    )

    monkeypatch.setattr(worker, "RAY_BATCH_ROWS", 2)
    with pytest.raises(RuntimeError, match="bounded"):
        worker._attach_identity_batch(
            table,
            columns=["feature", "target"],
            source_digest=digest,
            split_seed="seed",
        )


def test_identity_summaries_merge_without_collecting_row_ids() -> None:
    first = worker._identity_summary_batch(
        pa.table({"row_id": ["a", "b"], "split_role": ["train", "validation"]})
    )
    second = worker._identity_summary_batch(
        pa.table({"row_id": ["c"], "split_role": ["final_test"]})
    )
    merged = worker._merge_identity_summaries(
        [
            {"summary_json": first["summary_json"][0].as_py()},
            {"summary_json": second["summary_json"][0].as_py()},
        ]
    )
    assert merged["row_count"] == 3
    assert merged["split_counts"] == {"train": 1, "validation": 1, "final_test": 1}
    assert all(len(value) == 64 for value in merged["split_digests"].values())
    assert merged["split_integrity"] == {
        "cross_role_fingerprint_overlaps": 0,
        "duplicate_content_rows": 0,
    }


def test_identity_summary_reports_zero_cross_role_overlap_for_consistent_roles() -> None:
    digest = hashlib.sha256(b"source").hexdigest()
    seed = f"{worker.SPLIT_SEED_REVISION}:{digest}"
    table = pa.table(
        {
            "row_id": [stable_row_id(digest, index) for index in range(4)],
            "content_fingerprint": [f"{index:064x}" for index in range(4)],
        }
    )
    fingerprints = table.column("content_fingerprint").to_pylist()
    roles = [split_role(fingerprint, seed) for fingerprint in fingerprints]
    table = table.append_column("split_role", pa.array(roles))

    summary = worker._identity_summary_batch(table, split_seed=seed)

    payload = json.loads(summary["summary_json"][0].as_py())
    assert payload["role_mismatches"] == 0
    assert payload["duplicate_rows_in_batch"] == 0


def test_identity_summary_counts_cross_role_overlap_and_duplicates_opaquely() -> None:
    digest = hashlib.sha256(b"source").hexdigest()
    seed = f"{worker.SPLIT_SEED_REVISION}:{digest}"
    fingerprint = "f" * 64
    expected_role = split_role(fingerprint, seed)
    forged_role = next(
        role for role in ("train", "validation", "final_test") if role != expected_role
    )
    table = pa.table(
        {
            "row_id": ["a", "b", "c"],
            "content_fingerprint": [fingerprint, fingerprint, "e" * 64],
            "split_role": [forged_role, expected_role, expected_role],
        }
    )

    summary = worker._identity_summary_batch(table, split_seed=seed)

    payload = json.loads(summary["summary_json"][0].as_py())
    assert payload["role_mismatches"] == 1
    assert payload["duplicate_rows_in_batch"] == 1

    with pytest.raises(ValueError, match="crossed"):
        worker._merge_identity_summaries([{"summary_json": summary["summary_json"][0].as_py()}])


class _LocalStore:
    def __init__(self, root: Path, raw: Path) -> None:
        self.root = root
        self.raw = raw

    def dataframe_source(self, uri: str) -> RayDataSourceDescriptor:
        return RayDataSourceDescriptor(path=uri, filesystem_options={}, provider="embedded")

    def uri_for_key(self, key: str) -> str:
        return str(self.root / key)

    def stat(self, uri: str) -> ObjectMetadata:
        return ObjectMetadata(uri=uri, byte_size=Path(uri).stat().st_size)

    def open_stream(self, uri: str):
        return Path(uri).open("rb")

    def read_head(self, uri: str, byte_count: int = 4096) -> bytes:
        with Path(uri).open("rb") as source:
            return source.read(byte_count)


@pytest.mark.parametrize("time_column", [None, "id", "day", "dense_day", "one_day", "two_days"])
def test_split_dataset_writes_role_isolated_parquet_locally(
    tmp_path, monkeypatch, time_column
) -> None:
    rows = [
        f"{index},{index % 7},{index % 2},2026-01-{index // 17 + 1:02d},"
        f"{0 if index < 498 else index - 497},0,{index % 2}\n"
        for index in range(500)
    ]
    content = ("id,feature,target,day,dense_day,one_day,two_days\n" + "".join(rows)).encode()
    raw = tmp_path / "raw.csv"
    raw.write_bytes(content)
    store = _LocalStore(tmp_path, raw)
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    monkeypatch.setattr("automl_api.services.ray_polars_profiling.get_object_store", lambda: store)
    version = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        object_uri=str(raw),
        byte_size=len(content),
        content_hash=hashlib.sha256(content).hexdigest(),
        content_hash_algorithm="sha256",
        format=DatasetFormat.CSV,
    )

    if time_column in {"one_day", "two_days"}:
        with pytest.raises(ValueError, match="at least three distinct"):
            worker.split_dataset(version, uuid.uuid4(), 1, "target", time_column=time_column)
        ray.shutdown()
        return

    result = worker.split_dataset(version, uuid.uuid4(), 1, "target", time_column=time_column)

    assert result["row_count"] == 500
    for role in ("train", "validation"):
        for path in Path(result["uris"][role]).rglob("*.parquet"):
            table = pq.ParquetFile(path).read()
            assert table["id"].to_pylist() == table["source_ordinal"].to_pylist()
    assert result["split_strategy"] == ("temporal" if time_column else "content_hash")
    if time_column:
        boundaries = result["time_boundaries"]
        if time_column == "day":
            assert boundaries["train"][1] < boundaries["validation"][0]
            assert boundaries["validation"][1] < boundaries["final_test"][0]
    assert all(
        result["identity"]["split_counts"][role] for role in result["identity"]["split_counts"]
    )
    assert not Path(result["uris"]["train"]).samefile(Path(result["uris"]["validation"]))
    final_input_files = list(Path(result["uris"]["final_input"]).rglob("*.parquet"))
    final_label_files = list(Path(result["uris"]["final_label"]).rglob("*.parquet"))
    assert final_input_files and final_label_files
    assert "id" in pq.read_schema(final_input_files[0]).names
    assert "target" not in pq.read_schema(final_input_files[0]).names
    assert pq.read_schema(final_label_files[0]).names == ["row_id", "target"]
    assert not Path(str(Path(result["root_uri"]) / "roles" / "split_role=final_test")).exists()
    ray.shutdown()


def test_raw_verification_rejects_size_digest_and_algorithm(monkeypatch) -> None:
    content = b"verified"
    store = MagicMock()
    store.stat.return_value = SimpleNamespace(byte_size=len(content))
    store.open_stream.side_effect = lambda _uri: io.BytesIO(content)
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    version = SimpleNamespace(
        object_uri="memory://raw",
        byte_size=len(content),
        content_hash=hashlib.sha256(content).hexdigest(),
        content_hash_algorithm="sha256",
    )
    assert worker._verify_raw(version) == version.content_hash
    version.byte_size += 1
    with pytest.raises(ValueError, match="byte size"):
        worker._verify_raw(version)
    version.byte_size = len(content)
    version.content_hash = "0" * 64
    with pytest.raises(ValueError, match="SHA-256"):
        worker._verify_raw(version)
    version.content_hash_algorithm = "md5"
    with pytest.raises(ValueError, match="only accepts SHA-256"):
        worker._verify_raw(version)


def test_worker_attempt_context_requires_both_fence_values(monkeypatch) -> None:
    monkeypatch.delenv("AUTOML_ATTEMPT_ID", raising=False)
    monkeypatch.delenv("AUTOML_FENCING_TOKEN", raising=False)
    with pytest.raises(ValueError, match="required"):
        worker._attempt_context()
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(uuid.uuid4()))
    with pytest.raises(ValueError, match="required"):
        worker._attempt_context()
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", "fence")
    attempt_id, fence = worker._attempt_context()
    assert isinstance(attempt_id, uuid.UUID) and fence == "fence"


class _SessionContext:
    def __init__(self, db: MagicMock) -> None:
        self.db = db

    def __enter__(self):
        return self.db

    def __exit__(self, *_args):
        return False


def _session_factory(monkeypatch, db: MagicMock) -> None:
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: _SessionContext(db))


def test_begin_attempt_transitions_submitted_and_accepts_running(monkeypatch) -> None:
    attempt = _dataset_attempt()
    attempt.status = AttemptStatus.SUBMITTED
    profile, _ = _profile_and_version(attempt)
    profile_id = profile.id
    db = MagicMock()
    locked_query = db.query.return_value.filter.return_value.with_for_update.return_value
    locked_query.one_or_none.return_value = attempt
    db.get.return_value = profile
    _session_factory(monkeypatch, db)
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(attempt.id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", attempt.fencing_token)
    transition = MagicMock()
    monkeypatch.setattr(worker, "transition_attempt", transition)

    assert worker._begin_attempt(profile_id, WorkflowStage.SPLITTER) == (
        attempt.id,
        attempt.fencing_token,
    )
    transition.assert_called_once()
    assert profile.current_stage == "splitter" and profile.started_at is not None

    attempt.status = AttemptStatus.RUNNING
    transition.reset_mock()
    worker._begin_attempt(profile_id, WorkflowStage.SPLITTER)
    transition.assert_not_called()


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("missing", ValueError),
        ("wrong_lineage", ValueError),
        ("stale_fence", worker.StaleFence),
        ("terminal", worker.StaleFence),
    ],
)
def test_begin_attempt_rejects_invalid_or_stale_lineage(monkeypatch, mutation, expected) -> None:
    attempt = _dataset_attempt()
    attempt.status = AttemptStatus.RUNNING
    profile, _ = _profile_and_version(attempt)
    db = MagicMock()
    query = db.query.return_value.filter.return_value.with_for_update.return_value
    query.one_or_none.return_value = attempt
    db.get.return_value = profile
    if mutation == "missing":
        query.one_or_none.return_value = None
    elif mutation == "wrong_lineage":
        attempt.project_id = uuid.uuid4()
    elif mutation == "stale_fence":
        attempt.fencing_token = "newer"
    else:
        attempt.status = AttemptStatus.SUCCEEDED
    _session_factory(monkeypatch, db)
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(attempt.id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", "dataset-fence")

    with pytest.raises(expected):
        worker._begin_attempt(profile.id, WorkflowStage.SPLITTER)


def test_local_prefix_helpers_handle_missing_and_existing_paths(tmp_path, monkeypatch) -> None:
    raw = tmp_path / "raw.csv"
    raw.write_text("a\n1\n")
    store = _LocalStore(tmp_path, raw)
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    missing = tmp_path / "missing"
    worker._delete_prefix(str(missing))
    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "part").write_bytes(b"1234")
    assert worker._prefix_size(str(existing)) == 4
    worker._delete_prefix(str(existing))
    assert not existing.exists()

    filesystem = MagicMock()
    filesystem.get_file_info.side_effect = [
        SimpleNamespace(type=worker.pa_fs.FileType.Directory),
        [SimpleNamespace(size=7, is_file=True), SimpleNamespace(size=0, is_file=False)],
    ]
    monkeypatch.setattr(worker, "_storage_path", lambda _uri: ("bucket/prefix", filesystem))
    worker._delete_prefix("s3://bucket/prefix")
    assert worker._prefix_size("s3://bucket/prefix") == 7
    filesystem.delete_dir.assert_called_once_with("bucket/prefix")


def test_splitter_preflight_rejects_empty_schema_and_missing_target(monkeypatch) -> None:
    version = SimpleNamespace(byte_size=1)
    source = MagicMock()
    source.count.return_value = 1
    monkeypatch.setattr(worker, "_verify_raw", lambda _version: "a" * 64)
    monkeypatch.setattr(worker, "_load_dataset", lambda *_args, **_kwargs: source)
    source.schema.return_value.names = []
    with pytest.raises(ValueError, match="no columns"):
        worker.split_dataset(version, uuid.uuid4(), 1, None)
    source.schema.return_value.names = ["feature"]
    with pytest.raises(ValueError, match="absent"):
        worker.split_dataset(version, uuid.uuid4(), 1, "target")


def test_next_revision_handles_empty_and_existing_history() -> None:
    model = SimpleNamespace(revision=MagicMock(), dataset_version_id=MagicMock())
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = []
    assert worker._next_revision(db, model, uuid.uuid4()) == 1
    db.query.return_value.filter.return_value.all.return_value = [(1,), (4,)]
    assert worker._next_revision(db, model, uuid.uuid4()) == 5


def _split_result(project_id: uuid.UUID) -> dict:
    return {
        "project_id": str(project_id),
        "root_uri": "s3://bucket/prepared",
        "byte_size": 100,
        "row_count": 10,
        "source_digest": "a" * 64,
        "identity": {
            "split_digests": {
                "train": "b" * 64,
                "validation": "c" * 64,
                "final_test": "d" * 64,
            }
        },
        "uris": {"train": "train", "validation": "validation"},
    }


@pytest.mark.parametrize("failure", ["lineage", "cas", "command"])
def test_persist_split_fails_closed_before_submitting_preparation(monkeypatch, failure) -> None:
    attempt = _dataset_attempt()
    attempt.terminal_cas_version = 0
    profile, _ = _profile_and_version(attempt)
    result = _split_result(profile.project_id)
    store = MagicMock()
    store.put_bytes.return_value.uri = "s3://bucket/split-manifest.json"
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    db = MagicMock()
    attempt_query = MagicMock()
    attempt_query.filter.return_value.with_for_update.return_value.one.return_value = attempt
    revision_query = MagicMock()
    revision_query.filter.return_value.all.return_value = []
    command_query = MagicMock()
    command_query.filter.return_value.order_by.return_value.first.return_value = None
    db.query.side_effect = [attempt_query, revision_query, revision_query, command_query]
    db.get.return_value = None if failure == "lineage" else profile
    _session_factory(monkeypatch, db)
    monkeypatch.setattr(
        worker,
        "cas_register_terminal_artifact",
        lambda *_args, **_kwargs: failure != "cas",
    )

    expected = ValueError if failure in {"lineage", "command"} else worker.StaleFence
    with pytest.raises(expected):
        worker._persist_split(profile.id, attempt.id, attempt.fencing_token, result)
    manifest_key = store.put_bytes.call_args.args[0]
    assert f"attempts/{attempt.id}/split-manifest.json" in manifest_key


@pytest.mark.parametrize("failure", ["profile", "version"])
def test_prepare_profile_requires_complete_source_lineage(monkeypatch, failure) -> None:
    profile_id = uuid.uuid4()
    profile = SimpleNamespace(id=profile_id, dataset_version_id=uuid.uuid4())
    db = MagicMock()
    if failure == "profile":
        db.get.return_value = None
    else:
        db.get.side_effect = [profile, None]
        monkeypatch.setattr(worker, "_prepared_for_profile", lambda *_args: SimpleNamespace())
    _session_factory(monkeypatch, db)
    with pytest.raises(ValueError):
        worker.prepare_profile(profile_id)


def test_preparation_progress_is_monotonic_and_fenced(monkeypatch) -> None:
    attempt = _dataset_attempt(WorkflowStage.PREPARATION)
    attempt.status = AttemptStatus.RUNNING
    profile, _ = _profile_and_version(attempt)
    profile.progress = 0.55
    profile.overview_json = {"workflow_attempt_id": str(attempt.id)}
    db = MagicMock()
    db.scalar.side_effect = [attempt, profile]
    _session_factory(monkeypatch, db)

    worker._record_preparation_progress(
        profile.id,
        attempt.id,
        attempt.fencing_token,
        progress=0.3,
        milestone="row_counted",
    )

    assert profile.progress == 0.55
    assert profile.current_stage == "features"
    assert profile.overview_json["stages"]["features"] == "running"
    assert profile.overview_json["preparation_milestone"] == "row_counted"
    assert profile.heartbeat_at == attempt.heartbeat_at
    db.commit.assert_called_once()


def test_preparation_progress_persists_each_completed_feature(monkeypatch) -> None:
    attempt = _dataset_attempt(WorkflowStage.PREPARATION)
    attempt.status = AttemptStatus.RUNNING
    profile, _ = _profile_and_version(attempt)
    profile.progress = 0.55
    profile.completed_columns = 0
    profile.total_columns = 2
    profile.feature_profiles_json = {}
    profile.overview_json = {"workflow_attempt_id": str(attempt.id)}
    db = MagicMock()
    db.scalar.side_effect = [attempt, profile]
    _session_factory(monkeypatch, db)

    worker._record_preparation_progress(
        profile.id,
        attempt.id,
        attempt.fencing_token,
        progress=0.65,
        milestone="feature_complete",
        feature_profile={"name": "amount", "semantic_type": "numerical_continuous"},
        completed_columns=1,
        total_columns=2,
        row_count=100,
    )

    assert profile.feature_profiles_json["amount"]["semantic_type"] == "numerical_continuous"
    assert profile.completed_columns == 1
    assert profile.total_columns == 2
    assert profile.row_count == 100
    assert profile.progress == 0.65
    assert profile.current_stage == "features"
    db.commit.assert_called_once()


def test_preparation_progress_rejects_a_stale_fence(monkeypatch) -> None:
    attempt = _dataset_attempt(WorkflowStage.PREPARATION)
    attempt.status = AttemptStatus.RUNNING
    profile, _ = _profile_and_version(attempt)
    profile.progress = 0.2
    profile.overview_json = {"workflow_attempt_id": str(attempt.id)}
    db = MagicMock()
    db.scalar.side_effect = [attempt, profile]
    _session_factory(monkeypatch, db)

    with pytest.raises(worker.StaleFence):
        worker._record_preparation_progress(
            profile.id,
            attempt.id,
            "stale-fence",
            progress=0.3,
            milestone="row_counted",
        )

    db.commit.assert_not_called()


@pytest.mark.parametrize("failure", ["lineage", "cas"])
def test_persist_profile_rejects_missing_lineage_and_stale_terminal_cas(
    monkeypatch, failure
) -> None:
    profile_id = uuid.uuid4()
    attempt = _dataset_attempt(WorkflowStage.PREPARATION)
    profile = SimpleNamespace()
    version = SimpleNamespace()
    artifact = SimpleNamespace(project_id=attempt.project_id)
    store = MagicMock()
    store.put_bytes.return_value.uri = "s3://bucket/complete.json"
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    db = MagicMock()
    query = MagicMock()
    query.filter.return_value.with_for_update.return_value.one.return_value = attempt
    db.query.return_value = query
    db.get.side_effect = [None, version] if failure == "lineage" else [profile, version]
    _session_factory(monkeypatch, db)
    monkeypatch.setattr(worker, "cas_register_terminal_artifact", lambda *_args, **_kwargs: False)
    expected = ValueError if failure == "lineage" else worker.StaleFence
    with pytest.raises(expected):
        worker._persist_profile(profile_id, attempt.id, attempt.fencing_token, {}, artifact)
    manifest_key = store.put_bytes.call_args.args[0]
    assert f"attempts/{attempt.id}/complete.json" in manifest_key


def test_prepared_artifact_lookup_is_profile_scoped() -> None:
    profile = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
    )
    other = SimpleNamespace(specification={"profile_job_id": str(uuid.uuid4())})
    own = SimpleNamespace(specification={"profile_job_id": str(profile.id)})
    db = MagicMock()
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [other, own]
    assert worker._prepared_for_profile(db, profile) is own
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [other]
    with pytest.raises(ValueError, match="sealed splitter artifact"):
        worker._prepared_for_profile(db, profile)


def test_feature_launch_revisions_are_metadata_only_and_metric_bound() -> None:
    project_id = uuid.uuid4()
    version_id = uuid.uuid4()
    profile = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=project_id,
        dataset_version_id=version_id,
        target_column="outcome",
        overview_json={"split_revision_id": str(uuid.uuid4())},
    )
    artifact = SimpleNamespace(
        id=uuid.uuid4(),
        specification={
            "source_digest": "a" * 64,
            "identity": {"split_digests": {"train": "b" * 64}},
        },
    )
    result = {
        "task_inference": {"task_type": "classification"},
        "leakage_analysis": {"excluded_columns": ["outcome_copy"]},
        "columns": [
            {"name": "age", "semantic_type": "numerical_continuous", "quality_flags": []},
            {
                "name": "customer_id",
                "semantic_type": "categorical",
                "quality_flags": ["identifier_like"],
            },
            {"name": "outcome_copy", "semantic_type": "categorical", "quality_flags": []},
            {"name": "outcome", "semantic_type": "categorical", "quality_flags": []},
        ],
    }
    db = MagicMock()

    def assign_ids() -> None:
        for call in db.add_all.call_args_list:
            for row in call.args[0]:
                if getattr(row, "id", None) is None:
                    row.id = uuid.uuid4()

    db.flush.side_effect = assign_ids

    bindings = worker._feature_launch_revisions(db, profile, artifact, result)

    first_rows = db.add_all.call_args_list[0].args[0]
    registry = first_rows[1]
    recipe_rows = db.add_all.call_args_list[1].args[0]
    recipe = recipe_rows[0]
    assert [item["name"] for item in registry.specification["accepted"]] == ["age"]
    assert recipe.specification["materialization"] == "metadata_only"
    assert set(bindings["feature_search_space_revision_ids"]) == set(
        worker.TASK_METRICS["classification"]
    )
    assert bindings["estimator_catalog_revision_id"]


def test_feature_launch_revisions_fail_closed_when_no_safe_inputs() -> None:
    profile = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        target_column="outcome",
        overview_json={"split_revision_id": str(uuid.uuid4())},
    )
    artifact = SimpleNamespace(id=uuid.uuid4(), specification={})
    result = {
        "task_inference": {"task_type": "classification"},
        "leakage_analysis": {"excluded_columns": []},
        "columns": [{"name": "outcome", "semantic_type": "categorical", "quality_flags": []}],
    }
    with pytest.raises(ValueError, match="rejected every"):
        worker._feature_launch_revisions(MagicMock(), profile, artifact, result)


@pytest.mark.parametrize("objects_present", [False, True])
def test_record_failure_is_safe_with_partial_lineage(monkeypatch, objects_present) -> None:
    attempt_id = uuid.uuid4()
    attempt = SimpleNamespace(heartbeat_at=None, terminal_reason=None) if objects_present else None
    profile = (
        SimpleNamespace(
            heartbeat_at=None,
            failure_message=None,
            overview_json={"workflow_attempt_id": str(attempt_id)},
        )
        if objects_present
        else None
    )
    db = MagicMock()
    db.scalar.side_effect = [attempt, profile]
    _session_factory(monkeypatch, db)

    worker._record_failure(uuid.uuid4(), attempt_id, RuntimeError("worker failed"))

    db.commit.assert_called_once()
    if objects_present:
        assert attempt.terminal_reason == "worker failed"
        assert profile.failure_message == "worker failed"


def test_run_stage_dispatches_preparation_and_records_errors(monkeypatch) -> None:
    profile_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    artifact = SimpleNamespace()
    monkeypatch.setattr(worker, "_begin_attempt", lambda *_args: (attempt_id, "fence"))
    monkeypatch.setattr(
        worker,
        "prepare_profile",
        lambda *_args, **_kwargs: ({"columns": []}, artifact),
    )
    persist = MagicMock()
    monkeypatch.setattr(worker, "_persist_profile", persist)
    worker.run_stage(profile_id, WorkflowStage.PREPARATION)
    persist.assert_called_once_with(profile_id, attempt_id, "fence", {"columns": []}, artifact)

    record = MagicMock()
    monkeypatch.setattr(worker, "_record_failure", record)
    with pytest.raises(ValueError, match="Unsupported"):
        worker.run_stage(profile_id, WorkflowStage.TRAINING_RUN)
    record.assert_called_once()


def test_run_stage_dispatches_splitter_and_rejects_missing_source(monkeypatch) -> None:
    profile_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    profile = SimpleNamespace(
        dataset_version_id=uuid.uuid4(), target_column="target", overview_json={}
    )
    version = SimpleNamespace()
    attempt = SimpleNamespace(generation=3)
    monkeypatch.setattr(worker, "_begin_attempt", lambda *_args: (attempt_id, "fence"))
    db = MagicMock()
    db.get.side_effect = [profile, version, attempt]
    _session_factory(monkeypatch, db)
    split = MagicMock(return_value={"result": True})
    persist = MagicMock()
    monkeypatch.setattr(worker, "split_dataset", split)
    monkeypatch.setattr(worker, "_persist_split", persist)

    worker.run_stage(profile_id, WorkflowStage.SPLITTER)

    split.assert_called_once_with(version, profile_id, 3, "target", time_column=None)
    persist.assert_called_once_with(profile_id, attempt_id, "fence", {"result": True})

    db.get.side_effect = [None, None]
    record = MagicMock()
    monkeypatch.setattr(worker, "_record_failure", record)
    with pytest.raises(ValueError, match="source lineage"):
        worker.run_stage(profile_id, WorkflowStage.SPLITTER)
    record.assert_called_once()


def test_csv_time_column_survives_splitter_identity_and_parquet_roundtrip():
    import io

    import pyarrow.csv as csv
    import pyarrow.parquet as parquet

    table = csv.read_csv(io.BytesIO(b"clock,target\n09:30:00,1\n09:30:00,1\n10:45:30,0\n"))
    assert pa.types.is_time32(table.schema.field("clock").type)
    table = table.append_column("id", pa.array([0, 1, 2]))
    first = worker._attach_identity_batch(
        table, columns=["clock", "target"], source_digest="a" * 64, split_seed="seed"
    )
    buffer = io.BytesIO()
    parquet.write_table(table, buffer)
    buffer.seek(0)
    second = worker._attach_identity_batch(
        parquet.read_table(buffer),
        columns=["clock", "target"],
        source_digest="a" * 64,
        split_seed="seed",
    )
    assert first.to_pylist() == second.to_pylist()
    rows = first.to_pylist()
    assert rows[0]["content_fingerprint"] == rows[1]["content_fingerprint"]
    assert rows[0]["split_role"] == rows[1]["split_role"]
    assert rows[0]["content_fingerprint"] != rows[2]["content_fingerprint"]


@pytest.mark.parametrize("current_attempt", [True, False])
def test_invalid_data_fails_current_profile_without_retry(monkeypatch, current_attempt):
    attempt = _dataset_attempt()
    attempt.status = AttemptStatus.RUNNING
    profile, version = _profile_and_version(attempt)
    profile.status = "running"
    profile.overview_json["workflow_attempt_id"] = str(
        attempt.id if current_attempt else uuid.uuid4()
    )
    db = MagicMock()
    db.scalar.side_effect = [attempt, profile]
    db.get.return_value = version
    _session_factory(monkeypatch, db)

    worker._record_failure(profile.id, attempt.id, ValueError("Not enough distinct times"))

    if current_attempt:
        assert attempt.status == AttemptStatus.FAILED
        assert attempt.terminal_cas_version == 1
        assert profile.status == "failed"
        assert profile.finished_at is not None
        assert version.status == "ready"
    else:
        assert attempt.status == AttemptStatus.RUNNING
        assert profile.status == "running"
        db.get.assert_not_called()
    db.commit.assert_called_once()


def test_temporal_split_rejects_missing_observation_times():
    with pytest.raises(ValueError, match="missing values"):
        worker._assign_temporal_split_batch(
            pa.table({"time": [1, None, 3]}),
            time_column="time", validation_start=2, final_start=3,
        )
