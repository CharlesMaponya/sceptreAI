"""Declared startup consumes validated control metadata without opening worker SQL."""

import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.services.attempt_identity import attempt_audience
from automl_api.training import pipeline
from automl_api.training.attempt_rpc import AttemptRPC
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_ray_pod_credentials import env, pods
from test_ray_pod_credentials import manifests as manifests
from test_ray_pod_credentials import stage as stage
from test_training_publication_postgres import publication as publication


@pytest.fixture
def remote_context(publication, monkeypatch, tmp_path):
    engine, run_id, attempt_id, fence, _ = publication
    from automl_api.models.runs import ModelRun
    from sqlalchemy.orm import Session

    with Session(engine) as db:
        project_id, version_id = (
            db.get(ModelRun, run_id).project_id,
            db.get(ModelRun, run_id).dataset_version_id,
        )
    split_id = uuid.uuid4()
    reply = {
        "protocol": "training-context-v1",
        "attempt_id": str(attempt_id),
        "project_id": str(project_id),
        "audience": attempt_audience(attempt_id, fence),
        "run": {
            "id": str(run_id),
            "project_id": str(project_id),
            "dataset_version_id": str(version_id),
            "status": "running",
            "task_type": "regression",
            "run_name": "context-test",
            "target_column": "target",
            "params": {"split_revision_id": str(split_id)},
            "tags": {},
        },
        "split": {
            "id": str(split_id),
            "project_id": str(project_id),
            "dataset_version_id": str(version_id),
            "train_uri": "s3://owned/train",
            "validation_uri": "s3://owned/validation",
            "split_counts": {"train": 8, "validation": 2},
            "split_strategy": "hash",
            "time_column": None,
            "row_set_digest": None,
            "provider_import_id": None,
        },
        "approved_columns": ["x", "time", "target"],
        "source_plan": None,
    }
    token = tmp_path / "token"
    token.write_text("fixture-token")
    for key, value in {
        "AUTOML_TRAINING_CONTEXT_PROTOCOL": "training-context-v1",
        "AUTOML_PROJECT_ID": str(project_id),
        "ATTEMPT_CONTROL_BASE_URL": "https://control.example.test/internal/attempts",
        "AUTOML_IDENTITY_TOKEN_FILE": str(token),
    }.items():
        monkeypatch.setenv(key, value)
    sql = MagicMock(side_effect=AssertionError("declared training context opened worker SQL"))
    reader = MagicMock(side_effect=RuntimeError("stop after approved context"))
    tracking = MagicMock(side_effect=AssertionError("tracking started before the reader"))
    monkeypatch.setattr(pipeline, "get_session_factory", sql)
    monkeypatch.setattr(pipeline, "_load_prepared_training_frames", reader)
    monkeypatch.setattr(pipeline, "_tracked_mlflow_run", tracking)

    def request(control, method, path, **kwargs):
        assert control.attempt_id == attempt_id and path == "training/context" and method == "POST"
        return reply

    monkeypatch.setattr(AttemptRPC, "request", request)
    return run_id, reply, sql, reader, tracking


def test_declared_startup_reaches_approved_reader_without_worker_sql(remote_context):
    run_id, _, sql, reader, tracking = remote_context
    with pytest.raises(RuntimeError, match="stop after approved context"):
        pipeline.execute_training_run(run_id)
    sql.assert_not_called()
    assert reader.call_args.kwargs["approved_columns"] == ["x", "time", "target"]
    split = reader.call_args.args[0]
    assert set(split.specification["uris"]) == {"train", "validation"}
    tracking.assert_not_called()


@pytest.mark.parametrize(
    "field",
    [
        "attempt",
        "project",
        "run",
        "audience",
        "protocol",
        "extra",
        "final_role",
        "extra_parameter",
        "source_missing",
        "split_lineage",
        "nonfinite",
    ],
)
def test_invalid_context_never_reaches_reader_or_sql(remote_context, field):
    run_id, reply, sql, reader, tracking = remote_context
    if field == "attempt":
        reply["attempt_id"] = str(uuid.uuid4())
    elif field == "project":
        reply["project_id"] = str(uuid.uuid4())
    elif field == "run":
        reply["run"]["id"] = str(uuid.uuid4())
    elif field == "audience":
        reply["audience"] = "wrong"
    elif field == "protocol":
        reply["protocol"] = "unknown"
    elif field == "extra":
        reply["DATABASE_URL"] = "never-accept"
    elif field == "final_role":
        reply["split"]["final_input"] = "s3://forbidden/final"
    elif field == "extra_parameter":
        reply["run"]["params"]["DATABASE_URL"] = "never-accept"
    elif field == "source_missing":
        reply["split"]["provider_import_id"] = str(uuid.uuid4())
    elif field == "split_lineage":
        reply["split"]["dataset_version_id"] = str(uuid.uuid4())
    else:
        reply["run"]["params"]["sample_tier_rows"] = float("nan")
    with pytest.raises(pipeline.StaleFence):
        pipeline.execute_training_run(run_id)
    sql.assert_not_called()
    reader.assert_not_called()
    tracking.assert_not_called()


@pytest.mark.parametrize("protocol", ["", "unknown", "training-parameters-v1"])
def test_unknown_protocol_never_falls_back_to_sql(remote_context, monkeypatch, protocol):
    monkeypatch.setenv("AUTOML_TRAINING_CONTEXT_PROTOCOL", protocol)
    with pytest.raises(pipeline.StaleFence):
        pipeline.execute_training_run(remote_context[0])
    remote_context[2].assert_not_called()
    remote_context[3].assert_not_called()


def test_missing_parent_cannot_reach_reader_or_sql(remote_context, monkeypatch):
    monkeypatch.delenv("AUTOML_ATTEMPT_ID")
    monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    with pytest.raises(pipeline.StaleFence):
        pipeline.execute_training_run(remote_context[0])
    remote_context[2].assert_not_called()
    remote_context[3].assert_not_called()


def test_unavailable_control_does_not_fall_back_or_leak_details(remote_context, monkeypatch):
    def unavailable(*args, **kwargs):
        raise OSError("https://user:password@unavailable.invalid")

    monkeypatch.setattr(AttemptRPC, "request", unavailable)
    with pytest.raises(pipeline.StaleFence) as error:
        pipeline.execute_training_run(remote_context[0])
    assert "password" not in str(error.value)
    remote_context[2].assert_not_called()
    remote_context[3].assert_not_called()


@pytest.mark.parametrize("configured", [False, True])
def test_v4_selects_context_only_for_configured_training_head(manifests, configured, monkeypatch):
    from dataclasses import replace

    from automl_api.services import reconciler

    build, _, stage, settings = manifests
    settings = replace(
        settings,
        attempt_control_base_url="https://control.example.test/internal/attempts"
        if configured
        else "",
    )
    monkeypatch.setattr(reconciler, "get_settings", lambda: settings)
    submitter, head, worker = pods(build(credential_protocol="ray-training-context-v4"))
    assert "AUTOML_TRAINING_CONTEXT_PROTOCOL" not in env(submitter)
    assert "AUTOML_TRAINING_CONTEXT_PROTOCOL" not in env(worker)
    assert env(head).get("AUTOML_TRAINING_CONTEXT_PROTOCOL") == (
        {"name": "AUTOML_TRAINING_CONTEXT_PROTOCOL", "value": "training-context-v1"}
        if configured and stage == "training_run"
        else None
    )
    for protocol in (None, "ray-pod-env-v1", "ray-parent-control-v2", "ray-trial-control-v3"):
        assert all(
            "AUTOML_TRAINING_CONTEXT_PROTOCOL" not in env(pod)
            for pod in pods(build(credential_protocol=protocol))
        )


def test_terminal_context_cannot_authorize_a_prepared_read(remote_context):
    run_id, reply, sql, reader, _ = remote_context
    reply["run"]["status"] = "succeeded"
    with pytest.raises(pipeline.StaleFence):
        pipeline.execute_training_run(run_id)
    sql.assert_not_called()
    reader.assert_not_called()


@pytest.mark.parametrize(
    "uris",
    [
        {"train": "s3://bucket/train", "validation": "minio://bucket/train"},
        {
            "train": "s3://bucket/train",
            "validation": "s3://bucket/validation",
            "final_target": "s3c://bucket/train",
        },
        {"train": "file:///owned/../final", "validation": "file:///owned/validation"},
    ],
)
def test_shared_role_guard_rejects_aliases_and_traversal(uris):
    from automl_api.services.dataset_digests import require_distinct_training_roles

    with pytest.raises(ValueError):
        require_distinct_training_roles(uris)


def test_shared_role_guard_preserves_distinct_input_locations():
    from automl_api.services.dataset_digests import require_distinct_training_roles

    uris = {
        "train": "s3://bucket/train",
        "validation": "s3://bucket/validation",
        "final_input": "s3://bucket/final",
    }
    before = dict(uris)
    require_distinct_training_roles(uris)
    assert uris == before


@pytest.mark.parametrize(
    "uris",
    [
        {
            "train": "s3://bucket/split",
            "validation": "s3://bucket/validation",
            "final_input": "s3://bucket/split/final_input",
        },
        {
            "train": "s3://bucket/split/train",
            "validation": "s3://bucket/validation",
            "final_input": "s3://bucket/split",
        },
        {"train": "s3://bucket/train", "validation": "s3://bucket/train/validation"},
    ],
)
def test_shared_role_guard_rejects_directory_overlap(uris):
    from automl_api.services.dataset_digests import require_distinct_training_roles

    with pytest.raises(ValueError):
        require_distinct_training_roles(uris)


def test_shared_role_guard_allows_different_names_with_a_common_prefix():
    from automl_api.services.dataset_digests import require_distinct_training_roles

    require_distinct_training_roles(
        {
            "train": "s3://bucket/train",
            "validation": "s3://bucket/training-validation",
            "final_input": "s3://bucket/train-final",
        }
    )


def provider_context_reply(remote_context, *, canonical=False):
    from automl_api.services.final_test_credentials import FinalDataManifest
    from automl_api.services.provider_distribution import ProviderDistribution, final_role_digests
    from automl_api.services.provider_source import ProviderReadPlan
    from test_provider_copy_verification import VersionStore, budget, manifest

    value = manifest(VersionStore())
    if canonical:
        final = FinalDataManifest(
            project_reference=value.project_reference,
            scope_id=value.scope_id,
            split_digest=value.split_digest,
            provider="aws",
            bucket="locked-final-fixture",
            objects=[
                {
                    "role": role,
                    "key": "final/" + role,
                    "version": "v1",
                    "sha256": digest * 64,
                    "byte_size": 1,
                }
                for role, digest in [("inputs", "c"), ("labels", "d")]
            ],
        )
        value = ProviderDistribution.model_validate(
            {
                **value.model_dump(mode="json"),
                "canonical_provider": "aws",
                "mode": "promotional",
                "final_data": final.model_dump(mode="json"),
                "final_role_digests": final_role_digests(final).model_dump(mode="json"),
            }
        )
    reply = remote_context[1]
    import_id = uuid.uuid4()
    plan = ProviderReadPlan(
        project_id=reply["project_id"],
        dataset_version_id=reply["run"]["dataset_version_id"],
        import_id=import_id,
        split_revision_id=reply["split"]["id"],
        prepared_artifact_id=uuid.uuid4(),
        manifest=value,
        budget=budget(),
    )
    reply["source_plan"] = plan.model_dump(mode="json")
    reply["split"].update(
        provider_import_id=str(import_id),
        split_counts={"train": 2, "validation": 2},
        train_uri=f"provider-import:{import_id}:train",
        validation_uri=f"provider-import:{import_id}:validation",
    )
    return plan


def test_safe_provider_plan_reaches_the_prepared_reader(remote_context):
    plan = provider_context_reply(remote_context)
    with pytest.raises(RuntimeError, match="stop after approved context"):
        pipeline.execute_training_run(remote_context[0])
    assert remote_context[3].call_args.kwargs["source_plan"] == plan
    assert plan.manifest.mode == "validation_only" and plan.manifest.final_data is None
    remote_context[2].assert_not_called()


def test_valid_canonical_provider_manifest_never_reaches_the_reader(remote_context):
    from automl_api.schemas.training_context import TrainingContextReply

    plan = provider_context_reply(remote_context, canonical=True)
    assert plan.manifest.mode == "promotional" and plan.manifest.final_data is not None
    with pytest.raises(ValueError, match="cannot disclose final provider objects"):
        TrainingContextReply.model_validate(remote_context[1])
    with pytest.raises(pipeline.StaleFence):
        pipeline.execute_training_run(remote_context[0])
    remote_context[2].assert_not_called()
    remote_context[3].assert_not_called()
