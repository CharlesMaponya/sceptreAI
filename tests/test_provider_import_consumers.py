"""Verified provider source consumers and non-promotional boundaries."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import RunStatus, TaskType, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    PromotionalScope,
    PromotionalScopeMember,
)
from automl_api.services import provider_reader, provider_source
from automl_api.services.dataset_digests import DatasetDigestError
from automl_api.training import pipeline
from cryptography.hazmat.primitives import serialization
from test_provider_import_registration import begin, run
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def source_case(registration_case, tmp_path, monkeypatch, request):
    c = registration_case
    if getattr(request, "param", None) == "larger":
        from automl_api.services.provider_import import ImportReferences
        from automl_shared.data_identity import content_fingerprint
        from test_provider_copy_cli import signed
        from test_provider_copy_verification import manifest, rows

        def values(start, count):
            result = rows(start, count)
            for ordinal, row in enumerate(result, start):
                row.update(x=ordinal % 5, target=float(ordinal * 0.75 + ordinal % 3))
                row["content_fingerprint"] = content_fingerprint(row, ["x", "target"])
            return result

        c.value = manifest(c.store, train=values(0, 40), validation=values(40, 20))
        c.policy = c.policy.model_copy(
            update={
                "references": ImportReferences(
                    **{name: getattr(c.value, name) for name in ImportReferences.model_fields}
                )
            }
        )
        c.receipt = signed(c.value, c.key)
    c.version_id, c.import_id, c.command_id = begin(c)
    assert run(c, c.version_id)
    c.registry = tmp_path / "approved-policies.json"
    c.registry.write_text(json.dumps({"policies": [c.policy.model_dump(mode="json")]}))
    c.public = tmp_path / "authority.pem"
    c.public.write_bytes(
        c.key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    monkeypatch.setenv("PROVIDER_IMPORT_POLICIES_FILE", str(c.registry))
    monkeypatch.setenv("EVALUATION_AUTHORITY_PUBLIC_KEY_FILE", str(c.public))
    with c.factory() as db:
        c.plan = provider_source.require_prepared_source(db, db.get(DatasetVersion, c.version_id))
    return c


def test_approved_source_is_bound_to_exact_prepared_revisions(source_case):
    c = source_case
    with c.factory() as db:
        version = db.get(DatasetVersion, c.version_id)
        plan = provider_source.require_prepared_source(
            db,
            version,
            split_revision_id=c.plan.split_revision_id,
            prepared_artifact_id=c.plan.prepared_artifact_id,
        )
        assert plan.manifest.mode == "validation_only" and plan.manifest.final_data is None
        assert [col["name"] for col in version.schema_json["columns"]] == ["target", "x"]
        for keyword in ("split_revision_id", "prepared_artifact_id"):
            with pytest.raises(DatasetDigestError, match="unapproved"):
                provider_source.require_prepared_source(db, version, **{keyword: uuid.uuid4()})


@pytest.mark.parametrize("fault", ["missing", "duplicate", "key", "reference", "extra"])
def test_unapproved_configuration_never_falls_back_to_raw(source_case, monkeypatch, fault):
    c = source_case
    if fault == "missing":
        monkeypatch.delenv("PROVIDER_IMPORT_POLICIES_FILE")
    elif fault == "duplicate":
        c.registry.write_text(json.dumps({"policies": [c.policy.model_dump(mode="json")] * 2}))
    elif fault == "key":
        c.public.write_text("invalid-key")
    elif fault == "reference":
        data = c.policy.model_dump(mode="json")
        data["references"]["split_digest"] = "0" * 64
        c.registry.write_text(json.dumps({"policies": [data]}))
    else:
        data = c.policy.model_dump(mode="json")
        data["client_override"] = True
        c.registry.write_text(json.dumps({"policies": [data]}))
    with c.factory() as db, pytest.raises(DatasetDigestError):
        provider_source.require_prepared_source(db, db.get(DatasetVersion, c.version_id))


def test_a_forged_raw_kind_cannot_hide_protected_import_provenance(source_case):
    c = source_case
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, c.version_id)
        version.source_kind = "raw"
        db.flush()
        with pytest.raises(DatasetDigestError, match="kind"):
            provider_source.require_prepared_source(db, version)
        with pytest.raises(DatasetDigestError, match="promotional"):
            provider_source.reject_provider_promotion(db, version.id)


@pytest.mark.parametrize("role", ["raw", "final_input", "final_label", "final_test", None])
def test_safe_reader_does_not_resolve_any_other_role(source_case, monkeypatch, role):
    monkeypatch.setattr(
        provider_reader, "provider_grant_reader", lambda *_: pytest.fail("Forbidden storage lookup")
    )
    with pytest.raises(ValueError, match="safe roles"):
        provider_reader.read_provider_role(source_case.plan, role)


def test_actual_versioned_reader_rejects_current_replacement_and_changed_bytes(
    source_case, monkeypatch
):
    c = source_case
    monkeypatch.setattr(provider_reader, "provider_grant_reader", lambda *_: c.store)
    item = c.value.safe_data.objects[0]
    batch = pa.Table.from_pylist([item.model_dump(mode="json")])
    args = dict(budget=c.limits.model_dump(), manifest=c.value.model_dump(mode="json"))
    blocks = list(provider_reader.read_provider_batches(batch, **args))
    assert sum(len(block) for block in blocks) == item.rows
    assert set(blocks[0]["split_role"].to_pylist()) == {"train"}
    c.store.objects[(item.uri, None)] = b"unapproved current replacement"
    assert (
        sum(len(block) for block in provider_reader.read_provider_batches(batch, **args))
        == item.rows
    )
    c.store.objects[(item.uri, item.version)] = b"changed signed version"
    with pytest.raises(ValueError, match="size or SHA-256"):
        list(provider_reader.read_provider_batches(batch, **args))


def test_worker_replay_checks_persisted_membership_not_client_mode_flags(source_case):
    c = source_case
    with c.factory() as db, db.begin():
        run_row = ModelRun(
            project_id=c.project_id,
            dataset_version_id=c.version_id,
            created_by_id=c.actor_id,
            status=RunStatus.QUEUED,
            task_type=TaskType.REGRESSION,
            target_column="target",
            params={
                "split_revision_id": str(c.plan.split_revision_id),
                "mode": "validation_only",
                "source_kind": "raw",
            },
        )
        scope = PromotionalScope(
            project_id=c.project_id,
            scope_key=uuid.uuid4().hex,
            split_revision_id=c.plan.split_revision_id,
            canonical_provider="aws",
            mode="promotional",
            expected_members=1,
            membership_deadline_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        db.add_all([scope, run_row])
        db.flush()
        attempt = SimpleNamespace(stage=WorkflowStage.TRAINING_RUN)
        assert provider_source.validate_attempt_source(db, attempt, run_row)
        db.add(
            PromotionalScopeMember(
                project_id=c.project_id, scope_id=scope.id, model_run_id=run_row.id, ordinal=0
            )
        )
        db.flush()
        with pytest.raises(DatasetDigestError, match="promotional"):
            provider_source.validate_attempt_source(db, attempt, run_row)


def test_import_cannot_receive_a_raw_splitter_stage(source_case):
    c = source_case
    with c.factory() as db:
        for stage in (WorkflowStage.PREPARATION, WorkflowStage.SPLITTER):
            attempt = SimpleNamespace(stage=stage, dataset_version_id=c.version_id)
            if stage == WorkflowStage.SPLITTER:
                with pytest.raises(DatasetDigestError, match="raw splitter"):
                    provider_source.validate_attempt_source(db, attempt)
            else:
                assert provider_source.validate_attempt_source(db, attempt)


def test_pipeline_rejects_unapproved_plan_or_prefix_fallback(source_case, monkeypatch):
    c = source_case
    monkeypatch.setattr(pipeline, "get_object_store", lambda: pytest.fail("Prefix fallback"))
    with c.factory() as db:
        split = db.get(DatasetSplitRevision, c.plan.split_revision_id)
        with pytest.raises(ValueError, match="approved versioned"):
            pipeline._load_prepared_training_frames(split)
        with pytest.raises(ValueError, match="does not belong"):
            pipeline._load_prepared_training_frames(
                split, source_plan=c.plan.model_copy(update={"split_revision_id": uuid.uuid4()})
            )


def test_sampling_seed_uses_signed_logical_identity_not_provider_local_ids(
    source_case, monkeypatch
):
    import pandas as pd

    c = source_case
    seeds = []
    monkeypatch.setattr(
        pipeline,
        "_load_prepared_role",
        lambda *a, **kwargs: seeds.append(kwargs["random_seed"]) or pd.DataFrame(),
    )
    with c.factory() as db:
        original = db.get(DatasetSplitRevision, c.plan.split_revision_id)
    pipeline._load_prepared_training_frames(original, source_plan=c.plan, sample_rows=1)
    project_id, version_id, split_id = (uuid.uuid4() for _ in range(3))
    copy_plan = c.plan.model_copy(
        update={
            "project_id": project_id,
            "dataset_version_id": version_id,
            "split_revision_id": split_id,
        }
    )
    copy_split = SimpleNamespace(
        id=split_id,
        project_id=project_id,
        dataset_version_id=version_id,
        specification=original.specification,
    )
    pipeline._load_prepared_training_frames(copy_split, source_plan=copy_plan, sample_rows=1)
    assert seeds[:2] == seeds[2:]
