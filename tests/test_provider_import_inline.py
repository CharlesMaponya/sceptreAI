"""Canonical signed descriptors live in protected metadata, not a duplicate object."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from automl_api.models.datasets import DatasetProviderImport, DatasetVersion
from automl_api.models.enums import RunKind, RunStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import PreparedArtifact
from automl_api.services import provider_source, training
from automl_api.services.evaluation_publication import _append
from automl_api.services.provider_import import descriptor_bytes
from automl_api.services.provider_import_attempts import verification_attempt
from automl_api.storage import object_store
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_publication import execution as execution  # noqa: F401
from test_provider_import_publication import publish
from test_provider_import_registration import registration_case as registration_case  # noqa: F401
from test_training_service_runtime import _estimate, _request

pytest_plugins = ["test_phase1_postgres_contracts"]


def test_existing_external_descriptor_stays_compatible_without_read_or_rewrite(
    source_case, monkeypatch
):
    c = source_case
    legacy_uri = f"s3c://legacy-owned/projects/{c.project_id}/descriptor.json"
    original = descriptor_bytes(c.value)
    c.store.descriptors[legacy_uri] = original
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, c.version_id)
        artifact = db.get(PreparedArtifact, c.plan.prepared_artifact_id)
        version.object_uri = artifact.object_uri = legacy_uri
    getter = Mock(side_effect=AssertionError("Metadata lookup constructed storage"))
    monkeypatch.setattr(object_store, "get_object_store", getter)
    with c.factory() as db:
        version = db.get(DatasetVersion, c.version_id)
        plan = provider_source.require_prepared_source(db, version)
        assert plan.manifest == c.value
        assert version.object_uri == legacy_uri
    getter.assert_not_called()
    assert c.store.descriptors[legacy_uri] == original and not c.store.writes


def test_logical_descriptor_cannot_be_relabelled_as_another_dataset(source_case):
    c = source_case
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, c.version_id)
        artifact = db.get(PreparedArtifact, c.plan.prepared_artifact_id)
        original = version.object_uri
        version.object_uri = artifact.object_uri = f"provider-import://{uuid.uuid4()}"
    with c.factory() as db:
        with pytest.raises(ValueError):
            provider_source.require_prepared_source(db, db.get(DatasetVersion, c.version_id))
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, c.version_id)
        artifact = db.get(PreparedArtifact, c.plan.prepared_artifact_id)
        version.object_uri = artifact.object_uri = original
        assert provider_source.require_prepared_source(db, version).manifest == c.value


@pytest.mark.parametrize("source_case", ["larger"], indirect=True)
def test_resource_estimate_uses_signed_train_bytes_without_a_descriptor_object(
    source_case, monkeypatch
):
    c = source_case
    getter = Mock(side_effect=AssertionError("Provider estimate checked a native descriptor"))
    monkeypatch.setattr(training, "get_object_store", getter)
    client = Mock()
    client.estimate.side_effect = [_estimate(), _estimate()]
    with c.factory() as db:
        payload = _request(
            dataset_version_id=c.version_id, split_revision_id=c.plan.split_revision_id
        )
        result = training.estimate_analysis_resources(
            db, db.get(User, c.actor_id), c.project_id, payload, client
        )
    assert client.estimate.call_args.kwargs["dataset_bytes"] == sum(
        o.byte_size for o in c.value.safe_data.objects if o.role == "train"
    )
    assert result.can_launch
    assert set(result.sample_tier_summary["split_counts"]) == {"train", "validation"}
    getter.assert_not_called()


def test_restart_passes_verified_provider_metadata_to_launch_without_native_descriptor(
    source_case, monkeypatch
):
    c = source_case
    bindings = {
        key: str(uuid.uuid4())
        for key in (
            "feature_contract_revision_id",
            "feature_registry_revision_id",
            "feature_recipe_revision_id",
            "feature_search_space_revision_id",
            "estimator_catalog_revision_id",
        )
    }
    with c.factory() as db, db.begin():
        run = ModelRun(
            project_id=c.project_id,
            dataset_version_id=c.version_id,
            created_by_id=c.actor_id,
            run_kind=RunKind.TRAINING,
            task_type=TaskType.REGRESSION,
            target_column="target",
            status=RunStatus.FAILED,
            params={
                **bindings,
                "split_revision_id": str(c.plan.split_revision_id),
                "candidate_models": ["Ridge"],
            },
        )
        db.add(run)
        db.flush()
        run_id = run.id
    getter = Mock(side_effect=AssertionError("Provider restart checked a native descriptor"))
    monkeypatch.setattr(training, "get_object_store", getter)
    launch = Mock(side_effect=LookupError("verified provider metadata reached launch"))
    monkeypatch.setattr(training, "launch_training_run", launch)
    with c.factory() as db:
        with pytest.raises(LookupError, match="verified provider metadata reached launch"):
            training._restart_training_run(db, db.get(User, c.actor_id), c.project_id, run_id, None)
    assert launch.call_args.args[3].dataset_version_id == c.version_id
    getter.assert_not_called()


def test_unresolved_old_external_publication_cannot_be_reclassified_as_inline(execution):
    c = execution
    with c.factory() as db, db.begin():
        row = db.get(DatasetProviderImport, c.import_id)
        attempt = verification_attempt(db, row)
        _append(db, attempt, "provider_import.descriptor_requested", {"legacy": True})
    with pytest.raises(ValueError, match="Legacy descriptor publication"):
        publish(c)
    c.factory_calls.assert_not_called()
    assert not c.store.writes


def test_concurrent_metadata_publication_commits_one_set_of_ids_without_storage(execution):
    c = execution
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: publish(c), range(2)))
    assert responses[0] == responses[1]
    assert not c.store.writes
    c.factory_calls.assert_not_called()
