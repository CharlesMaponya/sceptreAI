"""Bounded real local Ray reads, profiling and feature views from signed safe copies."""

import io
import shutil

import numpy as np
import pytest
import ray
import ray.cloudpickle as cloudpickle
from automl_api import preparation_worker
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import AttemptStatus, DatasetStatus, TaskType, WorkflowStage
from automl_api.models.iam import User
from automl_api.models.workflows import DatasetSplitRevision, WorkflowAttempt
from automl_api.schemas.profiling_jobs import ProfilingJobCreate
from automl_api.services import profiling_jobs, provider_reader
from automl_api.services.feature_contracts import validate_compiled_view, validate_contract
from automl_api.training import pipeline
from sklearn.linear_model import Ridge
from sqlalchemy import select
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.mark.parametrize("source_case", ["larger"], indirect=True)
@pytest.mark.parametrize("profile_target", ["target", None])
def test_real_ray_preparation_and_training_read_only_signed_versions(
    source_case, monkeypatch, profile_target
):
    c = source_case
    assert shutil.disk_usage(".").free >= 43_000_000_000
    assert not ray.is_initialized(), "Refuse to replace an existing Ray context"
    objects = dict(c.store.objects)
    for item in c.value.safe_data.objects:
        objects[(item.uri, None)] = b"unapproved current replacement"

    class WorkerStore:
        driver_name = "s3_compatible"

        def open_stream(self, uri, *, version):
            assert version is not None
            return io.BytesIO(objects[(uri, version)])

    worker_store = WorkerStore()
    # Serialize the same reader function with this fixture client into real workers.
    # This verifies data processing, not cloud credentials or IAM.
    monkeypatch.setattr(provider_reader, "provider_grant_reader", lambda *_: worker_store)
    cloudpickle.register_pickle_by_value(provider_reader)
    ray.init(
        address="local",
        num_cpus=2,
        object_store_memory=128 * 1024**2,
        _memory=512 * 1024**2,
        include_dashboard=False,
        log_to_driver=False,
    )
    try:
        with c.factory() as db:
            split = db.get(DatasetSplitRevision, c.plan.split_revision_id)
        train, validation = pipeline._load_prepared_training_frames(
            split,
            source_plan=c.plan,
            task_type=TaskType.REGRESSION,
            approved_columns=["x", "target"],
        )
        assert train.shape == (40, 2) and validation.shape == (20, 2)
        model = Ridge().fit(train[["x"]], train["target"])
        assert np.isfinite(model.predict(validation[["x"]])).all()
        with c.factory() as db, db.begin():
            version = db.get(DatasetVersion, c.version_id)
            actor = db.get(User, c.actor_id)
            job, created = profiling_jobs.create_profiling_job(
                db,
                actor,
                c.project_id,
                version.dataset_id,
                version.id,
                ProfilingJobCreate(target_column=profile_target),
            )
            assert created and job.overview_json["execution_mode"] == "kuberay"
            attempt = db.scalar(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.dataset_version_id == c.version_id,
                    WorkflowAttempt.logical_key == f"profile:{job.id}:preparation",
                )
            )
            assert attempt.stage == WorkflowStage.PREPARATION
            attempt.status = AttemptStatus.SUBMITTED
            job_id, attempt_id, fence = job.id, attempt.id, attempt.fencing_token
        monkeypatch.setattr(preparation_worker, "get_session_factory", lambda: c.factory)
        monkeypatch.setattr(preparation_worker, "get_object_store", lambda: c.store)
        monkeypatch.setattr(preparation_worker, "admit_parent", lambda *_: None)
        monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(attempt_id))
        monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
        preparation_worker.run_stage(job_id, WorkflowStage.PREPARATION)
        with c.factory() as db:
            job = db.get(ProfilingJob, job_id)
            assert job.status == "succeeded" and job.row_count == 40
            assert job.overview_json["validation_row_count"] == 20
            assert db.get(DatasetVersion, c.version_id).status == DatasetStatus.READY
            bindings = job.overview_json["launch_bindings"]
            from automl_api.models.workflows import FeatureContractRevision

            contract = db.get(FeatureContractRevision, bindings["feature_contract_revision_id"])
            validate_contract(
                db,
                c.project_id,
                contract.specification,
                TaskType.REGRESSION if profile_target else TaskType.CLUSTERING,
                profile_target,
            )
            from automl_api.models.workflows import (
                FeatureRecipeRevision,
                FeatureRegistryRevision,
                FeatureSearchSpaceRevision,
            )

            registry = db.get(FeatureRegistryRevision, bindings["feature_registry_revision_id"])
            recipe = db.get(FeatureRecipeRevision, bindings["feature_recipe_revision_id"])
            search = db.get(
                FeatureSearchSpaceRevision,
                next(iter(bindings["feature_search_space_revision_ids"].values())),
            )
            compiled = validate_compiled_view(contract, registry, recipe, search)
            assert compiled.target_column == (profile_target or "")
    finally:
        ray.shutdown()
        cloudpickle.unregister_pickle_by_value(provider_reader)
    assert shutil.disk_usage(".").free >= 43_000_000_000
