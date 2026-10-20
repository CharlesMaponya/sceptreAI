"""API/service and persisted scope boundaries cannot promote safe-copy sources."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import RunStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    EstimatorCatalogRevision,
    ExperimentSpecRevision,
    FeatureContractRevision,
    FeatureSearchSpaceRevision,
    PromotionalScopeMember,
    SearchObjectiveRevision,
)
from automl_api.schemas.contracts import EvaluationScopeCreate
from automl_api.services import (
    champion_planning,
    contracts,
    evaluation_planning,
    training,
    workflow_state,
)
from automl_api.services.dataset_digests import DatasetDigestError
from sqlalchemy import func, select
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


def experiment(db, c):
    common = dict(
        project_id=c.project_id,
        revision=1,
        digest_scope="fixture",
        content_digest="a" * 64,
        specification={},
    )
    contract = FeatureContractRevision(
        **common, name="contract", task_type="regression", target_column="target"
    )
    space = FeatureSearchSpaceRevision(
        **common, name="space", metric_name="rmse", metric_direction="minimize"
    )
    objective = SearchObjectiveRevision(
        **common, name="objective", metric_name="rmse", metric_direction="minimize"
    )
    catalog = EstimatorCatalogRevision(**common, name="catalog", release_version="fixture")
    db.add_all([contract, space, objective, catalog])
    db.flush()
    row = ExperimentSpecRevision(
        **common,
        name="experiment",
        dataset_version_id=c.version_id,
        split_revision_id=c.plan.split_revision_id,
        feature_contract_revision_id=contract.id,
        feature_search_space_revision_id=space.id,
        search_objective_revision_id=objective.id,
        catalog_revision_id=catalog.id,
        task_type="regression",
        target_column="target",
        primary_metric="rmse",
    )
    db.add(row)
    db.flush()
    return row


def payload(c, revision, mode="validation_only"):
    return EvaluationScopeCreate(
        scope_key=uuid.uuid4().hex,
        split_revision_id=c.plan.split_revision_id,
        experiment_spec_revision_id=revision.id,
        canonical_provider="aws",
        expected_member_count=2,
        mode=mode,
        membership_deadline_at=datetime.now(UTC) + timedelta(minutes=5),
        final_threshold_revision="fixture-threshold",
        comparison_policy={"mode": "promotional", "source_kind": "raw"},
    )


def test_scope_service_allows_validation_and_rejects_promotional_creation(source_case):
    c = source_case
    with c.factory() as db, db.begin():
        revision = experiment(db, c)
        actor = db.get(User, c.actor_id)
        scope = contracts.create_scope(db, actor, c.project_id, payload(c, revision))
        assert scope.mode == "validation_only"
        with pytest.raises(DatasetDigestError, match="promotional"):
            contracts.create_scope(db, actor, c.project_id, payload(c, revision, "promotional"))


@pytest.mark.parametrize("entry", ["member", "launch", "refit", "evaluation", "seal"])
def test_legacy_or_forged_promotional_scope_is_rejected_at_each_boundary(
    source_case, monkeypatch, entry
):
    from test_training_service_runtime import _launch_request

    c = source_case
    with c.factory() as db, db.begin():
        revision = experiment(db, c)
        actor = db.get(User, c.actor_id)
        scope = contracts.create_scope(db, actor, c.project_id, payload(c, revision))
        scope.mode = (
            "promotional"  # Simulate a legacy/forged persisted scope, not an allowed API mutation.
        )
        run = ModelRun(
            project_id=c.project_id,
            dataset_version_id=c.version_id,
            created_by_id=c.actor_id,
            status=RunStatus.PRECHECK_RUNNING,
            task_type=TaskType.REGRESSION,
            target_column="target",
            params={
                "mode": "validation_only",
                "source_kind": "raw",
                "split_revision_id": str(c.plan.split_revision_id),
            },
        )
        db.add(run)
        db.flush()
        if entry == "member":
            with pytest.raises(DatasetDigestError, match="promotional"):
                contracts.add_scope_member(db, actor, c.project_id, scope.id, run.id)
            assert db.scalar(select(func.count()).select_from(PromotionalScopeMember)) == 0
        elif entry == "launch":
            monkeypatch.setattr(training, "_lock_training_admission", lambda *_: None)
            monkeypatch.setattr(
                training, "_resolve_estimate_identity", lambda _db, _p, request: request
            )
            monkeypatch.setattr(training, "_resolve_launch_revisions", lambda *_: {"scope": scope})
            request = _launch_request(c.version_id).model_copy(
                update={"promotional_scope_id": scope.id}
            )
            with pytest.raises(DatasetDigestError, match="promotional"):
                training.launch_training_run(
                    db, actor, c.project_id, request, idempotency_key=uuid.uuid4().hex
                )
        elif entry == "refit":
            from automl_api.training.champion_refit import RefitPolicy

            policy = RefitPolicy(
                dataset_version_id=c.version_id,
                target_column="target",
                task_type="regression",
                partitions=[
                    dict(
                        role="train",
                        uri="s3://safe/train.parquet",
                        sha256="a" * 64,
                        byte_size=100,
                        rows=2,
                        row_digest="b" * 64,
                    )
                ],
                max_rows=10,
                max_decoded_bytes=1000000,
                max_model_bytes=1000000,
            )
            scope.comparison_policy = {"refit_policy": policy.model_dump(mode="json")}
            with pytest.raises(DatasetDigestError, match="promotional"):
                champion_planning.validate_refit_policy(db, scope)
        elif entry == "evaluation":
            from automl_api.training.champion_evaluation import EvaluationDataPolicy
            from test_champion_planning import install_evaluation_policy

            policy = install_evaluation_policy(db, scope)
            EvaluationDataPolicy.model_validate(policy)
            with pytest.raises(DatasetDigestError, match="promotional"):
                evaluation_planning.validate_evaluation_policy(db, scope)
        else:
            for ordinal in range(2):
                another = ModelRun(
                    project_id=c.project_id,
                    dataset_version_id=c.version_id,
                    created_by_id=c.actor_id,
                    status=RunStatus.PRECHECK_RUNNING,
                    task_type=TaskType.REGRESSION,
                )
                db.add(another)
                db.flush()
                db.add(
                    PromotionalScopeMember(
                        project_id=c.project_id,
                        scope_id=scope.id,
                        model_run_id=another.id,
                        ordinal=ordinal,
                    )
                )
            db.flush()
            with pytest.raises(DatasetDigestError, match="promotional"):
                workflow_state.seal_promotional_scope(
                    db, scope.id, expected_cas_version=scope.cas_version
                )
