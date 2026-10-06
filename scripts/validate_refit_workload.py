"""Run a bounded synthetic refit Job in an explicitly isolated qa-* namespace.

Run inside a test controller Pod with application DB/storage credentials and
namespaced Kubernetes permissions. It creates synthetic fixture rows and objects;
it never substitutes for release-scale or native-cloud qualification.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from threading import Event, Thread

import joblib
import pandas as pd
from automl_api.db.session import get_session_factory
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import (
    AttemptStatus,
    AuthProvider,
    DatasetFormat,
    ObjectStoreType,
    RunStatus,
    ScopeStatus,
    TaskType,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    ExperimentSpecRevision,
    FeatureContractRevision,
    FeatureSearchSpaceRevision,
    PromotionalScope,
    PromotionalScopeMember,
    SearchObjectiveRevision,
    WorkflowAttempt,
    WorkflowCheckpoint,
)
from automl_api.services.champion_planning import plan_scope_refit, validate_refit_policy
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.refit_jobs import reconcile_refit_jobs_once
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.storage.object_store import get_object_store
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import select


def seed(factory, store, *, slow, evaluation_policy=None):
    project_id, dataset_id, run_id, scope_id = (uuid.uuid4() for _ in range(4))
    count = 10000 if slow else 10
    model = Pipeline(
        [
            ("scale", StandardScaler()),
            (
                "model",
                GradientBoostingRegressor(n_estimators=1000, max_depth=2, random_state=7)
                if slow
                else LinearRegression(),
            ),
        ]
    )
    model.fit(pd.DataFrame({"x": range(20)}), [100 * i for i in range(20)])
    serialized = io.BytesIO()
    joblib.dump(model, serialized)
    candidate = serialized.getvalue()
    candidate_object = store.put_bytes(
        f"projects/{project_id}/runs/{run_id}/candidate.joblib", candidate
    )
    parts = []
    for role, numbers in (("train", range(count // 2)), ("validation", range(count // 2, count))):
        ids = [f"refit-fixture-{i}" for i in numbers]
        frame = pd.DataFrame(
            {
                "x": list(numbers),
                "target": [2 * i + 3 for i in numbers],
                "row_id": ids,
                "source_ordinal": list(numbers),
                "split_role": role,
            }
        )
        payload = frame.to_parquet(index=False)
        row_digest = 0
        for row_id in ids:
            row_digest ^= int(hashlib.sha256(row_id.encode()).hexdigest(), 16)
        uri = store.put_bytes(
            f"automl/projects/{project_id}/prepared/{dataset_id}/profiles/synthetic/"
            f"roles/split_role={role}/part.parquet",
            payload,
        ).uri
        parts.append(
            {
                "uri": uri,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "byte_size": len(payload),
                "role": role,
                "rows": len(ids),
                "row_digest": f"{row_digest:064x}",
            }
        )
    with factory() as db, db.begin():
        user = User(
            email=f"refit-{project_id}@example.test",
            full_name="Synthetic refit fixture",
            auth_provider=AuthProvider.SIMPLE,
        )
        db.add(user)
        db.flush()
        db.add(
            Project(id=project_id, owner_id=user.id, created_by_id=user.id, name="Refit fixture")
        )
        db.flush()
        dataset = Dataset(project_id=project_id, created_by_id=user.id, name="Synthetic refit")
        db.add(dataset)
        db.flush()
        db.add(
            DatasetVersion(
                id=dataset_id,
                project_id=project_id,
                dataset_id=dataset.id,
                created_by_id=user.id,
                version_number=1,
                object_uri=parts[0]["uri"],
                content_hash=parts[0]["sha256"],
                format=DatasetFormat.PARQUET,
                object_store_type=ObjectStoreType.S3_COMPATIBLE,
            )
        )
        db.flush()
        common = {
            "project_id": project_id,
            "revision": 1,
            "digest_scope": "synthetic",
            "content_digest": "a" * 64,
            "name": "synthetic",
        }
        split = DatasetSplitRevision(
            project_id=project_id,
            dataset_version_id=dataset_id,
            revision=1,
            digest_scope="xor-sha256-row-id-v1",
            content_digest=canonical_request_hash(parts),
            train_digest=parts[0]["row_digest"],
            validation_digest=parts[1]["row_digest"],
            final_test_digest="0" * 64,
            sealed_at=datetime.now(UTC),
            specification={
                "split_counts": {p["role"]: p["rows"] for p in parts},
                "uris": {p["role"]: p["uri"].rsplit("/", 1)[0] for p in parts},
            },
        )
        contract = FeatureContractRevision(**common, task_type="regression", target_column="target")
        space = FeatureSearchSpaceRevision(
            **common, metric_name="rmse", metric_direction="minimize"
        )
        objective = SearchObjectiveRevision(
            **common, metric_name="rmse", metric_direction="minimize"
        )
        catalog = EstimatorCatalogRevision(**common, release_version="synthetic")
        db.add_all([split, contract, space, objective, catalog])
        db.flush()
        experiment = ExperimentSpecRevision(
            **common,
            dataset_version_id=dataset_id,
            split_revision_id=split.id,
            feature_contract_revision_id=contract.id,
            feature_search_space_revision_id=space.id,
            search_objective_revision_id=objective.id,
            catalog_revision_id=catalog.id,
            task_type="regression",
            target_column="target",
            primary_metric="rmse",
        )
        db.add(experiment)
        db.flush()
        scope = PromotionalScope(
            id=scope_id,
            project_id=project_id,
            split_revision_id=split.id,
            experiment_spec_revision_id=experiment.id,
            scope_key=f"synthetic:{scope_id}",
            canonical_provider="local",
            mode="promotional",
            expected_members=1,
            status=ScopeStatus.SEALED,
            sealed_at=datetime.now(UTC),
            scope_started_at=datetime.now(UTC),
            scope_deadline_at=datetime.now(UTC) + timedelta(minutes=10),
            membership_digest=canonical_request_hash([{"ordinal": 0, "model_run_id": str(run_id)}]),
            comparison_policy={
                "refit_policy": {
                    "dataset_version_id": str(dataset_id),
                    "partitions": parts,
                    "target_column": "target",
                    "task_type": "regression",
                    "max_rows": count,
                    "max_decoded_bytes": 64 * 1024 * 1024,
                    "max_model_bytes": 16 * 1024 * 1024,
                }
            },
        )
        if evaluation_policy is not None:
            from automl_api.services.evaluation_planning import validate_evaluation_policy

            policy = json.loads(json.dumps(evaluation_policy))
            policy["final_data"]["split_digest"] = split.content_digest
            scope.canonical_provider = policy["final_data"]["provider"]
            split.final_test_digest = policy["final_row_digest"]
            final_bucket = policy["final_data"]["bucket"]
            prefixes = {
                "final_input"
                if item["role"] == "inputs"
                else "final_label": f"s3://{final_bucket}/{item['key'].rsplit('/', 1)[0]}"
                for item in policy["final_data"]["objects"]
            }
            split.specification = {
                **split.specification,
                "split_counts": {
                    **split.specification["split_counts"],
                    "final_test": policy["final_rows"],
                },
                "uris": {**split.specification["uris"], **prefixes},
            }
            scope.comparison_policy = {**scope.comparison_policy, "evaluation_policy": policy}
            _, evaluation_digest = validate_evaluation_policy(db, scope)
            scope.comparison_policy = {
                **scope.comparison_policy,
                "evaluation_policy_digest": evaluation_digest,
            }
        _, digest = validate_refit_policy(db, scope)
        scope.comparison_policy = {**scope.comparison_policy, "refit_policy_digest": digest}
        db.add(scope)
        db.add(
            ModelRun(
                id=run_id,
                project_id=project_id,
                dataset_version_id=dataset_id,
                created_by_id=user.id,
                status=RunStatus.SUCCEEDED,
                task_type=TaskType.REGRESSION,
                target_column="target",
                params={"split_revision_id": str(split.id)},
                tags={
                    "leaderboard_primary_metric": "rmse",
                    "leaderboard": [
                        {
                            "model": "synthetic",
                            "status": "succeeded",
                            "metrics": {"rmse": 1.0},
                            "model_artifact_uri": candidate_object.uri,
                            "model_artifact_sha256": hashlib.sha256(candidate).hexdigest(),
                        }
                    ],
                },
            )
        )
        db.flush()
        db.add(
            PromotionalScopeMember(
                project_id=project_id,
                scope_id=scope_id,
                model_run_id=run_id,
                ordinal=0,
                released_at=datetime.now(UTC),
            )
        )
        db.add(
            WorkflowAttempt(
                project_id=project_id,
                model_run_id=run_id,
                stage="training_run",
                logical_key=f"synthetic:{run_id}",
                workload_identity="synthetic",
                generation=1,
                fencing_token=uuid.uuid4().hex,
                status=AttemptStatus.SUCCEEDED,
            )
        )
        db.flush()
        attempt = plan_scope_refit(db, scope.id, store)
        first_id = attempt.id
    return scope_id, first_id, count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument(
        "--case",
        choices=["success", "kill", "lost-reply", "concurrent"],
        default="success",
    )
    args = parser.parse_args()
    k8s = KubernetesTrainingClient()
    if (
        not args.namespace.startswith("qa-")
        or args.namespace != k8s.settings.training_namespace
        or os.environ.get("ENVIRONMENT") != "development"
    ):
        raise ValueError("Synthetic probes require an isolated qa-* development namespace")
    factory, store = get_session_factory(), get_object_store()
    scope_id, first_id, count = seed(factory, store, slow=args.case == "kill")
    if args.case == "lost-reply":
        from kubernetes.client import ApiException

        original = k8s.batch.create_namespaced_job
        lost = False

        def create(*a, **kw):
            nonlocal lost
            result = original(*a, **kw)
            if not lost:
                lost = True
                raise ApiException(status=500, reason="synthetic lost submission reply")
            return result

        k8s.batch.create_namespaced_job = create
    killed = False
    stop = Event()
    concurrent_errors = []
    concurrent_cycles = []
    if args.case == "concurrent":

        def reconcile_again():
            other = KubernetesTrainingClient()
            while not stop.wait(0.25):
                try:
                    reconcile_refit_jobs_once(factory, other)
                    concurrent_cycles.append(True)
                except Exception as exc:
                    concurrent_errors.append(type(exc).__name__)
                    stop.set()

        Thread(target=reconcile_again, daemon=True).start()
    end = time.monotonic() + 240
    while time.monotonic() < end:
        reconcile_refit_jobs_once(factory, k8s)
        if concurrent_errors:
            raise RuntimeError("Concurrent reconciler failed: " + concurrent_errors[0])
        with factory() as db, db.begin():
            attempts = list(
                db.scalars(
                    select(WorkflowAttempt)
                    .where(
                        WorkflowAttempt.scope_id == scope_id,
                    )
                    .order_by(WorkflowAttempt.generation)
                )
            )
            latest = attempts[-1]
            if args.case == "kill" and not killed and latest.status == AttemptStatus.RUNNING:
                k8s.delete_job(f"refit-{latest.id.hex}")
                killed = True
                print(
                    json.dumps({"event": "killed_running_job", "attempt_id": str(latest.id)}),
                    flush=True,
                )
            if latest.status == AttemptStatus.FAILED:
                plan_scope_refit(db, scope_id, store)
            elif latest.status == AttemptStatus.SUCCEEDED:
                checkpoint = db.scalar(
                    select(WorkflowCheckpoint).where(
                        WorkflowCheckpoint.attempt_id == latest.id,
                    )
                )
                if args.case == "kill" and (not killed or len(attempts) != 2):
                    raise RuntimeError("Kill probe did not exercise a replacement")
                if args.case != "kill" and len(attempts) != 1:
                    raise RuntimeError("Submission replay created an unexpected attempt")
                if args.case == "concurrent" and not concurrent_cycles:
                    raise RuntimeError("Second reconciler did not complete an observation cycle")
                payload = store.read_bytes(checkpoint.object_uri)
                assert hashlib.sha256(payload).hexdigest() == checkpoint.content_digest
                fitted = joblib.load(io.BytesIO(payload))
                assert int(fitted.named_steps["scale"].n_samples_seen_) == count
                if args.case != "kill":
                    assert abs(float(fitted.predict(pd.DataFrame({"x": [10]}))[0]) - 23) < 1e-8
                print(
                    json.dumps(
                        {
                            "status": "passed",
                            "production_qualified": False,
                            "scope_id": str(scope_id),
                            "case": args.case,
                            "first_attempt_id": str(first_id),
                            "attempts": len(attempts),
                            "published_attempt_id": str(latest.id),
                            "pipeline_digest": checkpoint.content_digest,
                            "refit_rows": count,
                        }
                    ),
                    flush=True,
                )
                stop.set()
                return
        time.sleep(0.25)
    raise TimeoutError("Synthetic refit did not publish within its bounded probe window")


if __name__ == "__main__":
    main()
