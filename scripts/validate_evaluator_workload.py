"""Bounded synthetic refit/evaluator lifecycle probe for explicitly isolated qa-* namespaces."""

import argparse
import json
import logging
import os
import time
import uuid
from pathlib import Path

from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowCheckpoint
from automl_api.services.evaluation_planning import validate_evaluation_policy
from automl_api.services.evaluation_publication import _event
from automl_api.services.evaluation_reconciler import (
    configured_authority,
    reconcile_evaluation_jobs_once,
)
from automl_api.services.final_test_credentials import FinalDataManifest
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.refit_jobs import reconcile_refit_jobs_once
from automl_api.storage.object_store import get_object_store
from automl_api.training.champion_evaluation import EvaluationPlan, verify_result
from kubernetes.client import ApiException
from sqlalchemy import select
from validate_refit_workload import seed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["seed", "run"])
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--fixture")
    parser.add_argument("--scope")
    parser.add_argument("--lose-create-reply", action="store_true")
    args = parser.parse_args()
    if (
        not args.namespace.startswith("qa-")
        or os.environ.get("TRAINING_NAMESPACE") != args.namespace
    ):
        raise SystemExit("Probe requires matching isolated qa-* namespace")
    factory, store = get_session_factory(), get_object_store()
    if args.mode == "seed":
        policy = json.loads(Path(args.fixture).read_text())
        scope_id, attempt_id, rows = seed(factory, store, slow=False, evaluation_policy=policy)
        with factory() as db:
            scope = db.get(PromotionalScope, scope_id)
            registered, _ = validate_evaluation_policy(db, scope)
            manifest = FinalDataManifest(**registered.final_data.model_dump(), scope_id=scope_id)
        print(
            json.dumps(
                dict(
                    scope_id=str(scope_id),
                    refit_attempt_id=str(attempt_id),
                    refit_rows=rows,
                    manifest=manifest.model_dump(mode="json"),
                )
            )
        )
        return
    scope_id = uuid.UUID(args.scope)
    k8s = KubernetesTrainingClient()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s scope=%(scope_id)s error=%(error_type)s"))
    logger = logging.getLogger("automl_api.services.evaluation_reconciler")
    logger.addHandler(handler)
    logger.propagate = False
    lost_replies = 0
    create = k8s.batch.create_namespaced_job

    def lose_reply(*args, **kwargs):
        nonlocal lost_replies
        result = create(*args, **kwargs)
        if not lost_replies and kwargs["body"]["metadata"]["name"].startswith("evaluation-"):
            lost_replies += 1
            raise TimeoutError("Synthetic lost evaluator Job creation acknowledgement")
        return result

    if args.lose_create_reply:
        k8s.batch.create_namespaced_job = lose_reply
    deadline = time.monotonic() + 420
    while time.monotonic() < deadline:
        reconcile_refit_jobs_once(factory, k8s)
        reconcile_evaluation_jobs_once(factory, k8s)
        with factory() as db:
            scope = db.get(PromotionalScope, scope_id)
            if scope.status in {ScopeStatus.FAILED, ScopeStatus.CANCELLED}:
                raise RuntimeError("Synthetic evaluator scope failed; inspect durable events")
            if scope.status != ScopeStatus.SUCCEEDED:
                time.sleep(1)
                continue
            attempts = list(
                db.scalars(
                    select(WorkflowAttempt).where(
                        WorkflowAttempt.scope_id == scope_id,
                        WorkflowAttempt.stage.in_(
                            [WorkflowStage.CHAMPION_REFIT, WorkflowStage.CHAMPION_EVALUATION]
                        ),
                    )
                )
            )
            if any(a.cleanup_state != "complete" for a in attempts):
                time.sleep(1)
                continue
            assert len(attempts) == 2 and all(a.status == AttemptStatus.SUCCEEDED for a in attempts)
            evaluator = next(a for a in attempts if a.stage == WorkflowStage.CHAMPION_EVALUATION)
            plan = EvaluationPlan.model_validate(
                _event(db, evaluator.id, "evaluation_plan").payload
            )
            checkpoint = db.scalar(
                select(WorkflowCheckpoint).where(WorkflowCheckpoint.attempt_id == evaluator.id)
            )
            with store.open_stream(checkpoint.object_uri) as stream:
                result = verify_result(
                    stream.read(), plan, expected_digest=checkpoint.content_digest
                )
            assert result.metrics["rmse"] < 1e-8
            with configured_authority(plan.final_manifest.project_reference) as authority:
                response = authority.get(f"allocations/{plan.allocation_id}")
                response.raise_for_status()
                state = response.json()
            assert (
                state["status"] == "committed"
                and state["result_digest"] == checkpoint.content_digest
            )
            operations = [r["operation"] for r in state["receipts"]]
            assert operations.count("grant_issued") == 1 and operations.count("commit") == 1
            for attempt in attempts:
                prefix = "evaluation" if attempt.id == evaluator.id else "refit"
                name = f"{prefix}-{attempt.id.hex}"
                assert k8s.job_state(name) == "missing"
                for read, resource in (
                    (k8s.core.read_namespaced_secret, name),
                    (k8s.networking.read_namespaced_network_policy, name),
                ):
                    try:
                        read(name=resource, namespace=args.namespace)
                    except ApiException as error:
                        assert error.status == 404
                    else:
                        raise AssertionError("Completed workload resource was not removed")
            try:
                k8s.core.read_namespaced_secret(
                    name=f"evaluation-key-{scope.id.hex}-{evaluator.generation}",
                    namespace=args.namespace,
                )
            except ApiException as error:
                assert error.status == 404
            else:
                raise AssertionError("Evaluator signing key was not removed")
            assert lost_replies == int(args.lose_create_reply)
            print(
                json.dumps(
                    dict(
                        scope_id=str(scope.id),
                        status=str(scope.status),
                        rows=result.final_rows,
                        rmse=result.metrics["rmse"],
                        grants=operations.count("grant_issued"),
                        attempts=len(attempts),
                        lost_create_replies=lost_replies,
                        result_sha256=checkpoint.content_digest,
                        cleanup="complete",
                    )
                )
            )
            return
    raise TimeoutError(
        f"Synthetic scope {scope_id} still pending; resume same scope, never reseed on timeout"
    )


if __name__ == "__main__":
    main()
