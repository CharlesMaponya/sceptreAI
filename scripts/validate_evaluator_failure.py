"""Kill a synthetic evaluator before or after its final-data grant in isolated qa-* namespaces."""

import argparse
import json
import os
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowCheckpoint
from automl_api.services.evaluation_publication import _event
from automl_api.services.evaluation_reconciler import (
    configured_authority,
    reconcile_evaluation_jobs_once,
)
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.refit_jobs import reconcile_refit_jobs_once
from automl_api.storage.object_store import get_object_store
from automl_api.training.champion_evaluation import EvaluationPlan, verify_result
from kubernetes.client import ApiException
from sqlalchemy import select


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--scope", type=uuid.UUID, required=True)
    parser.add_argument("--stage", choices=["before-open", "after-open"], default="after-open")
    args = parser.parse_args()
    before = args.stage == "before-open"
    expected_attempts = 2 if before else 1
    expected_status = ScopeStatus.SUCCEEDED if before else ScopeStatus.FAILED
    if not args.namespace.startswith("qa-") or os.environ["TRAINING_NAMESPACE"] != args.namespace:
        raise SystemExit("Probe requires matching isolated qa-* namespace")
    original_rules = os.environ["EVALUATION_EGRESS_RULES"]
    rules = json.loads(original_rules)
    if before:
        rules = [
            rule
            for rule in rules
            if not any(
                peer.get("podSelector", {}).get("matchLabels", {}).get("app") == "refit-store"
                for peer in rule["to"]
            )
        ]
        assert len(rules) == len(json.loads(original_rules)) - 1
    else:
        changed = 0
        for rule in rules:
            if any(
                peer.get("namespaceSelector", {})
                .get("matchLabels", {})
                .get("kubernetes.io/metadata.name")
                == "qa-phase1"
                for peer in rule["to"]
            ):
                original = rule["ports"]
                rule["ports"] = [p for p in original if p["port"] != 8334]
                changed += len(original) - len(rule["ports"])
                assert rule["ports"]
        assert changed == 1, "Expected isolated final-store egress; do not broaden this probe"
    os.environ["EVALUATION_EGRESS_RULES"] = json.dumps(rules)
    factory, k8s = get_session_factory(), KubernetesTrainingClient()
    public_key = Path(os.environ["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"]).read_text()
    killed = False
    deadline = time.monotonic() + 420
    while time.monotonic() < deadline:
        reconcile_refit_jobs_once(factory, k8s)
        reconcile_evaluation_jobs_once(factory, k8s)
        with factory() as db:
            scope = db.get(PromotionalScope, args.scope)
            if not before:
                assert scope.status != ScopeStatus.SUCCEEDED, "Fault did not prevent evaluation"
            attempts = list(
                db.scalars(
                    select(WorkflowAttempt)
                    .where(
                        WorkflowAttempt.scope_id == scope.id,
                        WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
                    )
                    .order_by(WorkflowAttempt.generation)
                )
            )
            assert len(attempts) <= expected_attempts, "Unexpected evaluator replacement"
            if not attempts:
                time.sleep(0.2)
                continue
            attempt = attempts[-1]
            plan = EvaluationPlan.model_validate(_event(db, attempt.id, "evaluation_plan").payload)
            with configured_authority(plan.final_manifest.project_reference) as authority:
                response = authority.get(f"allocations/{plan.allocation_id}")
                response.raise_for_status()
                state = response.json()
            operations = [r["operation"] for r in state["receipts"]]
            for receipt in state["receipts"]:
                assert verify_receipt(SimpleNamespace(**receipt), public_key)
            name = f"evaluation-{attempt.id.hex}"
            boundary = (
                before
                and state["status"] == "allocated"
                and attempt.status == AttemptStatus.RUNNING
                or not before
                and state["status"] == "opened"
                and "grant_issued" in operations
            )
            if not killed and boundary:
                pods = k8s.core.list_namespaced_pod(
                    namespace=args.namespace, label_selector=f"job-name={name}"
                ).items
                assert any(p.status.phase == "Running" for p in pods)
                assert _event(db, attempt.id, "evaluation_output") is None
                k8s.batch.delete_namespaced_job(
                    name=name, namespace=args.namespace, propagation_policy="Foreground"
                )
                killed = True
                os.environ["EVALUATION_EGRESS_RULES"] = original_rules
                assert operations.count("grant_issued") == int(not before)
                print(
                    json.dumps(
                        dict(killed_job=name, observed_grants=operations.count("grant_issued"))
                    ),
                    flush=True,
                )
            if scope.status == expected_status and all(
                a.cleanup_state == "complete" for a in attempts
            ):
                assert killed and len(attempts) == expected_attempts
                assert attempts[0].status == AttemptStatus.FAILED
                assert state["status"] == ("committed" if before else "failed")
                assert attempt.generation == expected_attempts
                assert operations.count("grant_issued") == 1
                assert operations.count("fail") == int(not before)
                assert operations.count("commit") == int(before)
                checkpoint = db.scalar(
                    select(WorkflowCheckpoint).where(WorkflowCheckpoint.attempt_id == attempt.id)
                )
                if before:
                    assert attempt.status == AttemptStatus.SUCCEEDED and checkpoint is not None
                    with get_object_store().open_stream(checkpoint.object_uri) as stream:
                        result = verify_result(
                            stream.read(), plan, expected_digest=checkpoint.content_digest
                        )
                    assert result.metrics["rmse"] < 1e-8
                    assert state["result_digest"] == checkpoint.content_digest
                else:
                    assert checkpoint is None
                for old in attempts:
                    name = f"evaluation-{old.id.hex}"
                    assert k8s.job_state(name) == "missing"
                    if old.status == AttemptStatus.FAILED:
                        assert (
                            db.scalar(
                                select(WorkflowCheckpoint).where(
                                    WorkflowCheckpoint.attempt_id == old.id
                                )
                            )
                            is None
                        )
                    for read, resource in (
                        (k8s.core.read_namespaced_secret, name),
                        (
                            k8s.core.read_namespaced_secret,
                            f"evaluation-key-{scope.id.hex}-{old.generation}",
                        ),
                        (k8s.networking.read_namespaced_network_policy, name),
                    ):
                        try:
                            read(name=resource, namespace=args.namespace)
                        except ApiException as error:
                            assert error.status == 404
                        else:
                            raise AssertionError("Evaluator resource remains")
                break
        time.sleep(0.2)
    else:
        raise TimeoutError(f"Inspect scope {args.scope}; do not reseed after timeout")
    for _ in range(2):
        reconcile_evaluation_jobs_once(factory, k8s)
    with configured_authority(plan.final_manifest.project_reference) as authority:
        response = authority.get(f"allocations/{plan.allocation_id}")
        response.raise_for_status()
        assert response.json() == state
    with factory() as db:
        attempts = list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.scope_id == args.scope,
                    WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
                )
            )
        )
        assert len(attempts) == expected_attempts
        assert db.get(PromotionalScope, args.scope).status == expected_status
    print(
        json.dumps(
            dict(
                scope_id=str(args.scope),
                status=str(expected_status),
                stage=args.stage,
                evaluator_attempts=expected_attempts,
                grants=1,
                commits=int(before),
                terminal_replays=2,
                cleanup="complete",
            )
        )
    )


if __name__ == "__main__":
    main()
