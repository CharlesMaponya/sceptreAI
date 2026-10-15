"""Recover a real QA evaluator after a commit fault has terminated its worker."""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

from automl_api.db.session import get_session_factory
from automl_api.models.enums import ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt
from automl_api.services import evaluation_jobs, refit_jobs
from automl_api.services.evaluation_publication import _event, _read_result
from automl_api.services.evaluation_reconciler import (
    configured_authority,
    reconcile_evaluation_jobs_once,
)
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.storage.object_store import get_object_store
from automl_api.training.champion_evaluation import EvaluationPlan
from sqlalchemy import select
from validate_evaluator_workload import main as verify_workload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--scope", type=uuid.UUID, required=True)
    parser.add_argument("--mode", choices=["before", "after"], required=True)
    args = parser.parse_args()
    if not args.namespace.startswith("qa-") or os.environ["TRAINING_NAMESPACE"] != args.namespace:
        raise SystemExit("Probe requires matching isolated qa-* namespace")
    factory, k8s = get_session_factory(), KubernetesTrainingClient()
    original_refit, original_evaluation = refit_jobs.submission, evaluation_jobs.submission

    def pin(desired):
        desired["job"]["spec"]["template"]["spec"]["nodeSelector"] = {
            "kubernetes.io/hostname": "k3d-sceptre-server-0"
        }
        return desired

    def fault_submission(*a, **kw):
        desired = pin(original_evaluation(*a, **kw))
        env = desired["job"]["spec"]["template"]["spec"]["containers"][0]["env"]
        entry = next(e for e in env if e["name"] == "EVALUATION_AUTHORITY_URL")
        entry["value"] = f"https://evaluation-commit-fault.{args.namespace}.svc:8443"
        desired["network"]["spec"]["egress"].append(
            {
                "to": [{"podSelector": {"matchLabels": {"app": "evaluation-commit-fault"}}}],
                "ports": [{"port": 8443, "protocol": "TCP"}],
            }
        )
        return desired

    refit_jobs.submission = lambda *a, **kw: pin(original_refit(*a, **kw))
    evaluation_jobs.submission = fault_submission
    deadline = time.monotonic() + 420
    try:
        submitted = False
        while time.monotonic() < deadline:
            with factory() as db:
                scope = db.get(PromotionalScope, args.scope)
                assert scope.status in {ScopeStatus.SEALED, ScopeStatus.RUNNING}
                attempts = list(
                    db.scalars(
                        select(WorkflowAttempt).where(
                            WorkflowAttempt.scope_id == args.scope,
                            WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
                        )
                    )
                )
                assert len(attempts) <= 1, "Fault probe must not create a replacement"
                if attempts:
                    attempt = attempts[0]
                    name = f"evaluation-{attempt.id.hex}"
                    state = k8s.job_state(name)
                    submitted = state != "missing"
                    if state == "failed":
                        assert scope.status == ScopeStatus.RUNNING
                        pods = k8s.core.list_namespaced_pod(
                            namespace=args.namespace, label_selector=f"job-name={name}"
                        ).items
                        assert len(pods) == 1, "Fault must terminate exactly one worker"
                        statuses = pods[0].status.container_statuses
                        assert len(statuses) == 1
                        terminated = statuses[0].state.terminated
                        assert terminated and terminated.exit_code == 1
                        log = k8s.core.read_namespaced_pod_log(
                            name=pods[0].metadata.name, namespace=args.namespace
                        )
                        assert "Evaluator worker failed: RemoteProtocolError" in log
                        output = _event(db, attempt.id, "evaluation_output")
                        assert output is not None, "Worker failed before persisting its result"
                        assert _event(db, attempt.id, "evaluation_committed") is None
                        plan = EvaluationPlan.model_validate(
                            _event(db, attempt.id, "evaluation_plan").payload
                        )
                        _read_result(get_object_store(), output.payload, plan)
                        break
            if not submitted:
                refit_jobs.reconcile_refit_jobs_once(factory, k8s)
                reconcile_evaluation_jobs_once(factory, k8s)
            time.sleep(0.2)
        else:
            raise TimeoutError(f"Inspect scope {args.scope}; do not reseed after timeout")
    finally:
        refit_jobs.submission, evaluation_jobs.submission = original_refit, original_evaluation
    key = Path(os.environ["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"]).read_text()
    with configured_authority(plan.final_manifest.project_reference) as authority:
        response = authority.get(f"allocations/{plan.allocation_id}")
        response.raise_for_status()
        before = response.json()
    operations = [r["operation"] for r in before["receipts"]]
    assert operations.count("grant_issued") == 1
    assert operations.count("commit") == int(args.mode == "after")
    assert before["status"] == ("committed" if args.mode == "after" else "opened")
    assert all(verify_receipt(SimpleNamespace(**r), key) for r in before["receipts"])
    print(
        json.dumps(
            {
                "scope": str(args.scope),
                "fault_mode": args.mode,
                "failed_job": name,
                "stored_result_verified": True,
                "authority_status_before_recovery": before["status"],
            }
        ),
        flush=True,
    )
    sys.argv = [
        "validate_evaluator_workload",
        "run",
        "--namespace",
        args.namespace,
        "--scope",
        str(args.scope),
    ]
    verify_workload()
    for _ in range(2):
        verify_workload()


if __name__ == "__main__":
    main()
