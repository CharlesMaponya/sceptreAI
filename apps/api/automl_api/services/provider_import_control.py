"""Pod-bound control of one protected provider verification execution."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.workflows import DeletionTombstone, WorkflowAttempt
from automl_api.services.attempt_identity import (
    AdmissionScope,
    _admit,
    _scope,
    admit_attempt,
    attempt_audience,
)
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.provider_copy_verification import CopyVerification
from automl_api.services.provider_import import (
    ImportExecution,
    _current,
    _operator,
    _ready,
    _validate,
    fail_provider_import,
    publish_provider_import,
    renew_provider_import,
    verification_receipt_payload,
)
from automl_api.services.provider_import_attempts import (
    verification_attempt,
    verification_identity,
    verification_key,
)
from automl_api.services.provider_source import ProviderVerificationPlan, configured_policy
from automl_api.services.workflow_state import StaleFence, canonical_request_hash

PUBLISHER_EVENT = "provider_import.publisher"


def _receipt_scope(db, attempt_id, *, lock=False, expected_project=None):
    attempt = db.get(WorkflowAttempt, attempt_id, populate_existing=True)
    if attempt is None or attempt.dataset_version_id is None:
        raise StaleFence("Provider verification receipt execution is missing")
    version, row, command = _current(db, attempt.dataset_version_id)
    attempt = db.get(WorkflowAttempt, attempt_id, with_for_update=True, populate_existing=True)
    latest = verification_attempt(db, row)
    policy, key = configured_policy(row.project_id, version.id)
    _, manifest, _ = _validate(version, row, command, policy, key)
    _operator(db, command.actor_id, row.project_id)
    _ready(db, version, row, command, manifest)
    deleting = db.scalar(
        select(DeletionTombstone.id)
        .where(
            DeletionTombstone.project_id == row.project_id,
            DeletionTombstone.resource_type == "project",
        )
        .limit(1)
    )
    publisher = _event(db, attempt_id, PUBLISHER_EVENT)
    receipt = _event(db, attempt_id, "provider_import.receipt")
    if (
        (expected_project is not None and row.project_id != expected_project)
        or attempt.project_id != row.project_id
        or attempt.logical_key != verification_key(row.id)
        or attempt.stage != WorkflowStage.PREPARATION
        or attempt.status != AttemptStatus.SUCCEEDED
        or attempt.workload_identity != verification_identity(attempt.id)
        or attempt.lease_owner is not None
        or attempt.lease_expires_at is not None
        or latest is None
        or latest.id != attempt.id
        or deleting is not None
        or publisher is None
        or receipt is None
        or publisher.event_type != PUBLISHER_EVENT
        or publisher.result_digest != canonical_request_hash(publisher.payload)
        or set(publisher.payload) != {"pod_uid", "binding_digest", "fencing_token"}
        or publisher.payload["fencing_token"] != attempt.fencing_token
        or receipt.event_type != "provider_import.receipt"
        or receipt.result_digest != canonical_request_hash(receipt.payload)
        or receipt.payload
        != verification_receipt_payload(version, row, command, attempt, publisher)
    ):
        raise StaleFence("Provider verification receipt authority changed or is incomplete")
    return AdmissionScope(
        attempt.id,
        row.project_id,
        attempt.generation,
        attempt.fencing_token,
        attempt.workload_identity,
        attempt.ray_job_name,
        get_settings().training_namespace,
        canonical_request_hash(receipt.payload),
    )


def _recover_receipt(factory, attempt_id, token, k8s, proof):
    proof = CopyVerification.model_validate(proof)
    identity = _admit(
        factory,
        attempt_id,
        token,
        k8s,
        _receipt_scope,
        event_type="provider_import.receipt_identity",
    )
    with factory() as db, db.begin():
        _receipt_scope(
            db, attempt_id, lock=True, expected_project=uuid.UUID(identity["project_id"])
        )
        publisher = _event(db, attempt_id, PUBLISHER_EVENT)
        receipt = _event(db, attempt_id, "provider_import.receipt")
        if (
            publisher.payload["pod_uid"] != identity["pod_uid"]
            or receipt.payload["proof_digest"]
            != canonical_request_hash(proof.model_dump(mode="json"))
            or identity["expires_at"] <= int(datetime.now(UTC).timestamp())
        ):
            raise StaleFence("Provider verification receipt belongs to another pod or proof")
        return dict(receipt.payload["response"])


def _authorized(factory, attempt_id, token, k8s, *, start=False):
    identity = admit_attempt(factory, attempt_id, token, k8s)
    with factory() as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id)
        if attempt is None or attempt.dataset_version_id is None:
            raise StaleFence("Provider verification execution is missing")
        # Match publication's project/version/command/attempt lock order.
        version, row, command = _current(db, attempt.dataset_version_id)
        scope = _scope(db, attempt_id, lock=True, expected_project=version.project_id)
        if (
            identity["project_id"] != str(scope.project_id)
            or identity["generation"] != scope.generation
            or identity["audience"] != attempt_audience(scope.id, scope.fencing_token)
            or identity["expires_at"] <= int(datetime.now(UTC).timestamp())
        ):
            raise StaleFence("Provider verification identity changed or expired")
        if not isinstance(scope.source_plan, ProviderVerificationPlan):
            raise StaleFence("This attempt is not a pending provider verifier")
        if _event(db, attempt_id, "attempt.credentials.revoked") is not None:
            raise StaleFence("Provider verification authority was revoked")
        policy, key = configured_policy(scope.project_id, version.id)
        intent, manifest, content = _validate(version, row, command, policy, key)
        binding = {
            "pod_uid": identity["pod_uid"],
            "binding_digest": scope.binding_digest,
            "fencing_token": scope.fencing_token,
        }
        previous = _event(db, attempt_id, PUBLISHER_EVENT)
        if previous is None:
            if not start:
                raise StaleFence("Provider verification was not started by this pod")
            _append(db, attempt, PUBLISHER_EVENT, binding)
        elif (
            previous.event_type != PUBLISHER_EVENT
            or previous.payload != binding
            or previous.result_digest != canonical_request_hash(binding)
        ):
            raise StaleFence("Provider verification publisher binding changed")
        execution = ImportExecution(
            version_id=version.id,
            dataset_id=version.dataset_id,
            command_id=command.id,
            actor_id=command.actor_id,
            import_id=row.id,
            owner=command.lease_owner,
            attempt_id=attempt_id,
            fence=scope.fencing_token,
            intent=intent,
            manifest=manifest,
            content=content,
        )
    return execution, policy, key, identity


def control_provider_import(
    factory, attempt_id, token, k8s, action, *, proof=None, failure_code=None
):
    if action not in {"start", "heartbeat", "result", "receipt", "failure"}:
        raise ValueError("Unsupported verification action")
    if action == "receipt":
        return _recover_receipt(factory, attempt_id, token, k8s, proof)
    if action == "result":
        with factory() as db:
            attempt = db.get(WorkflowAttempt, attempt_id)
            completed = attempt is not None and attempt.status == AttemptStatus.SUCCEEDED
        if completed:
            return _recover_receipt(factory, attempt_id, token, k8s, proof)
    execution, policy, key, identity = _authorized(
        factory, attempt_id, token, k8s, start=action == "start"
    )

    def renew():
        renew_provider_import(
            factory,
            execution,
            policy=policy,
            public_key=key,
            identity_deadline=datetime.fromtimestamp(identity["expires_at"], UTC),
        )

    if action in {"start", "heartbeat"}:
        renew()
        if action == "heartbeat":
            return {"attempt_id": str(attempt_id)}
        return {
            "protocol": "provider-import-verifier-v1",
            "attempt_id": str(attempt_id),
            "project_id": str(policy.project_id),
            "dataset_version_id": str(execution.version_id),
            "fencing_token": execution.fence,
            "manifest": execution.manifest.model_dump(mode="json"),
            "budget": execution.intent.budget.model_dump(mode="json"),
        }
    if action == "failure":
        if failure_code not in {"invalid_copy", "verifier_unavailable"}:
            raise ValueError("Unsupported verification failure")
        fail_provider_import(
            factory,
            execution,
            ValueError("Provider copies failed verification")
            if failure_code == "invalid_copy"
            else RuntimeError("Provider verifier unavailable"),
        )
        return {"attempt_id": str(attempt_id)}

    def check_authority():
        current, _, _, _ = _authorized(factory, attempt_id, token, k8s)
        if current != execution:
            raise StaleFence("Provider verification execution changed during publication")

    return publish_provider_import(
        factory,
        execution,
        proof,
        policy=policy,
        public_key=key,
        check_authority=check_authority,
    )
