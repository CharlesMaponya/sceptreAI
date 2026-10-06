"""Persist each evaluator result key in an immutable Kubernetes Secret."""

import base64
from datetime import UTC, datetime

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kubernetes.client import ApiException
from sqlalchemy import select

from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt
from automl_api.services.evaluation_authority import _record, prepare_evaluator
from automl_api.services.evaluation_planning import validate_evaluation_policy, validate_replacement
from automl_api.services.evaluation_publication import _event
from automl_api.services.workflow_state import InvalidTransition

KEY_FIELD = "result-key.pem"


def _owner(db, scope_id, generation):
    scope = db.scalar(
        select(PromotionalScope)
        .where(PromotionalScope.id == scope_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        scope is None
        or scope.mode != "promotional"
        or scope.status != ScopeStatus.RUNNING
        or scope.scope_deadline_at is None
        or scope.scope_deadline_at <= datetime.now(UTC)
    ):
        raise InvalidTransition("Evaluator key requires a live promotional scope")
    _, digest = validate_evaluation_policy(db, scope)
    if scope.comparison_policy.get("evaluation_policy_digest") != digest:
        raise InvalidTransition("Evaluator key requires a preregistered policy")
    refit = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.scope_id == scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
            WorkflowAttempt.status == AttemptStatus.SUCCEEDED,
        )
        .with_for_update()
    )
    if refit is None or _event(db, refit.id, "refit_frozen") is None:
        raise InvalidTransition("Evaluator key requires a successful refit publication")
    if generation == 2:
        validate_replacement(db, scope_id)
    elif generation != 1:
        raise InvalidTransition("Evaluator retry budget is exhausted")
    return scope, refit


def ensure_evaluator_key(session_factory, scope_id, k8s, *, generation=1):
    """Return Secret name and public key; never recreate a recorded key after loss.

    A replacement has a separate key and intent. It cannot be submitted until
    the central authority accepts the pre-open generation CAS.
    """
    name = f"evaluation-key-{scope_id.hex}-{generation}"
    suffix = "" if generation == 1 else f"_{generation}"
    namespace = k8s.settings.training_namespace
    with session_factory() as db, db.begin():
        scope, refit = _owner(db, scope_id, generation)
        labels = {
            "automl.platform/evaluation-scope": str(scope.id),
            "automl.platform/project-id": str(scope.project_id),
            "automl.platform/evaluation-generation": str(generation),
        }
        intent = {"name": name, "namespace": namespace, "labels": labels, "algorithm": "Ed25519"}
        _record(db, refit, "evaluation_key_intent" + suffix, intent)
        ready = _event(db, refit.id, "evaluation_key_ready" + suffix)
        expected_public = ready.payload["public_key"] if ready is not None else None
    try:
        secret = k8s.core.read_namespaced_secret(name, namespace)
    except ApiException as error:
        if error.status != 404:
            raise
        if expected_public is not None:
            raise InvalidTransition(
                "Recorded evaluator key is missing; refusing to rotate it"
            ) from None
        private_key = Ed25519PrivateKey.generate()
        body = dict(
            apiVersion="v1",
            kind="Secret",
            immutable=True,
            metadata=dict(name=name, namespace=namespace, labels=labels),
            stringData={
                KEY_FIELD: private_key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                ).decode()
            },
        )
        try:
            k8s.core.create_namespaced_secret(namespace, body)
        except ApiException as error:
            if error.status != 409:
                raise
        # Read the authoritative Secret after creation or a concurrent creator's 409.
        secret = k8s.core.read_namespaced_secret(name, namespace)
    if (
        secret.immutable is not True
        or secret.metadata.name != name
        or secret.metadata.namespace != namespace
        or any((secret.metadata.labels or {}).get(k) != v for k, v in labels.items())
    ):
        raise InvalidTransition("Evaluator key Secret has different ownership or is mutable")
    try:
        key = serialization.load_pem_private_key(
            base64.b64decode(secret.data[KEY_FIELD], validate=True), password=None
        )
        if not isinstance(key, Ed25519PrivateKey):
            raise ValueError("Wrong key algorithm")
        public_key = key.public_key().public_bytes_raw().hex()
    except (KeyError, TypeError, ValueError):
        raise InvalidTransition("Evaluator Secret does not contain a valid Ed25519 key") from None
    if expected_public is not None and expected_public != public_key:
        raise InvalidTransition("Recorded evaluator key changed")
    with session_factory() as db, db.begin():
        _, refit = _owner(db, scope_id, generation)
        _record(db, refit, "evaluation_key_ready" + suffix, {**intent, "public_key": public_key})
    return name, public_key


def prepare_evaluator_credentials(
    session_factory, scope_id, k8s, authority, *, authority_public_key, generation=1
):
    """Bind the authority handoff to a recoverable private key; no grant is opened."""
    name, public_key = ensure_evaluator_key(session_factory, scope_id, k8s, generation=generation)
    plan, token = prepare_evaluator(
        session_factory,
        scope_id,
        authority,
        authority_public_key=authority_public_key,
        result_public_key=public_key,
        generation=generation,
    )
    return plan, token, name
