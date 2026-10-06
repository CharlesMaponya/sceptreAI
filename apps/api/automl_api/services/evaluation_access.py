"""Local evaluator control capability, separate from authority and user identities."""

import hashlib
import hmac
import uuid
from datetime import UTC, datetime, timedelta

from automl_api.core.config import get_settings
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.security.tokens import create_signed_token, decode_token
from automl_api.services.evaluation_publication import _event, _locked
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.champion_evaluation import EvaluationPlan


def _key():
    secret = get_settings().jwt_secret_key
    if len(secret) < 32:
        raise ValueError("Evaluator control requires a configured signing key")
    return hmac.new(secret.encode(), b"sceptre-evaluation-control-v1", hashlib.sha256).hexdigest()


def evaluation_token(attempt, deadline):
    remaining = deadline - datetime.now(UTC)
    if not timedelta(0) < remaining <= timedelta(hours=2):
        raise ValueError("Evaluator token requires a live scope deadline")
    return create_signed_token(
        subject=str(attempt.id),
        email="",
        token_version=0,
        secret=_key(),
        token_type="champion_evaluation",
        expires_delta=remaining,
        extra={"project_id": str(attempt.project_id), "fence": attempt.fencing_token},
    )


def evaluation_claims(token, attempt_id):
    claims = decode_token(token, secret=_key(), expected_type="champion_evaluation")
    if claims["sub"] != str(attempt_id):
        raise ValueError("Evaluator capability belongs to another attempt")
    claims["project_id"] = uuid.UUID(claims["project_id"])
    return claims


def locked_evaluation(db, attempt_id, claims, statuses=(AttemptStatus.RUNNING,)):
    event = _event(db, attempt_id, "evaluation_plan")
    if event is None:
        raise StaleFence("Evaluator execution plan is missing")
    plan = EvaluationPlan.model_validate(event.payload)
    if plan.attempt_id != attempt_id or plan.project_id != claims["project_id"]:
        raise StaleFence("Evaluator capability belongs to another plan")
    scope, attempt = _locked(db, plan, claims["fence"], statuses=statuses)
    if _event(db, attempt.id, "evaluation_recovery_started") is not None:
        raise StaleFence("Evaluator is fenced for controller recovery")
    now = datetime.now(UTC)
    if (
        scope.status != ScopeStatus.RUNNING
        or scope.scope_deadline_at is None
        or scope.scope_deadline_at <= now
        or (
            attempt.status == AttemptStatus.RUNNING
            and (attempt.lease_expires_at is None or attempt.lease_expires_at <= now)
        )
    ):
        raise StaleFence("Evaluator scope or lease expired")
    registered = _event(db, attempt.id, "authority_registered")
    if registered is None or registered.result_digest != canonical_request_hash(registered.payload):
        raise StaleFence("Evaluator authority registration is missing or changed")
    identity = registered.payload["claims"]
    if (
        identity["evaluator_attempt_id"] != str(attempt.id)
        or identity["generation"] != attempt.generation
        or identity["frozen_pipeline_digest"] != plan.frozen_pipeline.sha256
        or identity["exp"] <= now.timestamp()
    ):
        raise StaleFence("Evaluator authority identity is stale")
    return scope, attempt, plan
