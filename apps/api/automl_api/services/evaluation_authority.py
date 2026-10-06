"""Replay-safe handoff from a durable frozen pipeline to the central authority."""

import hashlib
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import jwt
from pydantic import TypeAdapter
from sqlalchemy import select

from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowCheckpoint
from automl_api.services.evaluation_planning import (
    register_evaluation_plan,
    validate_evaluation_policy,
)
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.final_evaluator_identity import AUDIENCE, ISSUER
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.final_test_credentials import FinalDataManifest
from automl_api.services.workflow_state import InvalidTransition, canonical_request_hash
from automl_api.training.champion_evaluation import EvaluationPlan
from automl_api.training.champion_refit import Digest, FrozenPipeline


def _record(db, attempt, key, payload):
    existing = _event(db, attempt.id, key)
    if existing is None:
        return _append(db, attempt, key, payload)
    if existing.payload != payload or existing.result_digest != canonical_request_hash(payload):
        raise InvalidTransition("Authority handoff differs from its durable intent")
    return existing


def prepare_evaluator(
    session_factory, scope_id, authority, *, authority_public_key, result_public_key, generation=1
):
    """Return a registered plan/token; never open final data or submit a workload.

    The caller supplies a TLS client with allocator credentials, and must preserve
    the private result key corresponding to result_public_key across retries.
    Secret delivery and workload submission happen only after this function.
    """
    if authority.base_url.scheme != "https":
        raise ValueError("Evaluator authority handoff requires HTTPS")
    TypeAdapter(Digest).validate_python(result_public_key)
    with session_factory() as db, db.begin():
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
            raise InvalidTransition("Authority handoff requires a live promotional scope")
        policy, digest = validate_evaluation_policy(db, scope)
        if scope.comparison_policy.get("evaluation_policy_digest") != digest:
            raise InvalidTransition("Evaluator policy was not registered before release")
        frozen = db.execute(
            select(WorkflowAttempt, WorkflowCheckpoint)
            .join(WorkflowCheckpoint, WorkflowCheckpoint.attempt_id == WorkflowAttempt.id)
            .where(
                WorkflowAttempt.scope_id == scope.id,
                WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                WorkflowAttempt.status == AttemptStatus.SUCCEEDED,
            )
        ).one_or_none()
        if frozen is None:
            raise InvalidTransition("Authority handoff requires a published frozen pipeline")
        refit, checkpoint = frozen
        event = _event(db, refit.id, "refit_frozen")
        if event is None:
            raise InvalidTransition("Frozen pipeline evidence is missing")
        output = FrozenPipeline.model_validate(event.payload)
        if (
            checkpoint.object_uri != output.frozen_pipeline_uri
            or checkpoint.content_digest != output.frozen_pipeline_digest
            or event.result_digest != output.frozen_pipeline_digest
        ):
            raise InvalidTransition("Frozen pipeline evidence changed")
        manifest = FinalDataManifest(**policy.final_data.model_dump(), scope_id=scope.id)
        allocation_request = dict(
            split_digest=manifest.split_digest,
            scope_id=str(scope.id),
            canonical_provider=manifest.provider,
            provider_manifest_digest=manifest.digest,
        )
        refit_request = dict(
            refit_attempt_id=str(refit.id),
            frozen_pipeline_digest=output.frozen_pipeline_digest,
            frozen_pipeline_uri=output.frozen_pipeline_uri,
            refit_policy_digest=output.refit_policy_digest,
        )
        deadline = scope.scope_deadline_at
        _record(
            db,
            refit,
            "evaluation_authority_intent"
            if generation == 1
            else f"evaluation_authority_intent_{generation}",
            dict(
                allocation=allocation_request,
                refit=refit_request,
                result_public_key=result_public_key,
                project_reference=manifest.project_reference,
                scope_deadline_at=deadline.isoformat(),
            ),
        )
    response = authority.post("allocations", json=allocation_request)
    response.raise_for_status()
    allocation_id = uuid.UUID(response.json()["allocation_id"])
    path = f"allocations/{allocation_id}"
    response = authority.get(path)
    response.raise_for_status()
    state = response.json()
    if any(state.get(key) != value for key, value in allocation_request.items()) or (
        state.get("allocation_id") != str(allocation_id)
        or state.get("project_reference") != manifest.project_reference
        or state.get("status") != "allocated"
        or state.get("cas_version") != 0
    ):
        raise InvalidTransition("Authority allocation differs or final data has already opened")
    with session_factory() as db, db.begin():
        attempt = register_evaluation_plan(
            db,
            scope_id,
            allocation_id=allocation_id,
            result_public_key=result_public_key,
            generation=generation,
        )
        plan = EvaluationPlan.model_validate(_event(db, attempt.id, "evaluation_plan").payload)
        generation = attempt.generation
        _record(
            db,
            attempt,
            "authority_allocation",
            allocation_request | {"allocation_id": str(allocation_id)},
        )
    response = authority.post(path + "/refit", json=refit_request)
    response.raise_for_status()
    receipt = response.json()
    record = SimpleNamespace(**receipt)
    if (
        record.signature_algorithm != "ed25519"
        or not verify_receipt(record, authority_public_key)
        or str(record.allocation_id) != str(allocation_id)
        or record.operation != "refit_published"
        or record.provider != manifest.provider
        or record.request_digest != canonical_request_hash(refit_request)
        or any(record.payload.get(k) != v for k, v in refit_request.items())
    ):
        raise ValueError("Authority frozen-pipeline receipt is invalid")
    response = authority.post(
        path + "/evaluators",
        json=dict(
            evaluator_attempt_id=str(plan.attempt_id),
            expected_generation=generation - 1,
            scope_deadline_at=deadline.isoformat(),
        ),
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    claims = jwt.decode(
        token,
        authority_public_key,
        algorithms=["EdDSA"],
        audience=AUDIENCE,
        issuer=ISSUER,
        options={"require": ["exp", "iat", "sub"]},
    )
    expected = dict(
        sub=str(plan.attempt_id),
        allocation_id=str(allocation_id),
        scope_id=str(scope_id),
        project_reference=manifest.project_reference,
        provider=manifest.provider,
        evaluator_attempt_id=str(plan.attempt_id),
        frozen_pipeline_digest=plan.frozen_pipeline.sha256,
        generation=generation,
    )
    if any(claims.get(k) != v for k, v in expected.items()) or claims["exp"] > int(
        deadline.timestamp()
    ):
        raise ValueError("Authority evaluator identity has invalid lineage or expiry")
    with session_factory() as db, db.begin():
        attempt = register_evaluation_plan(
            db,
            scope_id,
            allocation_id=allocation_id,
            result_public_key=result_public_key,
            generation=generation,
        )
        _record(
            db,
            attempt,
            "authority_registered",
            dict(
                refit_receipt=receipt,
                claims=claims,
                token_sha256=hashlib.sha256(token.encode()).hexdigest(),
            ),
        )
    return plan, token
