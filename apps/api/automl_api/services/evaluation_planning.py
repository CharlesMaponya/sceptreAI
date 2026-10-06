"""Freeze evaluator data and metric policy before releasing promotional training."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import (
    DatasetSplitRevision,
    ExperimentSpecRevision,
    PromotionalScope,
    WorkflowAttempt,
    WorkflowCheckpoint,
    WorkflowEvent,
)
from automl_api.services.final_test_credentials import FinalDataManifest
from automl_api.services.workflow_state import InvalidTransition, canonical_request_hash
from automl_api.training.champion_evaluation import EvaluationDataPolicy, EvaluationPlan, _final_uri
from automl_api.training.champion_refit import FrozenPipeline, RefitObject, _within


def validate_evaluation_policy(db, scope):
    policy = EvaluationDataPolicy.model_validate(
        (scope.comparison_policy or {}).get("evaluation_policy")
    )
    split = db.get(DatasetSplitRevision, scope.split_revision_id)
    experiment = db.get(ExperimentSpecRevision, scope.experiment_spec_revision_id)
    if (
        split is None
        or experiment is None
        or split.sealed_at is None
        or split.project_id != scope.project_id
        or experiment.project_id != scope.project_id
        or experiment.split_revision_id != split.id
        or experiment.dataset_version_id != split.dataset_version_id
        or experiment.target_column != policy.target_column
        or experiment.task_type != policy.task_type
        or experiment.primary_metric != policy.primary_metric
        or scope.canonical_provider != policy.final_data.provider
        or split.digest_scope != "xor-sha256-row-id-v1"
        or split.content_digest != policy.final_data.split_digest
        or split.final_test_digest != policy.final_row_digest
        or (split.specification.get("split_counts") or {}).get("final_test") != policy.final_rows
    ):
        raise InvalidTransition("Evaluation policy differs from the sealed split and experiment")
    prefixes = split.specification.get("uris") or {}
    for obj in policy.final_data.objects:
        prefix = prefixes.get("final_input" if obj.role == "inputs" else "final_label", "")
        # Compatible S3 and Azure adapters use these persisted URI aliases.
        scheme, separator, path = prefix.partition("://")
        prefix = {"s3c": "s3", "az": "azure"}.get(scheme, scheme) + separator + path
        if not prefix or not _within(_final_uri(policy.final_data, obj), prefix.rstrip("/") + "/"):
            raise InvalidTransition("Evaluation object is outside the sealed final-role prefix")
    digest = canonical_request_hash(
        {
            **policy.model_dump(mode="json"),
            "project_id": str(scope.project_id),
            "scope_id": str(scope.id),
        }
    )
    return policy, digest


def register_evaluation_plan(db, scope_id, *, allocation_id, result_public_key, generation=1):
    """Persist evaluator identity and plan before any credential or Job request.

    Caller commits. Allocation authentication and private-key custody belong to
    the control-plane caller; neither worker nor evaluator can choose this plan.
    Generation two requires a failed initial attempt without output. The caller
    must win the central pre-open generation CAS before submitting that plan.
    """
    if generation not in (1, 2):
        raise InvalidTransition("Evaluator retry budget is exhausted")
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
        raise InvalidTransition("Evaluator registration requires a live promotional scope")
    policy, digest = validate_evaluation_policy(db, scope)
    if scope.comparison_policy.get("evaluation_policy_digest") != digest:
        raise InvalidTransition("Evaluation policy was not registered before scope release")
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
        raise InvalidTransition("Evaluator requires a published frozen pipeline")
    refit, checkpoint = frozen
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == refit.id, WorkflowEvent.event_key == "refit_frozen"
        )
    )
    if event is None:
        raise InvalidTransition("Frozen pipeline publication evidence is missing")
    output = FrozenPipeline.model_validate(event.payload)
    if (
        checkpoint.object_uri != output.frozen_pipeline_uri
        or checkpoint.content_digest != output.frozen_pipeline_digest
        or event.result_digest != output.frozen_pipeline_digest
    ):
        raise InvalidTransition("Frozen pipeline publication evidence changed")
    existing = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.scope_id == scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
        )
        .order_by(WorkflowAttempt.generation.desc())
        .with_for_update()
    )
    previous_plan = None
    if existing is not None and existing.generation != generation:
        if generation != 2 or existing.generation != 1:
            raise InvalidTransition("Evaluator generation is stale")
        validate_replacement(db, scope.id)
        recorded = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == existing.id,
                WorkflowEvent.event_key == "evaluation_plan",
            )
        )
        if recorded is None or recorded.payload.get("allocation_id") != str(allocation_id):
            raise InvalidTransition("Evaluator replacement changed its allocation")
        previous_plan = EvaluationPlan.model_validate(recorded.payload)
        if recorded.result_digest != previous_plan.digest:
            raise InvalidTransition("Original evaluator plan changed")
        existing = None
    elif existing is None and generation != 1:
        raise InvalidTransition("Evaluator replacement requires an initial attempt")
    attempt_id = existing.id if existing is not None else uuid.uuid4()
    plan = EvaluationPlan(
        **policy.model_dump(exclude={"final_data"}),
        final_manifest=FinalDataManifest(**policy.final_data.model_dump(), scope_id=scope.id),
        project_id=scope.project_id,
        scope_id=scope.id,
        attempt_id=attempt_id,
        allocation_id=allocation_id,
        result_public_key=result_public_key,
        frozen_pipeline=RefitObject(
            uri=output.frozen_pipeline_uri,
            sha256=output.frozen_pipeline_digest,
            byte_size=output.byte_size,
        ),
    )
    if previous_plan is not None and previous_plan.model_dump(
        exclude={"attempt_id", "result_public_key"}
    ) != plan.model_dump(exclude={"attempt_id", "result_public_key"}):
        raise InvalidTransition("Evaluator replacement changed the frozen evaluation policy")
    if existing is not None:
        recorded = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == existing.id,
                WorkflowEvent.event_key == "evaluation_plan",
            )
        )
        if (
            recorded is None
            or recorded.payload != plan.model_dump(mode="json")
            or recorded.result_digest != plan.digest
        ):
            raise InvalidTransition("Evaluator registration changed its durable plan")
        return existing
    attempt = WorkflowAttempt(
        id=attempt_id,
        project_id=scope.project_id,
        scope_id=scope.id,
        model_run_id=refit.model_run_id,
        dataset_version_id=refit.dataset_version_id,
        stage=WorkflowStage.CHAMPION_EVALUATION,
        logical_key=f"evaluation:{scope.id}",
        workload_identity=f"evaluation:{attempt_id}",
        generation=generation,
        retry_count=generation - 1,
        fencing_token=uuid.uuid4().hex,
        status=AttemptStatus.PENDING,
        retry_budget=1,
    )
    db.add(attempt)
    db.flush()
    db.add(
        WorkflowEvent(
            project_id=scope.project_id,
            attempt_id=attempt.id,
            sequence=1,
            event_key="evaluation_plan",
            event_type="evaluation_plan",
            payload=plan.model_dump(mode="json"),
            result_digest=plan.digest,
        )
    )
    db.flush()
    return attempt


def validate_replacement(db, scope_id):
    """Caller holds the scope lock; a recorded output always takes recovery priority."""
    first = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.scope_id == scope_id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
            WorkflowAttempt.generation == 1,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        first is None
        or first.status != AttemptStatus.FAILED
        or first.retry_budget < 1
        or db.scalar(
            select(WorkflowEvent.id).where(
                WorkflowEvent.attempt_id == first.id,
                WorkflowEvent.event_key == "evaluation_output",
            )
        )
        is not None
    ):
        raise InvalidTransition("Evaluator replacement requires a failed attempt without output")
    return first
