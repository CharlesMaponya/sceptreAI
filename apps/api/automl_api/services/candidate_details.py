"""Read recorded candidate evidence without granting execution or storage access."""

from __future__ import annotations

import math
import re
import uuid
from collections import defaultdict

from fastapi import HTTPException
from sqlalchemy import and_, func, or_, select

from automl_api.core.redaction import redact_mapping, redact_text
from automl_api.models.enums import AttemptStatus, RunKind, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowCheckpoint,
    WorkflowEvent,
)
from automl_api.schemas.candidates import (
    CandidateAttemptRead,
    CandidateCheckpointRead,
    CandidateDetailRead,
    CandidateReceiptRead,
    CandidateTrialRead,
)
from automl_api.services.contracts import candidate_for_run
from automl_api.services.workflow_state import canonical_request_hash


def _revision_id(value):
    try:
        return uuid.UUID(str(value)) if value is not None else None
    except ValueError:
        return None


def _receipt(trial, attempt, parent, event):
    if event is None:
        return CandidateReceiptRead(state="missing")
    payload = event.payload
    valid = (
        isinstance(payload, dict)
        and isinstance(trial.parameters, dict)
        and parent is not None
        and parent.stage == WorkflowStage.TRAINING_RUN
        and parent.project_id == attempt.project_id == trial.project_id == event.project_id
        and parent.model_run_id == attempt.model_run_id
        and parent.workload_identity == attempt.workload_identity
        and attempt.logical_key == f"trial:{trial.id}"
        and attempt.status in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}
        and attempt.terminal_cas_version == 1
        and event.event_key == event.event_type == "trial.terminal"
        and payload.get("status") == attempt.status.value == trial.status
        and trial.params_digest == canonical_request_hash(trial.parameters)
        and event.result_digest == trial.result_digest == canonical_request_hash(payload)
        and (
            set(payload) == {"status", "result"} and isinstance(payload.get("result"), dict)
            if attempt.status == AttemptStatus.SUCCEEDED
            else set(payload) == {"status", "failure"}
            and isinstance(payload.get("failure"), str)
            and bool(payload["failure"].strip())
        )
    )
    if not valid:
        return CandidateReceiptRead(state="inconsistent")
    return CandidateReceiptRead(
        state="verified",
        result_digest=event.result_digest,
        result=redact_mapping(payload["result"]) if "result" in payload else None,
        failure=redact_text(payload["failure"]) if "failure" in payload else None,
    )


def get_candidate_detail(db, user, project_id, run_id, candidate_id):
    candidate = candidate_for_run(db, user, project_id, run_id, candidate_id)
    run = db.scalar(
        select(ModelRun).where(ModelRun.id == run_id, ModelRun.project_id == project_id)
    )
    if run is None:
        raise HTTPException(status_code=404, detail="Training run not found.")
    if run.run_kind != RunKind.TRAINING:
        raise HTTPException(status_code=409, detail={"code": "candidate_scope_inconsistent"})
    trials = list(
        db.scalars(
            select(TrainingTrial)
            .where(
                TrainingTrial.project_id == project_id,
                TrainingTrial.candidate_id == candidate_id,
            )
            .order_by(TrainingTrial.suggestion_id, TrainingTrial.id)
        )
    )
    attempts = list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.project_id == project_id,
                WorkflowAttempt.model_run_id == run_id,
                or_(
                    and_(
                        WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
                        WorkflowAttempt.trial_id.is_(None),
                    ),
                    and_(
                        WorkflowAttempt.stage == WorkflowStage.TRAINING_TRIAL,
                        WorkflowAttempt.trial_id.in_([t.id for t in trials]),
                    ),
                ),
            )
            .order_by(WorkflowAttempt.generation, WorkflowAttempt.id)
        )
    )
    by_id = {attempt.id: attempt for attempt in attempts}
    trial_by_id = {trial.id: trial for trial in trials}
    events, checkpoints = defaultdict(list), defaultdict(list)
    if by_id:
        for event in db.scalars(
            select(WorkflowEvent)
            .where(
                WorkflowEvent.project_id == project_id,
                WorkflowEvent.attempt_id.in_(by_id),
                or_(
                    WorkflowEvent.event_key == "trial.terminal",
                    WorkflowEvent.event_type == "trial.dispatch",
                ),
            )
            .order_by(WorkflowEvent.sequence, WorkflowEvent.id)
        ):
            events[event.attempt_id].append(event)
        for checkpoint in db.scalars(
            select(WorkflowCheckpoint)
            .where(
                WorkflowCheckpoint.project_id == project_id,
                WorkflowCheckpoint.attempt_id.in_(by_id),
            )
            .order_by(WorkflowCheckpoint.sequence, WorkflowCheckpoint.id)
        ):
            checkpoints[checkpoint.attempt_id].append(
                CandidateCheckpointRead.model_validate(checkpoint)
            )
    projected, trial_attempts = {}, defaultdict(list)
    for attempt in attempts:
        output = CandidateAttemptRead.model_validate(attempt)
        output.terminal_reason = (
            redact_text(attempt.terminal_reason) if attempt.terminal_reason else None
        )
        for name in ("ray_job_name", "ray_cluster_name", "ray_submission_id"):
            value = getattr(output, name)
            if value is not None:
                setattr(output, name, redact_text(value))
        output.checkpoints = checkpoints[attempt.id]
        for event in events[attempt.id]:
            payload = event.payload
            if (
                event.event_type == "trial.dispatch"
                and isinstance(payload, dict)
                and event.result_digest == canonical_request_hash(payload)
                and isinstance(payload.get("ray_trial_id"), str)
                and event.event_key == f"trial.dispatch:{payload['ray_trial_id']}"
                and attempt.trial_id in trial_by_id
                and payload.get("suggestion_id") == trial_by_id[attempt.trial_id].suggestion_id
            ):
                output.ray_trial_ids.append(redact_text(payload["ray_trial_id"]))
        if attempt.trial_id is not None:
            trial = trial_by_id[attempt.trial_id]
            terminals = [e for e in events[attempt.id] if e.event_key == "trial.terminal"]
            output.receipt = _receipt(
                trial,
                attempt,
                by_id.get(attempt.run_attempt_id),
                terminals[0] if len(terminals) == 1 else None,
            )
            trial_attempts[trial.id].append(output)
        projected[attempt.id] = output
    parameters = run.params if isinstance(run.params, dict) else {}
    lineage = {
        key: _revision_id(parameters.get(key))
        for key in (
            "experiment_spec_revision_id",
            "split_revision_id",
            "feature_contract_revision_id",
            "feature_registry_revision_id",
            "feature_search_space_revision_id",
        )
    }
    lineage.update(
        dataset_version_id=run.dataset_version_id,
        catalog_revision_id=candidate.catalog_revision_id,
        feature_recipe_revision_id=candidate.feature_recipe_revision_id,
    )
    lineage_state = "recorded" if all(lineage.values()) else "incomplete"
    split_digests = {}
    if lineage["split_revision_id"] is not None:
        split = db.scalar(
            select(DatasetSplitRevision).where(
                DatasetSplitRevision.id == lineage["split_revision_id"],
                DatasetSplitRevision.project_id == project_id,
                DatasetSplitRevision.dataset_version_id == run.dataset_version_id,
                DatasetSplitRevision.sealed_at.is_not(None),
            )
        )
        if split is None:
            lineage_state = "inconsistent"
        else:
            split_digests = {
                "train": split.train_digest,
                "validation": split.validation_digest,
                "final_test": split.final_test_digest,
            }
    entries = [
        item
        for item in (run.tags or {}).get("leaderboard", [])
        if isinstance(item, dict)
        and item.get("model") == candidate.estimator_key
        and (item.get("extension_run_id") is None or item.get("extension_run_id") == str(run_id))
    ]
    duplicate_count = db.scalar(
        select(func.count())
        .select_from(TrainingCandidate)
        .where(
            TrainingCandidate.project_id == project_id,
            TrainingCandidate.model_run_id == run_id,
            TrainingCandidate.estimator_key == candidate.estimator_key,
        )
    )
    summary_state = (
        "ambiguous"
        if len(entries) > 1 or duplicate_count != 1
        else "recorded"
        if entries
        else "missing"
    )
    entry = entries[0] if summary_state == "recorded" else {}
    raw_metrics = entry.get("metrics")
    metrics = (
        {
            name: float(value)
            for name, value in redact_mapping(raw_metrics).items()
            if isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        }
        if isinstance(raw_metrics, dict)
        else {}
    )
    digest = entry.get("model_artifact_sha256")
    artifacts = (
        {"model_sha256": digest}
        if isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)
        else {}
    )
    params = redact_mapping(candidate.params) if isinstance(candidate.params, dict) else {}
    return CandidateDetailRead(
        **{
            name: getattr(candidate, name)
            for name in (
                "id",
                "project_id",
                "model_run_id",
                "candidate_key",
                "estimator_key",
                "catalog_revision_id",
                "feature_recipe_revision_id",
                "status",
                "created_at",
                "updated_at",
            )
        },
        params=params,
        parameters=params,
        run_status=run.status.value,
        lineage=lineage,
        lineage_state=lineage_state,
        split_digests=split_digests,
        summary_state=summary_state,
        recorded_status=entry.get("status") if isinstance(entry.get("status"), str) else None,
        metrics=metrics,
        artifact_digests=artifacts,
        terminal_reason=redact_text(entry["error"])
        if isinstance(entry.get("error"), str)
        else None,
        resources={
            "resource_class": params.get("resource_class")
            if isinstance(params.get("resource_class"), str)
            else None,
            **{
                f"run_{name}": getattr(run, name)
                for name in (
                    "cpu_request_cores",
                    "cpu_limit_cores",
                    "memory_request_mb",
                    "memory_limit_mb",
                    "gpu_requested",
                )
            },
        },
        sample_tier={
            key: redact_text(parameters[key])
            if isinstance(parameters.get(key), str)
            else parameters.get(key)
            if isinstance(parameters.get(key), (int, type(None)))
            else None
            for key in (
                "sample_tier_rows",
                "pairwise_sample_rows",
                "validation_sample_rows",
                "sample_tier_policy",
                "sample_tier_order",
            )
        },
        run_attempts=[projected[a.id] for a in attempts if a.trial_id is None],
        trials=[
            CandidateTrialRead(
                id=t.id,
                suggestion_id=t.suggestion_id,
                status=t.status,
                params_digest=t.params_digest,
                parameters=redact_mapping(t.parameters) if isinstance(t.parameters, dict) else {},
                result_digest=t.result_digest,
                attempts=trial_attempts[t.id],
            )
            for t in trials
        ],
    )
