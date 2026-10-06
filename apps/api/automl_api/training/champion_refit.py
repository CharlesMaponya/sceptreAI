"""Refit the selected pipeline against exactly the preregistered prepared rows."""

from __future__ import annotations

import hashlib
import io
import tempfile
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated, Literal
from urllib.parse import unquote, urlsplit

import joblib
import pandas as pd
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sklearn.base import clone

from automl_api.services.temporal import normalize_temporal_features
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.storage.contracts import ObjectStoreDriver

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Positive = Annotated[int, Field(gt=0)]


class RefitObject(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    uri: str
    sha256: Digest
    byte_size: Positive


class RefitPartition(RefitObject):
    role: Literal["train", "validation"]
    rows: Positive
    row_digest: Digest
    row_digest_revision: Literal["xor-sha256-row-id-v1"] = "xor-sha256-row-id-v1"


class RefitPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    dataset_version_id: uuid.UUID
    partitions: Annotated[tuple[RefitPartition, ...], Field(min_length=1)]
    target_column: str
    task_type: Literal["regression", "classification", "time_series"]
    max_decoded_bytes: Positive
    max_rows: Positive
    max_model_bytes: Positive

    @model_validator(mode="after")
    def fixed_inputs(self):
        if not self.target_column or self.target_column in {
            "row_id",
            "source_ordinal",
            "split_role",
            "content_fingerprint",
        }:
            raise ValueError("Refit target must be a data column")
        if "train" not in {p.role for p in self.partitions}:
            raise ValueError("Refit requires preregistered training rows")
        if len({p.uri for p in self.partitions}) != len(self.partitions):
            raise ValueError("Refit partitions must be distinct")
        if sum(p.rows for p in self.partitions) > self.max_rows:
            raise ValueError("Refit exceeds its preregistered row budget")
        return self


class RefitPlan(RefitPolicy):
    project_id: uuid.UUID
    scope_id: uuid.UUID
    attempt_id: uuid.UUID
    run_id: uuid.UUID
    candidate: RefitObject

    @property
    def policy_digest(self):
        return canonical_request_hash(
            self.model_dump(
                mode="json",
                exclude={"attempt_id", "run_id", "candidate"},
            )
        )

    @property
    def digest(self):
        return canonical_request_hash(self.model_dump(mode="json"))


def _within(uri: str, prefix: str) -> bool:
    parts = urlsplit(uri)
    return (
        uri.startswith(prefix)
        and not parts.query
        and not parts.fragment
        and not any(part in {".", ".."} for part in unquote(parts.path).split("/"))
    )


def _copy_verified(store, item, destination, progress):
    """Read no more than the declared bytes; never materialize whole input objects."""
    if store.stat(item.uri).byte_size != item.byte_size:
        raise ValueError("Refit object size differs from its preregistration")
    digest = hashlib.sha256()
    remaining = item.byte_size
    with store.open_stream(item.uri) as source:
        while remaining:
            chunk = source.read(min(remaining, 1024 * 1024))
            if len(chunk) > remaining:
                raise ValueError("Refit source violated its bounded read contract")
            if not chunk:
                raise ValueError("Refit object ended before its declared size")
            remaining -= len(chunk)
            digest.update(chunk)
            destination.write(chunk)
            progress()
        if source.read(1):
            raise ValueError("Refit object exceeds its declared size")
    if digest.hexdigest() != item.sha256:
        raise ValueError("Refit object digest differs from its preregistration")
    destination.seek(0)


def validate_refit_paths(plan, store):
    candidate_prefix = store.uri_for_key(f"projects/{plan.project_id}/runs/{plan.run_id}/")
    if not _within(plan.candidate.uri, candidate_prefix):
        raise ValueError("Refit candidate belongs to another run")
    prepared_prefix = store.uri_for_key(
        f"automl/projects/{plan.project_id}/prepared/{plan.dataset_version_id}/"
    )
    for part in plan.partitions:
        if not _within(part.uri, prepared_prefix) or (
            f"/roles/split_role={part.role}/" not in unquote(urlsplit(part.uri).path)
        ):
            raise ValueError("Refit may read only registered train/validation partitions")
    if plan.candidate.byte_size > plan.max_model_bytes:
        raise ValueError("Candidate exceeds its preregistered model budget")



def execute_refit(
    plan: RefitPlan,
    store: ObjectStoreDriver,
    *,
    progress: Callable[[], None] = lambda: None,
) -> dict[str, str | int]:
    """Execute a bounded sklearn refit; publication still requires a fenced DB CAS.

    The stage identity must independently deny raw and final-data access. URI
    validation here protects lineage; it is not a substitute for that storage IAM.
    Oversized plans fail before fitting instead of silently changing data tiers.
    """
    validate_refit_paths(plan, store)

    with tempfile.TemporaryFile() as candidate_file:
        _copy_verified(store, plan.candidate, candidate_file, progress)
        selected = joblib.load(candidate_file)
    columns = list(getattr(selected, "feature_names_in_", []))
    if not columns or plan.target_column in columns:
        raise ValueError("Selected pipeline lacks a valid frozen input schema")
    model = clone(selected)  # Retain its recipe and hyperparameters; discard fitted state.
    del selected
    frames = []
    seen_rows = set()
    decoded_bytes = 0
    pandas_bytes = 0
    for part in plan.partitions:
        with tempfile.TemporaryFile() as parquet_file:
            _copy_verified(store, part, parquet_file, progress)
            parquet = pq.ParquetFile(parquet_file)
            metadata = parquet.metadata
            decoded_bytes += sum(
                metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups)
            )
            if decoded_bytes > plan.max_decoded_bytes or metadata.num_rows != part.rows:
                raise ValueError(
                    "Refit partition exceeds its registered decoded budget or row count"
                )
            count = 0
            row_xor = 0
            for batch in parquet.iter_batches(batch_size=8192):
                frame = batch.to_pandas()
                pandas_bytes += int(frame.memory_usage(index=True, deep=True).sum())
                if pandas_bytes > plan.max_decoded_bytes:
                    raise ValueError("Refit dataframe exceeds its preregistered memory budget")
                if "row_id" not in frame or frame["row_id"].isna().any():
                    raise ValueError("Refit partition has missing row identities")
                if "split_role" in frame and not frame["split_role"].eq(part.role).all():
                    raise ValueError("Refit partition contains another split role")
                for row_id in frame["row_id"]:
                    if not isinstance(row_id, str) or row_id in seen_rows:
                        raise ValueError("Refit row identities overlap or are invalid")
                    seen_rows.add(row_id)
                    row_xor ^= int.from_bytes(hashlib.sha256(row_id.encode()).digest(), "big")
                count += len(frame)
                frames.append(frame)
                progress()
            if count != part.rows or f"{row_xor:064x}" != part.row_digest:
                raise ValueError("Refit row identities differ from preregistration")
    data = pd.concat(frames, ignore_index=True)
    if plan.task_type == "time_series":
        if "source_ordinal" not in data or data["source_ordinal"].duplicated().any():
            raise ValueError("Temporal refit requires unique source ordinals")
        data = data.sort_values("source_ordinal", kind="stable")
    if plan.target_column not in data or data[plan.target_column].isna().any():
        raise ValueError("Refit target is missing; changing the preregistered row set is forbidden")
    features = normalize_temporal_features(data[columns])
    target = data[plan.target_column]
    progress()
    model.fit(features, target)
    progress()
    result = io.BytesIO()
    joblib.dump(model, result, compress=3)
    payload = result.getvalue()
    if len(payload) > plan.max_model_bytes:
        raise ValueError("Refitted pipeline exceeds its preregistered artifact budget")
    digest = hashlib.sha256(payload).hexdigest()
    key = (
        f"projects/{plan.project_id}/scopes/{plan.scope_id}/"
        f"refit/{plan.attempt_id}/{digest}/pipeline.joblib"
    )
    stored = store.put_bytes(key, payload)
    if stored.uri != store.uri_for_key(key):
        raise ValueError("Refit output store returned a different artifact location")
    return {
        "frozen_pipeline_uri": stored.uri,
        "frozen_pipeline_digest": digest,
        "refit_policy_digest": plan.policy_digest,
        "row_count": len(data),
        "byte_size": len(payload),
    }


class FrozenPipeline(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    frozen_pipeline_uri: str
    frozen_pipeline_digest: Digest
    refit_policy_digest: Digest
    row_count: Positive
    byte_size: Positive


def publish_frozen_pipeline(db, plan: RefitPlan, result: FrozenPipeline, *, fencing_token: str):
    """Publish one scope pipeline and its attempt terminal state in one transaction.

    The caller commits; a lost acknowledgement can replay the identical publication.
    All champion controllers must acquire scope before attempt, in this order.
    """
    from sqlalchemy import func, select

    from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
    from automl_api.models.workflows import (
        PromotionalScope,
        WorkflowAttempt,
        WorkflowCheckpoint,
        WorkflowEvent,
    )
    from automl_api.services.workflow_state import (
        IdempotencyConflict,
        StaleFence,
        cas_register_terminal_artifact,
    )

    scope = db.scalar(
        select(PromotionalScope)
        .where(
            PromotionalScope.id == plan.scope_id,
            PromotionalScope.project_id == plan.project_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.id == plan.attempt_id,
            WorkflowAttempt.project_id == plan.project_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        scope is None
        or attempt is None
        or attempt.scope_id != plan.scope_id
        or attempt.model_run_id != plan.run_id
        or attempt.dataset_version_id != plan.dataset_version_id
        or attempt.generation > 2
        or attempt.stage != WorkflowStage.CHAMPION_REFIT
        or attempt.fencing_token != fencing_token
    ):
        raise StaleFence("Refit publication lineage or fence does not match")
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key == "refit_plan",
        )
    )
    if (
        event is None
        or event.result_digest != plan.digest
        or event.payload != plan.model_dump(mode="json")
    ):
        raise StaleFence("Refit execution differs from its durable plan")
    if result.refit_policy_digest != plan.policy_digest or result.row_count != sum(
        p.rows for p in plan.partitions
    ):
        raise ValueError("Refit result differs from its registered data policy")
    if result.byte_size > plan.max_model_bytes:
        raise ValueError("Refit result exceeds the registered model budget")
    suffix = (
        f"projects/{plan.project_id}/scopes/{plan.scope_id}/refit/{attempt.id}/"
        f"{result.frozen_pipeline_digest}/pipeline.joblib"
    )
    root, separator, _ = plan.candidate.uri.partition(
        f"/projects/{plan.project_id}/runs/{plan.run_id}/"
    )
    if not separator or result.frozen_pipeline_uri != root + "/" + suffix:
        raise ValueError("Frozen pipeline URI does not match its attempt and content digest")
    if (scope.comparison_policy or {}).get("refit_policy_digest") != plan.policy_digest:
        raise StaleFence("Refit data policy was not preregistered for this scope")
    first_plan_event = db.scalar(
        select(WorkflowEvent)
        .join(WorkflowAttempt, WorkflowEvent.attempt_id == WorkflowAttempt.id)
        .where(
            WorkflowAttempt.scope_id == scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
            WorkflowAttempt.generation == 1,
            WorkflowEvent.event_key == "refit_plan",
        )
    )
    if first_plan_event is None:
        raise StaleFence("The original champion selection is missing")
    first_plan = RefitPlan.model_validate(first_plan_event.payload)
    if (first_plan.run_id, first_plan.candidate, first_plan.policy_digest) != (
        plan.run_id,
        plan.candidate,
        plan.policy_digest,
    ):
        raise StaleFence("A refit retry cannot change the champion or data policy")
    existing = db.scalar(
        select(WorkflowCheckpoint)
        .join(
            WorkflowAttempt,
            WorkflowCheckpoint.attempt_id == WorkflowAttempt.id,
        )
        .where(
            WorkflowAttempt.scope_id == scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
            WorkflowAttempt.status == AttemptStatus.SUCCEEDED,
        )
    )
    if existing is not None:
        if (existing.attempt_id, existing.object_uri, existing.content_digest) != (
            attempt.id,
            result.frozen_pipeline_uri,
            result.frozen_pipeline_digest,
        ):
            raise IdempotencyConflict("This scope already published another frozen pipeline")
        return existing
    if scope.status != ScopeStatus.RUNNING or attempt.status != AttemptStatus.RUNNING:
        raise StaleFence("Refit no longer owns a running scope")
    now = datetime.now(UTC)
    if (
        scope.scope_deadline_at is None
        or scope.scope_deadline_at <= now
        or attempt.lease_expires_at is None
        or attempt.lease_expires_at <= now
    ):
        raise StaleFence("Refit lease or scope deadline expired before publication")
    if not cas_register_terminal_artifact(
        db,
        attempt_id=attempt.id,
        fencing_token=fencing_token,
        expected_cas_version=attempt.terminal_cas_version,
        checkpoint_uri=result.frozen_pipeline_uri,
    ):
        raise StaleFence("Refit terminal publication lost its fence")
    event_sequence = 1 + (db.scalar(select(func.max(WorkflowEvent.sequence)).where(
        WorkflowEvent.attempt_id == attempt.id,
    )) or 0)
    checkpoint = WorkflowCheckpoint(
        project_id=plan.project_id,
        attempt_id=attempt.id,
        sequence=1,
        object_uri=result.frozen_pipeline_uri,
        content_digest=result.frozen_pipeline_digest,
        event_sequence=event_sequence,
    )
    db.add(checkpoint)
    db.add(
        WorkflowEvent(
            project_id=plan.project_id,
            attempt_id=attempt.id,
            sequence=event_sequence,
            event_key="refit_frozen",
            event_type="refit_frozen",
            payload=result.model_dump(),
            result_digest=result.frozen_pipeline_digest,
        )
    )
    db.flush()
    return checkpoint


def start_refit_attempt(db, *, project_id, attempt_id, fencing_token):
    """Claim submitted refit work; caller commits before returning its plan."""
    from datetime import timedelta

    from sqlalchemy import select

    from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
    from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
    from automl_api.services.workflow_state import StaleFence, transition_attempt

    scope_id = db.scalar(
        select(WorkflowAttempt.scope_id).where(
            WorkflowAttempt.id == attempt_id,
            WorkflowAttempt.project_id == project_id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
        )
    )
    scope = db.scalar(
        select(PromotionalScope)
        .where(
            PromotionalScope.id == scope_id,
            PromotionalScope.project_id == project_id,
        )
        .with_for_update()
    )
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.id == attempt_id,
            WorkflowAttempt.project_id == project_id,
        )
        .with_for_update()
    )
    now = datetime.now(UTC)
    if (
        scope is None
        or attempt is None
        or scope.status != ScopeStatus.RUNNING
        or scope.scope_deadline_at is None
        or scope.scope_deadline_at <= now
        or attempt.status != AttemptStatus.SUBMITTED
        or attempt.fencing_token != fencing_token
    ):
        raise StaleFence("Refit worker does not own a submitted attempt in a running scope")
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt_id,
            WorkflowEvent.event_key == "refit_plan",
        )
    )
    if event is None:
        raise StaleFence("Refit attempt has no durable execution plan")
    plan = RefitPlan.model_validate(event.payload)
    if (
        (plan.project_id, plan.scope_id, plan.attempt_id, plan.run_id, plan.dataset_version_id)
        != (project_id, scope.id, attempt_id, attempt.model_run_id, attempt.dataset_version_id)
        or plan.digest != event.result_digest
        or scope.comparison_policy.get("refit_policy_digest") != plan.policy_digest
    ):
        raise StaleFence("Refit execution plan does not match its registered lineage")
    deadline = scope.scope_deadline_at
    transition_attempt(attempt, AttemptStatus.RUNNING, fencing_token=fencing_token)
    attempt.heartbeat_at = now
    attempt.lease_expires_at = min(deadline, now + timedelta(seconds=90))
    attempt.lease_owner = f"refit:{attempt_id}"
    return plan


def run_refit_attempt(session_factory, store, *, project_id, attempt_id, fencing_token):
    """Run one submitted attempt; retry ownership remains with the reconciler."""
    from datetime import timedelta
    from threading import Event, Thread

    from sqlalchemy import select, update

    from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
    from automl_api.models.workflows import PromotionalScope, WorkflowAttempt
    from automl_api.services.workflow_state import StaleFence, transition_attempt

    with session_factory() as db, db.begin():
        plan = start_refit_attempt(
            db, project_id=project_id, attempt_id=attempt_id, fencing_token=fencing_token,
        )
        scope_id = plan.scope_id
        deadline = db.get(PromotionalScope, scope_id).scope_deadline_at

    stop = Event()
    lost = Event()

    def heartbeat():
        while not stop.wait(15):
            now = datetime.now(UTC)
            try:
                with session_factory() as db, db.begin():
                    renewed = db.execute(
                        update(WorkflowAttempt)
                        .where(
                            WorkflowAttempt.id == attempt_id,
                            WorkflowAttempt.project_id == project_id,
                            WorkflowAttempt.fencing_token == fencing_token,
                            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                            WorkflowAttempt.status == AttemptStatus.RUNNING,
                            WorkflowAttempt.lease_expires_at > now,
                            PromotionalScope.id == scope_id,
                            PromotionalScope.id == WorkflowAttempt.scope_id,
                            PromotionalScope.status == ScopeStatus.RUNNING,
                            PromotionalScope.scope_deadline_at > now,
                        )
                        .values(
                            heartbeat_at=now,
                            lease_expires_at=min(deadline, now + timedelta(seconds=90)),
                        )
                    )
                    if renewed.rowcount != 1:
                        lost.set()
                        return
            except Exception:
                lost.set()
                return

    def progress():
        if lost.is_set() or datetime.now(UTC) >= deadline:
            raise StaleFence("Refit lease or scope deadline was lost during execution")

    thread = Thread(target=heartbeat, name=f"refit-heartbeat-{attempt_id}", daemon=True)
    thread.start()
    try:
        result = FrozenPipeline.model_validate(execute_refit(plan, store, progress=progress))
        progress()
        with session_factory() as db, db.begin():
            publish_frozen_pipeline(db, plan, result, fencing_token=fencing_token)
        return result
    except Exception as exc:
        with session_factory() as db, db.begin():
            attempt = db.scalar(
                select(WorkflowAttempt)
                .where(
                    WorkflowAttempt.id == attempt_id,
                    WorkflowAttempt.project_id == project_id,
                )
                .with_for_update()
            )
            if (
                attempt is not None
                and attempt.fencing_token == fencing_token
                and attempt.status == AttemptStatus.RUNNING
            ):
                transition_attempt(
                    attempt,
                    AttemptStatus.FAILED,
                    fencing_token=fencing_token,
                    terminal_reason=f"Refit execution failed: {type(exc).__name__}",
                )
        raise
    finally:
        stop.set()
        thread.join(timeout=5)


def main():
    import argparse
    import os

    from automl_api.db.session import get_session_factory
    from automl_api.storage.object_store import get_object_store

    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt-id", required=True, type=uuid.UUID)
    args = parser.parse_args()
    if os.environ.get("AUTOML_WORKFLOW_STAGE") != "champion_refit":
        raise ValueError("This entrypoint requires the champion-refit stage configuration")
    result = run_refit_attempt(
        get_session_factory(),
        get_object_store(),
        project_id=uuid.UUID(os.environ["AUTOML_PROJECT_ID"]),
        attempt_id=args.attempt_id,
        fencing_token=os.environ["AUTOML_FENCING_TOKEN"],
    )
    print(result.model_dump_json())


if __name__ == "__main__":
    main()
