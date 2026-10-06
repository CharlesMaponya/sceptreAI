from __future__ import annotations

import hashlib
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowCheckpoint, WorkflowEvent
from automl_api.qualification_control import receipt_response
from automl_api.services import evaluation_publication as publication
from automl_api.services import final_test_authority as authority
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.training.champion_evaluation import execute_evaluation
from automl_api.training.champion_refit import publish_frozen_pipeline
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import func, select
from sqlalchemy.orm import Session

pytest_plugins = ["test_champion_refit_publication", "test_champion_evaluation_execution"]


@pytest.fixture
def publication_case(durable_refit, refit_case, evaluation_case):
    db, refit_plan, frozen, refit_attempt, scope = durable_refit
    output_store, _ = refit_case
    publish_frozen_pipeline(db, refit_plan, frozen, fencing_token=refit_attempt.fencing_token)
    plan, signer, _, objects = evaluation_case()
    plan = plan.model_copy(
        update={
            "project_id": scope.project_id,
            "scope_id": scope.id,
            "frozen_pipeline": plan.frozen_pipeline.model_copy(
                update={
                    "uri": frozen.frozen_pipeline_uri,
                    "sha256": frozen.frozen_pipeline_digest,
                    "byte_size": frozen.byte_size,
                }
            ),
            "final_manifest": plan.final_manifest.model_copy(update={"scope_id": scope.id}),
        }
    )
    final_store = Mock()
    import io

    from automl_api.storage.contracts import ObjectMetadata

    final_store.stat.side_effect = lambda uri: ObjectMetadata(uri=uri, byte_size=len(objects[uri]))
    final_store.open_stream.side_effect = lambda uri: io.BytesIO(objects[uri])
    allocation = authority.allocate(
        db,
        project_reference="fixture",
        scope_id=scope.id,
        canonical_provider="aws",
        provider_manifest_digest=plan.final_manifest.digest,
        split_digest=uuid.uuid4().hex * 2,
    )
    plan = plan.model_copy(update={"allocation_id": allocation.id})
    payload = execute_evaluation(plan, output_store, final_store, signing_key=signer)
    attempt = WorkflowAttempt(
        id=plan.attempt_id,
        project_id=plan.project_id,
        scope_id=scope.id,
        stage="champion_evaluation",
        logical_key=f"evaluate:{scope.id}",
        generation=1,
        fencing_token=uuid.uuid4().hex,
        workload_identity="evaluator",
        status=AttemptStatus.RUNNING,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    db.add(attempt)
    db.flush()
    db.add(
        WorkflowEvent(
            project_id=plan.project_id,
            attempt_id=attempt.id,
            sequence=1,
            event_key="evaluation_plan",
            event_type="evaluation_plan",
            payload=plan.model_dump(mode="json"),
            result_digest=plan.digest,
        )
    )
    authority_key = Ed25519PrivateKey.generate()
    binding = dict(
        evaluator_attempt_id=str(attempt.id), frozen_pipeline_digest=plan.frozen_pipeline.sha256
    )
    authority.open_allocation(
        db,
        allocation_id=allocation.id,
        provider="aws",
        provider_manifest_digest=plan.final_manifest.digest,
        request_digest="1" * 64,
        signing_secret=authority_key,
        **binding,
    )
    db.flush()

    @contextmanager
    def factory():
        with Session(bind=db.get_bind(), join_transaction_mode="create_savepoint") as session:
            yield session

    def commit(digest):
        with factory() as session, session.begin():
            receipt = authority.commit_result(
                session,
                allocation_id=allocation.id,
                provider="aws",
                provider_manifest_digest=plan.final_manifest.digest,
                result_digest=digest,
                request_digest=canonical_request_hash({"result_digest": digest}),
                signing_secret=authority_key,
                **binding,
            )
            return receipt_response(receipt)

    return (
        db,
        plan,
        attempt,
        scope,
        payload,
        output_store,
        factory,
        Mock(side_effect=commit),
        authority_key,
    )


def recover(case, **overrides):
    _, plan, attempt, _, _, store, factory, commit, key = case
    return publication.recover_evaluation_result(
        factory,
        plan,
        attempt.fencing_token,
        store,
        commit=overrides.get("commit", commit),
        authority_public_key=key.public_key(),
    )


def persist(case):
    _, plan, attempt, _, payload, store, factory, _, _ = case
    return publication.persist_evaluation_result(
        factory, plan, attempt.fencing_token, payload, store
    )


def test_intent_precedes_storage_and_lost_reply_recovers_once(publication_case, monkeypatch):
    db, plan, attempt, scope, _, store, factory, commit, _ = publication_case
    real_put = store.put_bytes

    def lost_reply(key, payload):
        assert not db.get_bind().in_nested_transaction()
        with factory() as session:
            intent = publication._event(session, attempt.id, "evaluation_output")
            assert intent.payload["sha256"] == hashlib.sha256(payload).hexdigest()
        real_put(key, payload)
        raise ConnectionError("Lost object write acknowledgement")

    monkeypatch.setattr(store, "put_bytes", lost_reply)
    with pytest.raises(ConnectionError):
        persist(publication_case)
    commit.assert_not_called()
    # Recovery is allowed after worker death/lease expiry, using only its stored result.
    attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    first = recover(publication_case)
    assert recover(publication_case) == first
    db.refresh(scope)
    db.refresh(attempt)
    assert scope.status == ScopeStatus.SUCCEEDED and attempt.status == AttemptStatus.SUCCEEDED
    assert attempt.terminal_cas_version == 1
    assert (
        db.scalar(
            select(func.count())
            .select_from(WorkflowCheckpoint)
            .where(WorkflowCheckpoint.attempt_id == attempt.id)
        )
        == 1
    )


def test_authority_commit_ack_loss_replays_without_rewriting_result(publication_case):
    persist(publication_case)
    commit = publication_case[-2]

    def lost(digest):
        commit(digest)
        raise ConnectionError("Lost authority response")

    with pytest.raises(ConnectionError):
        recover(publication_case, commit=lost)
    db, _, attempt, scope, *_ = publication_case
    db.refresh(scope)
    assert scope.status == ScopeStatus.RUNNING
    recover(publication_case)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.SUCCEEDED


@pytest.mark.parametrize("fault", ["corrupt", "missing", "cancel", "fence", "plan"])
def test_invalid_or_stale_publication_never_commits(publication_case, fault, monkeypatch):
    db, plan, attempt, scope, _, store, _, commit, _ = publication_case
    intent = persist(publication_case)
    if fault == "corrupt":
        store._path_from_uri(intent["uri"]).write_bytes(b"corrupt")
    elif fault == "missing":
        monkeypatch.setattr(store, "open_stream", Mock(side_effect=FileNotFoundError()))
    elif fault == "cancel":
        scope.status = ScopeStatus.CANCELLED
    elif fault == "fence":
        # Keep the caller's fence stale without mutating its fixture instance.
        from sqlalchemy import update

        db.execute(
            update(WorkflowAttempt)
            .where(WorkflowAttempt.id == attempt.id)
            .values(fencing_token=uuid.uuid4().hex),
            execution_options={"synchronize_session": False},
        )
    else:
        event = publication._event(db, attempt.id, "evaluation_plan")
        event.result_digest = "0" * 64
    db.flush()
    with pytest.raises((ValueError, FileNotFoundError)):
        recover(publication_case)
    commit.assert_not_called()


def test_forged_authority_receipt_cannot_complete_scope(publication_case):
    persist(publication_case)
    commit = publication_case[-2]

    def forged(digest):
        receipt = commit(digest)
        receipt["signature"] = "0" * 128
        return receipt

    with pytest.raises(ValueError, match="receipt"):
        recover(publication_case, commit=forged)
    db, _, attempt, scope, *_ = publication_case
    db.refresh(scope)
    db.refresh(attempt)
    assert scope.status == ScopeStatus.RUNNING and attempt.status == AttemptStatus.RUNNING
