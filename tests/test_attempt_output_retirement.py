"""An elapsed logical deadline cannot retire a possibly active native write."""

import uuid
from datetime import timedelta

import pytest
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.workflows import WorkflowAttemptCredentialGrant, WorkflowEvent
from automl_api.services import attempt_object_grants as grants
from automl_api.services import deletion
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.storage.contracts import DeletionPending
from test_attempt_object_grant_schema import grant_values, make_attempt
from test_grant_retirement import clock as clock

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.mark.parametrize("status", ["issuing", "issued", "unknown", "failed", "expired", "revoked"])
def test_expired_started_put_remains_a_retirement_barrier(phase1_db, tenant, clock, status):
    db = phase1_db
    user, project = tenant
    attempt = make_attempt(db, user, project)
    attempt.status = AttemptStatus.FAILED
    now = clock[0]
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            attempt,
            method="PUT",
            role="model",
            object_version=None,
            status=status,
            created_at=now,
            expires_at=now + timedelta(seconds=60),
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            issued_at=now if status == "issued" else None,
            revoked_at=now if status == "revoked" else None,
        )
    )
    row.grant_digest = grants._digest(row)
    db.add(row)
    db.flush()
    deadline = row.expires_at
    clock[0] = deadline + timedelta(days=1)
    assert grants.unretired_grant_deadline(db, attempt.id) is None
    assert grants.has_unretired_attempt_credentials(db, attempt.id)
    with pytest.raises(DeletionPending):
        deletion._retire_deletion_grants(db, project.id, None)
    db.refresh(row)
    assert row.expires_at == deadline
    assert row.issuance_owner is not None and row.issuance_started_at == now


@pytest.fixture
def completed_put(phase1_db, tenant, clock):
    db = phase1_db
    user, project = tenant
    attempt = make_attempt(db, user, project)
    attempt.status = AttemptStatus.FAILED
    now = clock[0]
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            attempt,
            method="PUT",
            role="model",
            object_version=None,
            status="issued",
            created_at=now,
            expires_at=now + timedelta(seconds=60),
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            issued_at=now,
        )
    )
    row.grant_digest = grants._digest(row)
    db.add(row)
    db.flush()
    payload = {
        "grant_id": str(row.id),
        "grant_digest": row.grant_digest,
        "issuance_owner": str(row.issuance_owner),
        "uri": row.object_uri,
        "sha256": row.content_sha256,
        "byte_size": row.byte_limit,
        "object_version": "immutable-version-1",
    }
    event = WorkflowEvent(
        project_id=project.id,
        attempt_id=attempt.id,
        sequence=1,
        event_key=f"attempt.output.completed.{row.id}",
        event_type="attempt.output.completed",
        payload=payload,
        result_digest=canonical_request_hash(payload),
    )
    db.add(event)
    db.flush()
    return db, project, attempt, row, event


def test_verified_completed_put_waits_for_original_deadline(completed_put, clock):
    db, project, attempt, row, event = completed_put
    original = (row.grant_digest, row.expires_at, dict(event.payload))
    assert grants.output_completion(db, row) == event.payload
    assert grants.has_unretired_attempt_credentials(db, attempt.id)
    with pytest.raises(DeletionPending):
        deletion._retire_deletion_grants(db, project.id, None)
    clock[0] = row.expires_at
    assert not grants.has_unretired_attempt_credentials(db, attempt.id)
    deletion._retire_deletion_grants(db, project.id, None)
    assert (row.grant_digest, row.expires_at, event.payload) == original
    assert row.status == "revoked"


@pytest.mark.parametrize(
    "field,value",
    [
        ("grant_id", str(uuid.UUID(int=1))),
        ("grant_digest", "0" * 64),
        ("issuance_owner", str(uuid.UUID(int=1))),
        ("uri", "s3c://another/forged"),
        ("sha256", "0" * 64),
        ("byte_size", 1),
        ("object_version", None),
        ("object_version", "null"),
        ("object_version", ""),
        ("object_version", "v" * 257),
        ("object_version", "unsafe\nversion"),
        ("extra", "unbound"),
    ],
)
def test_rehashed_mismatched_completion_cannot_retire_write(completed_put, clock, field, value):
    db, project, attempt, row, event = completed_put
    event.payload = {**event.payload, field: value}
    event.result_digest = canonical_request_hash(event.payload)
    clock[0] = row.expires_at + timedelta(days=1)
    db.flush()
    assert grants.output_completion(db, row) is None
    assert grants.has_unretired_attempt_credentials(db, attempt.id)
    with pytest.raises(DeletionPending):
        deletion._retire_deletion_grants(db, project.id, None)


@pytest.mark.parametrize("field", ["event_type", "event_key", "result_digest", "grant_digest"])
def test_invalid_completion_record_still_blocks(completed_put, clock, field):
    db, _, attempt, row, event = completed_put
    if field == "grant_digest":
        row.grant_digest = "f" * 64
    else:
        setattr(event, field, "f" * 64)
    clock[0] = row.expires_at
    db.flush()
    assert grants.output_completion(db, row) is None
    assert grants.has_unretired_attempt_credentials(db, attempt.id)


@pytest.mark.parametrize(
    "stage,role",
    [
        (WorkflowStage.SPLITTER, "manifest"),
        (WorkflowStage.PREPARATION, "prepared"),
        (WorkflowStage.TRAINING_RUN, "model"),
        (WorkflowStage.TRAINING_TRIAL, "model"),
        (WorkflowStage.CHAMPION_REFIT, "pipeline"),
        (WorkflowStage.CHAMPION_EVALUATION, "result"),
        (WorkflowStage.PROVIDER_CONFORMANCE, "conformance_evidence"),
    ],
)
def test_unknown_put_barrier_covers_every_stage(phase1_db, tenant, clock, stage, role):
    db = phase1_db
    user, project = tenant
    attempt = make_attempt(db, user, project, stage)
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            attempt,
            method="PUT",
            role=role,
            object_version=None,
            status="unknown",
            created_at=clock[0],
            expires_at=clock[0] + timedelta(seconds=60),
            issuance_owner=uuid.uuid4(),
            issuance_started_at=clock[0],
        )
    )
    row.grant_digest = grants._digest(row)
    db.add(row)
    db.flush()
    clock[0] = row.expires_at + timedelta(days=1)
    assert grants.has_unretired_attempt_credentials(db, attempt.id)
    with pytest.raises(DeletionPending):
        deletion._retire_deletion_grants(db, project.id, None)
