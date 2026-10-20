"""Shared verifier publication validates proof and immutable execution before storage."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from automl_api.models.datasets import DatasetProviderImport
from automl_api.models.workflows import WorkflowCommand
from automl_api.services.provider_copy_verification import verify_provider_objects
from automl_api.services.provider_import import (
    claim_provider_import,
    publish_provider_import,
    renew_provider_import,
)
from automl_api.services.provider_import_attempts import verification_attempt
from automl_api.services.workflow_state import canonical_request_hash
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def execution(pending_case, monkeypatch):
    c = pending_case
    c.execution = claim_provider_import(
        c.factory, c.version_id, policy=c.policy, public_key=c.key.public_key()
    )
    assert c.execution is not None
    c.proof = verify_provider_objects(c.value, c.store, c.limits)
    c.factory_calls = Mock(side_effect=AssertionError("Metadata publication constructed storage"))
    from automl_api.storage import object_store

    monkeypatch.setattr(object_store, "get_object_store", c.factory_calls)
    return c


def publish(c, *, execution=None, proof=None):
    return publish_provider_import(
        c.factory,
        execution or c.execution,
        proof if proof is not None else c.proof,
        policy=c.policy,
        public_key=c.key.public_key(),
    )


def test_publication_replay_returns_same_ids_without_remote_io(execution):
    c = execution
    response = publish(c)
    before = (list(c.store.writes), list(c.store.calls))
    assert publish(c) == response
    assert (c.store.writes, c.store.calls) == before
    c.factory_calls.assert_not_called()
    assert c.store.writes == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("manifest_digest", "0" * 64),
        ("schema_digest", "0" * 64),
        ("objects", 999),
        ("byte_size", 1),
        ("rows", {"raw": 1}),
        ("role_digests", {"train": "0" * 64, "validation": "0" * 64}),
        ("columns", [{"name": "other", "type": "int64", "nullable": True}]),
        ("verified_at", datetime(2026, 1, 1)),
    ],
)
def test_invalid_copy_proof_is_rejected_before_store_construction(execution, field, value):
    c = execution
    bad = {**c.proof.model_dump(), field: value}
    with pytest.raises(ValueError):
        publish(c, proof=bad)
    c.factory_calls.assert_not_called()
    assert c.store.writes == []
    assert publish(c)["dataset_version_id"] == str(c.version_id)


@pytest.mark.parametrize(
    "field", ["dataset_id", "command_id", "actor_id", "import_id", "attempt_id", "fence"]
)
def test_stale_execution_binding_cannot_publish(execution, field):
    c = execution
    bad = replace(c.execution, **{field: uuid.uuid4().hex if field == "fence" else uuid.uuid4()})
    with pytest.raises(ValueError):
        publish(c, execution=bad)
    c.factory_calls.assert_not_called()
    assert publish(c)["dataset_version_id"] == str(c.version_id)


def test_request_budget_rewrite_does_not_adopt_a_new_execution_intent(execution):
    c = execution
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        original, digest = command.request_payload, command.request_hash
        command.request_payload = {
            **original,
            "budget": {
                **original["budget"],
                "max_temporary_bytes": original["budget"]["max_temporary_bytes"] + 4096,
            },
        }
        command.request_hash = canonical_request_hash(command.request_payload)
    with pytest.raises(ValueError, match="binding changed"):
        publish(c)
    with pytest.raises(ValueError, match="binding changed"):
        renew_provider_import(
            c.factory, c.execution, policy=c.policy, public_key=c.key.public_key()
        )
    c.factory_calls.assert_not_called()
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        command.request_payload, command.request_hash = original, digest
    assert publish(c)["dataset_version_id"] == str(c.version_id)


def test_renewal_is_bounded_by_current_pod_identity(execution):
    c = execution
    deadline = datetime.now(UTC) + timedelta(seconds=10)
    renew_provider_import(
        c.factory,
        c.execution,
        policy=c.policy,
        public_key=c.key.public_key(),
        identity_deadline=deadline,
    )
    with c.factory() as db:
        command = db.get(WorkflowCommand, c.command_id)
        attempt = verification_attempt(db, db.get(DatasetProviderImport, c.import_id))
        assert attempt.lease_expires_at == command.lease_expires_at == deadline
    assert publish(c)["dataset_version_id"] == str(c.version_id)
