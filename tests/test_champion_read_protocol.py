"""Legacy plan hashes survive; new plans cannot silently use old read protocols."""

import copy

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowEvent
from automl_api.services import deletion
from automl_api.services.attempt_object_grants import has_unretired_attempt_credentials
from automl_api.services.evaluation_planning import validate_replacement
from automl_api.services.workflow_state import InvalidTransition, canonical_request_hash
from automl_api.storage.contracts import DeletionPending
from automl_api.training.champion_evaluation import EvaluationPlan
from automl_api.training.champion_refit import RefitPlan
from champion_planning_fixture import plan_scope_refit
from pydantic import ConfigDict, ValidationError, create_model
from sqlalchemy import select
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_evaluation_jobs import evaluator_job_case as evaluator_job_case
from test_champion_refit_jobs import jobs_case as jobs_case

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


@pytest.fixture(params=["refit", "evaluation"])
def protocol_case(request):
    refit = request.param == "refit"
    if refit:
        db, store, scope, attempt, _, _ = request.getfixturevalue("jobs_case")
    else:
        c = request.getfixturevalue("evaluator_job_case")
        db, store, scope, attempt = c.db, None, c.scope, c.attempt
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key == request.param + "_plan",
        )
    )
    cls = RefitPlan if refit else EvaluationPlan
    return db, store, scope, attempt, event, cls


@pytest.mark.parametrize("legacy", [None, "pod-bound-read-v1"])
def test_legacy_plan_hash_and_serialization_are_unchanged(protocol_case, legacy):
    _, _, _, _, event, cls = protocol_case
    assert event.payload["credential_protocol"] == "pod-bound-object-v2"
    old = {k: v for k, v in event.payload.items() if k != "credential_protocol"}
    if legacy is not None:
        old["credential_protocol"] = legacy
    plan = cls.model_validate(old)
    assert plan.credential_protocol == legacy
    assert plan.model_dump(mode="json") == old
    assert plan.digest == canonical_request_hash(old)
    if cls is RefitPlan:
        assert plan.policy_digest == cls.model_validate(event.payload).policy_digest


def test_read_v1_replica_rejects_new_write_protocol_and_keeps_its_old_hash(protocol_case):
    from typing import Literal

    _, _, _, _, event, cls = protocol_case
    previous = create_model(
        "ReadV1" + cls.__name__,
        __base__=cls,
        credential_protocol=(Literal["pod-bound-read-v1"] | None, None),
    )
    old = {**event.payload, "credential_protocol": "pod-bound-read-v1"}
    assert previous.model_validate(old).model_dump(mode="json") == old
    assert previous.model_validate(old).digest == cls.model_validate(old).digest
    with pytest.raises(ValidationError):
        previous.model_validate(event.payload)


@pytest.mark.parametrize("protocol", [None, "pod-bound-read-v1", "pod-bound-object-v2"])
def test_unjournaled_output_intent_never_proves_native_retirement(protocol_case, protocol):
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.evaluation_publication import _append

    db, _, _, attempt, event, cls = protocol_case
    payload = {k: v for k, v in event.payload.items() if k != "credential_protocol"}
    if protocol is not None:
        payload["credential_protocol"] = protocol
    event.payload, event.result_digest = payload, cls.model_validate(payload).digest
    attempt.status = AttemptStatus.FAILED
    attempt.cleanup_state = "complete"
    key = "refit_output" if cls is RefitPlan else "evaluation_output"
    _append(
        db,
        db.get(WorkflowAttempt, attempt.id),
        key,
        {"uri": "s3c://legacy/output", "sha256": "f" * 64, "byte_size": 1},
    )
    db.flush()
    assert has_unretired_attempt_credentials(db, attempt.id)
    with pytest.raises(DeletionPending):
        deletion._retire_deletion_grants(db, attempt.project_id, None)


def test_legacy_closed_plan_reader_rejects_new_credential_protocol(protocol_case):
    _, _, _, _, event, cls = protocol_case
    # Models the original closed wire schema; actual previous-binary qualification
    # remains a separate gate and is not inferred from this structural control.
    old_type = create_model(
        "LegacyClosed" + cls.__name__,
        __config__=ConfigDict(extra="forbid"),
        **{
            name: (field.annotation, copy.deepcopy(field))
            for name, field in cls.model_fields.items()
            if name != "credential_protocol"
        },
    )
    old = {k: v for k, v in event.payload.items() if k != "credential_protocol"}
    assert old_type.model_validate(old).model_dump(mode="json") == old
    with pytest.raises(ValidationError) as error:
        old_type.model_validate(event.payload)
    assert any(
        e["loc"] == ("credential_protocol",) and e["type"] == "extra_forbidden"
        for e in error.value.errors()
    )


def test_legacy_champion_reads_without_verified_retirement_block_cleanup_and_replacement(
    protocol_case,
):
    db, store, scope, attempt, event, cls = protocol_case
    assert not has_unretired_attempt_credentials(db, attempt.id)
    old = {k: v for k, v in event.payload.items() if k != "credential_protocol"}
    event.payload = old
    event.result_digest = cls.model_validate(old).digest
    attempt.status = AttemptStatus.FAILED
    attempt.cleanup_state = "complete"
    db.flush()
    assert has_unretired_attempt_credentials(db, attempt.id)
    with pytest.raises(DeletionPending, match="Legacy champion"):
        deletion._retire_deletion_grants(db, attempt.project_id, None)
    if cls is RefitPlan:
        assert plan_scope_refit(db, scope.id, store) is None
    else:
        with pytest.raises(InvalidTransition, match="retired predecessor"):
            validate_replacement(db, scope.id)
    db.refresh(event)
    assert event.payload == old and event.result_digest == cls.model_validate(old).digest
