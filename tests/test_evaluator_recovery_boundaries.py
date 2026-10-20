"""Recovery must validate authority identities and immutable signed abort receipts."""

import hashlib
import uuid
from copy import deepcopy
from types import SimpleNamespace

import httpx
import pytest
from automl_api.services.evaluation_reconciler import _abort_registration, _authority_state
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.workflow_state import InvalidTransition
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_evaluation_jobs import evaluator_job_case as evaluator_job_case
from test_champion_identity import publish
from test_qualification_control import allocate, headers

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


def response(value, status=200):
    return httpx.Response(
        status, json=value, request=httpx.Request("GET", "https://authority.test/")
    )


def transport(client):
    return SimpleNamespace(
        post=lambda path, **kwargs: client.post(path, headers=headers("allocator"), **kwargs),
        get=lambda path, **kwargs: client.get(path, headers=headers("allocator"), **kwargs),
    )


@pytest.fixture
def allocation_case(authority_api):
    client, scope, options = authority_api
    allocation_id = uuid.UUID(allocate(client, scope))
    publish(client, f"/allocations/{allocation_id}")
    authority = transport(client)
    plan = SimpleNamespace(
        allocation_id=allocation_id,
        scope_id=scope,
        final_manifest=SimpleNamespace(
            project_reference=options["principals"][0].project_reference,
            split_digest=hashlib.sha256(str(scope).encode()).hexdigest(),
            digest="a" * 64,
            provider="aws",
        ),
    )
    key = options["signing_secret"].public_key()
    state = _authority_state(authority, plan, key)
    assert state["status"] == "allocated" and state["cas_version"] == 0
    assert state["receipts"]
    return SimpleNamespace(authority=authority, plan=plan, key=key, state=state)


@pytest.mark.parametrize(
    "field",
    [
        "allocation_id",
        "scope_id",
        "project_reference",
        "split_digest",
        "provider_manifest_digest",
        "canonical_provider",
    ],
)
def test_recovery_state_rejects_foreign_allocation_binding(allocation_case, field):
    c = allocation_case
    value = deepcopy(c.state)
    value[field] = "foreign-value"
    with pytest.raises(InvalidTransition, match="allocation differs"):
        _authority_state(SimpleNamespace(get=lambda *_: response(value)), c.plan, c.key)
    assert _authority_state(c.authority, c.plan, c.key) == c.state


@pytest.mark.parametrize("change", ["algorithm", "allocation", "payload", "signature", "signer"])
def test_recovery_state_rejects_untrusted_receipts(allocation_case, change):
    c = allocation_case
    value = deepcopy(c.state)
    receipt = value["receipts"][0]
    key = c.key
    if change == "algorithm":
        receipt["signature_algorithm"] = "hmac-sha256"
    elif change == "allocation":
        receipt["allocation_id"] = str(uuid.uuid4())
    elif change == "payload":
        receipt["payload"]["scope_id"] = str(uuid.uuid4())
    elif change == "signature":
        receipt["signature"] = "00" * 64
    else:
        key = Ed25519PrivateKey.generate().public_key()
    with pytest.raises(InvalidTransition, match="receipt is invalid"):
        _authority_state(SimpleNamespace(get=lambda *_: response(value)), c.plan, key)
    assert _authority_state(c.authority, c.plan, c.key) == c.state


def test_recovery_authority_http_failure_is_not_an_empty_allocation(allocation_case):
    c = allocation_case
    with pytest.raises(httpx.HTTPStatusError):
        _authority_state(SimpleNamespace(get=lambda *_: response({}, 503)), c.plan, c.key)
    assert _authority_state(c.authority, c.plan, c.key) == c.state


@pytest.fixture
def aborted_case(allocation_case):
    c = allocation_case
    receipt = _abort_registration(
        c.authority, c.plan.allocation_id, c.plan.scope_id, "a" * 64, c.key
    )
    assert verify_receipt(SimpleNamespace(**receipt), c.key)
    state = c.authority.get(f"allocations/{c.plan.allocation_id}").json()
    assert state["status"] == "failed" and state["cas_version"] == 1
    c.abort_receipt, c.abort_state = receipt, state
    return c


def recover_abort(c, *, state=None, receipt=None, key=None):
    authority = SimpleNamespace(
        post=lambda *_args, **_kwargs: (
            response(receipt, 200) if receipt is not None else response({}, 409)
        ),
        get=lambda *_args: response(c.abort_state if state is None else state),
    )
    return _abort_registration(
        authority, c.plan.allocation_id, c.plan.scope_id, "a" * 64, c.key if key is None else key
    )


def test_abort_replays_and_recovers_the_exact_immutable_receipt(aborted_case):
    c = aborted_case
    assert (
        _abort_registration(c.authority, c.plan.allocation_id, c.plan.scope_id, "a" * 64, c.key)
        == c.abort_receipt
    )
    assert recover_abort(c) == c.abort_receipt
    assert c.authority.get(f"allocations/{c.plan.allocation_id}").json() == c.abort_state


@pytest.mark.parametrize(
    "field,value",
    [
        ("allocation_id", "foreign"),
        ("scope_id", "foreign"),
        ("provider_manifest_digest", "b" * 64),
        ("status", "opened"),
        ("cas_version", 2),
    ],
)
def test_abort_conflict_state_must_match_the_expected_allocation(aborted_case, field, value):
    c = aborted_case
    assert recover_abort(c) == c.abort_receipt
    state = deepcopy(c.abort_state)
    state[field] = value
    with pytest.raises(InvalidTransition, match="abort state is invalid"):
        recover_abort(c, state=state)
    assert c.authority.get(f"allocations/{c.plan.allocation_id}").json() == c.abort_state


@pytest.mark.parametrize(
    "change", ["missing", "duplicate", "empty_reason", "oversized_reason", "wrong_reason_type"]
)
def test_abort_recovery_rejects_missing_ambiguous_or_invalid_custody(aborted_case, change):
    c = aborted_case
    assert recover_abort(c) == c.abort_receipt
    state = deepcopy(c.abort_state)
    receipt = next(r for r in state["receipts"] if r["operation"] == "abort")
    if change == "missing":
        state["receipts"].remove(receipt)
    elif change == "duplicate":
        state["receipts"].append(deepcopy(receipt))
    else:
        receipt["payload"]["reason"] = {
            "empty_reason": "",
            "oversized_reason": "x" * 2001,
            "wrong_reason_type": 1,
        }[change]
    with pytest.raises(InvalidTransition, match="Authority abort"):
        recover_abort(c, state=state)
    assert c.authority.get(f"allocations/{c.plan.allocation_id}").json() == c.abort_state


@pytest.mark.parametrize(
    "change", ["operation", "algorithm", "allocation", "request", "payload", "signature", "signer"]
)
def test_direct_abort_receipt_must_be_valid_and_bound(aborted_case, change):
    c = aborted_case
    assert recover_abort(c, receipt=c.abort_receipt) == c.abort_receipt
    receipt = deepcopy(c.abort_receipt)
    key = c.key
    if change == "operation":
        receipt["operation"] = "commit"
    elif change == "algorithm":
        receipt["signature_algorithm"] = "hmac-sha256"
    elif change == "allocation":
        receipt["allocation_id"] = str(uuid.uuid4())
    elif change == "request":
        receipt["request_digest"] = "f" * 64
    elif change == "payload":
        receipt["payload"]["scope_id"] = str(uuid.uuid4())
    elif change == "signature":
        receipt["signature"] = "00" * 64
    else:
        key = Ed25519PrivateKey.generate().public_key()
    with pytest.raises(InvalidTransition, match="abort receipt is invalid"):
        recover_abort(c, receipt=receipt, key=key)
    assert c.authority.get(f"allocations/{c.plan.allocation_id}").json() == c.abort_state


@pytest.mark.parametrize("status", [403, 503])
def test_abort_http_failure_cannot_supply_a_recovery_receipt(aborted_case, status):
    c = aborted_case
    authority = SimpleNamespace(post=lambda *_args, **_kwargs: response({}, status))
    with pytest.raises(httpx.HTTPStatusError):
        _abort_registration(authority, c.plan.allocation_id, c.plan.scope_id, "a" * 64, c.key)
    assert c.authority.get(f"allocations/{c.plan.allocation_id}").json() == c.abort_state


@pytest.fixture
def failed_evaluator(evaluator_job_case, handoff_case, authority_api):
    from automl_api.models.enums import AttemptStatus, ScopeStatus
    from automl_api.services.evaluation_reconciler import _seal_failed

    c = evaluator_job_case
    c.public_key = handoff_case[4]
    c.signing_key = authority_api[2]["signing_secret"]
    state = _authority_state(c.authority, c.plan, c.public_key)
    _seal_failed(c.factory, c.authority, c.scope.id, state, [c.plan], c.public_key)
    c.db.refresh(c.scope)
    c.db.refresh(c.attempt)
    assert c.scope.status == ScopeStatus.FAILED
    assert c.attempt.status == AttemptStatus.CANCELLED
    c.failed_state = _authority_state(c.authority, c.plan, c.public_key)
    assert c.failed_state["status"] == "failed"
    return c


@pytest.mark.parametrize("change", ["missing", "pipeline"])
def test_failure_recovery_requires_the_registered_local_plan(failed_evaluator, change):
    from automl_api.services.evaluation_reconciler import _seal_failed

    c = failed_evaluator
    plans = (
        []
        if change == "missing"
        else [
            SimpleNamespace(
                attempt_id=c.plan.attempt_id, frozen_pipeline=SimpleNamespace(sha256="f" * 64)
            )
        ]
    )
    with pytest.raises(InvalidTransition, match="no matching local plan"):
        _seal_failed(c.factory, c.authority, c.scope.id, c.failed_state, plans, c.public_key)
    assert _authority_state(c.authority, c.plan, c.public_key) == c.failed_state


@pytest.mark.parametrize(
    "change", ["operation", "algorithm", "signature", "allocation", "attempt", "pipeline"]
)
def test_failure_recovery_rejects_tampered_sealing_receipt(failed_evaluator, change):
    from automl_api.services.evaluation_reconciler import _seal_failed

    c = failed_evaluator
    state = deepcopy(c.failed_state)
    receipt = next(r for r in state["receipts"] if r["operation"] == "fail")
    if change == "operation":
        receipt["operation"] = "commit"
        # A direct response exercises the operation check before receipt selection.
        allocated = {**state, "status": "allocated"}
        authority = SimpleNamespace(post=lambda *_args, **_kwargs: response(receipt))
    else:
        allocated = state
        authority = c.authority
        if change == "algorithm":
            receipt["signature_algorithm"] = "hmac-sha256"
        elif change == "signature":
            receipt["signature"] = "00" * 64
        elif change == "allocation":
            receipt["allocation_id"] = str(uuid.uuid4())
        elif change == "attempt":
            receipt["payload"]["evaluator_attempt_id"] = str(uuid.uuid4())
        else:
            receipt["payload"]["frozen_pipeline_digest"] = "f" * 64
    if change in {"allocation", "attempt", "pipeline"}:
        from automl_api.services.final_test_authority import _receipt_bytes

        # Valid signatures still require exact allocation and evaluator bindings.
        body = _receipt_bytes(
            allocation_id=receipt["allocation_id"],
            operation=receipt["operation"],
            provider=receipt["provider"],
            request_digest=receipt["request_digest"],
            payload=receipt["payload"],
        )
        receipt["receipt_digest"] = hashlib.sha256(body).hexdigest()
        receipt["signature"] = c.signing_key.sign(body).hex()
        assert verify_receipt(SimpleNamespace(**receipt), c.public_key)
    with pytest.raises(InvalidTransition, match="failure receipt is invalid"):
        _seal_failed(c.factory, authority, c.scope.id, allocated, [c.plan], c.public_key)
    assert _authority_state(c.authority, c.plan, c.public_key) == c.failed_state


@pytest.mark.parametrize("deleted", [False, True])
def test_failure_receipt_cannot_fail_completed_or_deleted_scope(failed_evaluator, deleted):
    from automl_api.models.enums import ScopeStatus
    from automl_api.models.workflows import PromotionalScope
    from automl_api.services.evaluation_reconciler import _finish_failure
    from sqlalchemy import delete

    c = failed_evaluator
    receipt = next(r for r in c.failed_state["receipts"] if r["operation"] == "fail")
    if deleted:
        c.db.execute(delete(PromotionalScope).where(PromotionalScope.id == c.scope.id))
    else:
        c.scope.status = ScopeStatus.SUCCEEDED
    c.db.flush()
    with pytest.raises(InvalidTransition, match="completed or deleted scope"):
        _finish_failure(c.factory, c.scope.id, receipt)
    if not deleted:
        c.db.refresh(c.scope)
        assert c.scope.status == ScopeStatus.SUCCEEDED
    assert _authority_state(c.authority, c.plan, c.public_key) == c.failed_state
