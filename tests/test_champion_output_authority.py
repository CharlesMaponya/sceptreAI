"""Output writes obey revocation and cannot acknowledge changed authority."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from automl_api.api.routes import evaluation_control, refit_control
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
from automl_api.services import evaluation_publication as publication
from automl_api.services.attempt_object_grants import revoke_attempt_grants
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.storage.contracts import ObjectMetadata
from sqlalchemy import select
from test_champion_evaluation_control import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_control import control_case as evaluation_control_case  # noqa: F401
from test_champion_evaluation_control import handoff_case as handoff_case
from test_champion_evaluation_control import key_case as key_case
from test_champion_evaluation_publication import publication_case as publication_case
from test_champion_evaluation_publication import recover
from test_champion_refit_control import control_case as refit_control_case  # noqa: F401

pytest_plugins = [
    "test_champion_refit_publication",
    "test_champion_evaluation_planning",
    "test_champion_evaluation_execution",
    "test_qualification_control",
]


@pytest.fixture(params=["refit", "evaluation"])
def output_case(request, phase1_engine, monkeypatch, tmp_path):
    import uuid

    from champion_broker_fixture import install_champion_broker

    stage = request.param
    original = request.getfixturevalue(stage + "_control_case")
    control, source = original[:2]
    if stage == "refit":
        plan, attempt, scope = original[3:6]
        payload, route = b"refit output authority fixture", refit_control
        claims = refit_control.refit_claims(original[-1], attempt.id)
    else:
        scope, attempt, plan, payload = original[2:6]
        route = evaluation_control
        claims = evaluation_control.evaluation_claims(original[-1], attempt.id)
    assert control.post("start").status_code == 200
    store = route.get_object_store()
    binding = install_champion_broker(
        source,
        plan,
        route,
        claims,
        phase1_engine,
        monkeypatch,
        tmp_path,
        mint=lambda *_: None,
    )
    digest = hashlib.sha256(payload).hexdigest()
    role = "pipeline" if stage == "refit" else "result"
    c = SimpleNamespace(
        stage=stage,
        control=binding.broker,
        factory=binding.case.factory,
        engine=phase1_engine,
        plan=binding.case.plan,
        payload=payload,
        store=store,
        fence=attempt.fencing_token,
        path=f"/api/v1/internal/attempts/{attempt.id}/objects/{role}/{digest}",
        scope_id=scope.id,
        digest=digest,
        headers={
            "Authorization": "Bearer " + binding.case.token,
            "X-Request-ID": str(uuid.uuid4()),
        },
    )
    try:
        yield c
    finally:
        binding.close()


def write(c, *, during_body=None):
    async def upload():
        async def body():
            middle = len(c.payload) // 2
            yield c.payload[:middle]
            if during_body is not None:
                during_body()
            yield c.payload[middle:]

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=c.control.app, raise_app_exceptions=False),
            base_url=str(c.control.base_url),
            headers=c.headers,
        ) as client:
            return await client.put(
                c.path, content=body(), headers={"Content-Length": str(len(c.payload))}
            )

    return asyncio.run(upload())


def revoke(c):
    with c.factory() as db, db.begin():
        revoke_attempt_grants(db, c.plan.attempt_id)


def saved_intent(c):
    with c.factory() as db:
        return db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.plan.attempt_id,
                WorkflowEvent.event_key == c.stage + "_output",
            )
        )


def test_live_output_remains_available(output_case):
    c = output_case
    response = write(c)
    assert response.status_code == 200, response.text
    intent = saved_intent(c)
    assert {key: response.json()[key] for key in ("uri", "sha256", "byte_size")} == intent.payload
    with c.store.open_stream(intent.payload["uri"]) as stream:
        assert stream.read() == c.payload


def test_evaluator_committed_revocation_blocks_direct_persistence(output_case):
    c = output_case
    if c.stage == "refit":
        revoke(c)
        assert write(c).status_code == 401
    else:
        revoke(c)
        with pytest.raises(StaleFence):
            publication.persist_evaluation_result(c.factory, c.plan, c.fence, c.payload, c.store)
    assert saved_intent(c) is None


def test_evaluator_pending_revocation_blocks_shared_writing_guard(output_case):
    c = output_case
    if c.stage == "refit":
        from automl_api.services.refit_access import locked_refit

        with c.factory() as db, db.begin():
            revoke_attempt_grants(db, c.plan.attempt_id)
            assert any(isinstance(row, WorkflowEvent) for row in db.new)
            with pytest.raises(StaleFence):
                locked_refit(
                    db,
                    c.plan.attempt_id,
                    {"project_id": c.plan.project_id, "fence": c.fence},
                    (AttemptStatus.RUNNING,),
                )
    else:
        with c.factory() as db, db.begin():
            revoke_attempt_grants(db, c.plan.attempt_id)
            assert any(isinstance(row, WorkflowEvent) for row in db.new)
            with pytest.raises(StaleFence):
                publication._locked(db, c.plan, c.fence, writing=True)
    assert saved_intent(c) is None


def test_revocation_during_http_body_prevents_storage_and_preserves_prior_intent(
    output_case, monkeypatch
):
    c = output_case
    method = "put_stream"
    native = Mock(side_effect=AssertionError("Revoked body must not reach storage"))
    monkeypatch.setattr(c.store, method, native)
    response = write(c, during_body=lambda: revoke(c))
    assert response.status_code == 401, response.text
    intent = saved_intent(c)
    assert intent is None
    native.assert_not_called()


@pytest.mark.parametrize(
    "output_case,mutation",
    [
        (stage, mutation)
        for stage in ("refit", "evaluation")
        for mutation in [
            "none",
            "revoke",
            "cancel",
            "supersede",
            "fence",
            "lease",
            "deadline",
            "plan",
            "intent",
            "metadata_uri",
            "metadata_size",
            "recovery_claim",
        ]
        if mutation != "recovery_claim" or stage == "evaluation"
    ],
    indirect=["output_case"],
)
def test_output_acknowledgement_rechecks_authority_after_storage(
    output_case, monkeypatch, mutation
):
    c = output_case
    method = "put_stream"
    original = getattr(c.store, method)
    writes = []

    def store(*args, **kwargs):
        assert c.engine.pool.checkedout() == 0
        intent = saved_intent(c)
        assert intent is not None and intent.payload["sha256"] == c.digest
        metadata = original(*args, **kwargs)
        writes.append(metadata)
        with c.factory() as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.plan.attempt_id)
            scope = db.get(PromotionalScope, c.scope_id)
            if mutation == "revoke":
                revoke_attempt_grants(db, attempt.id)
            elif mutation == "recovery_claim":
                publication._append(
                    db, attempt, "evaluation_recovery_started", {"attempt_id": str(attempt.id)}
                )
            elif mutation == "cancel":
                scope.status = ScopeStatus.CANCELLED
            elif mutation == "supersede":
                attempt.status = AttemptStatus.SUPERSEDED
            elif mutation == "fence":
                from sqlalchemy.exc import IntegrityError

                with pytest.raises(IntegrityError), db.begin_nested():
                    attempt.fencing_token = "replacement-output-fence"
                    db.flush()
                db.refresh(attempt)
                attempt.status = AttemptStatus.SUPERSEDED
            elif mutation == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif mutation == "deadline":
                scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
            elif mutation in {"plan", "intent"}:
                key = c.stage + ("_plan" if mutation == "plan" else "_output")
                event = db.scalar(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == key
                    )
                )
                if mutation == "plan":
                    plan = c.plan.model_copy(update={"max_model_bytes": c.plan.max_model_bytes + 1})
                    event.payload = plan.model_dump(mode="json")
                    event.result_digest = plan.digest
                else:
                    event.payload = {**event.payload, "byte_size": len(c.payload) + 1}
                    event.result_digest = canonical_request_hash(event.payload)
        assert c.engine.pool.checkedout() == 0
        if mutation in {"metadata_uri", "metadata_size"}:
            return ObjectMetadata(
                uri=metadata.uri + ("/other" if mutation == "metadata_uri" else ""),
                byte_size=metadata.byte_size + (mutation == "metadata_size"),
            )
        return metadata

    monkeypatch.setattr(c.store, method, store)
    response = write(c)
    assert len(writes) == 1
    if mutation == "none":
        assert response.status_code == 200, response.text
    else:
        assert response.status_code == 503, response.text
    # An already started write is retained for reconciliation, never erased to fake denial.
    with c.store.open_stream(writes[0].uri) as stream:
        assert stream.read() == c.payload
    assert saved_intent(c) is not None


def test_trusted_recovery_of_valid_stored_output_survives_worker_revocation(publication_case):
    db, plan, attempt, _, payload, store, factory, commit, _ = publication_case
    intent = publication.persist_evaluation_result(
        factory, plan, attempt.fencing_token, payload, store
    )
    revoke_attempt_grants(db, attempt.id)
    attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    assert recover(publication_case) == intent
    assert recover(publication_case) == intent
    assert commit.call_count == 2


@pytest.mark.parametrize("output_case", ["evaluation"], indirect=True)
def test_evaluator_recovery_claim_during_body_blocks_storage(output_case, monkeypatch):
    c = output_case
    native = Mock(side_effect=AssertionError("Recovery-fenced worker cannot write"))
    monkeypatch.setattr(c.store, "put_stream", native)

    def claim():
        with c.factory() as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.plan.attempt_id)
            publication._append(
                db, attempt, "evaluation_recovery_started", {"attempt_id": str(attempt.id)}
            )

    assert write(c, during_body=claim).status_code == 401
    assert saved_intent(c) is None
    native.assert_not_called()


@pytest.mark.parametrize("output_case", ["evaluation"], indirect=True)
def test_evaluator_revalidates_after_stored_result_verification(output_case, monkeypatch):
    c = output_case
    from contextlib import contextmanager

    original = c.store.open_stream

    @contextmanager
    def verify(uri, **kwargs):
        assert c.engine.pool.checkedout() == 0
        with original(uri, **kwargs) as stream:
            yield stream
        if uri.endswith("/result.json"):
            revoke(c)

    monkeypatch.setattr(c.store, "open_stream", verify)
    assert write(c).status_code == 503
    intent = saved_intent(c)
    assert intent is not None
    with original(intent.payload["uri"]) as stream:
        assert stream.read() == c.payload


def test_corrupt_existing_output_intent_cannot_trigger_another_write(output_case, monkeypatch):
    c = output_case
    assert write(c).status_code == 200
    intent = saved_intent(c)
    with c.factory() as db, db.begin():
        db.get(WorkflowEvent, intent.id).result_digest = "f" * 64
    method = "put_stream"
    native = Mock(side_effect=AssertionError("Corrupt output history cannot authorize a write"))
    monkeypatch.setattr(c.store, method, native)
    assert write(c).status_code == 401
    native.assert_not_called()
