"""Committed output custody survives replay, response loss and authority changes."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from automl_api.models.enums import ScopeStatus
from automl_api.models.workflows import (
    PromotionalScope,
    WorkflowAttemptCredentialGrant,
    WorkflowEvent,
)
from automl_api.services import attempt_object_grants as grants
from automl_api.services import attempt_output_grants as outputs
from automl_api.services.workflow_state import canonical_request_hash
from sqlalchemy import select
from test_attempt_output_http import champion as champion
from test_attempt_output_http import compatible_uri_fixture as compatible_uri_fixture
from test_attempt_output_http import evaluation_control_case as evaluation_control_case
from test_attempt_output_http import grant_case as grant_case
from test_attempt_output_http import handoff_case as handoff_case
from test_attempt_output_http import key_case as key_case
from test_attempt_output_http import output_http as output_http
from test_attempt_output_http import refit_control_case as refit_control_case
from test_attempt_output_http import upload

pytest_plugins = [
    "test_champion_refit_publication",
    "test_champion_evaluation_planning",
    "test_qualification_control",
]


def saved(c):
    with c.case.factory() as db:
        return db.scalar(
            select(WorkflowAttemptCredentialGrant).where(
                WorkflowAttemptCredentialGrant.attempt_id == c.case.plan.attempt_id,
                WorkflowAttemptCredentialGrant.method == "PUT",
            )
        )


@pytest.mark.parametrize("change", ["nonce", "hash", "size", "role"])
def test_an_output_role_cannot_be_reissued_or_changed(output_http, change):
    c = output_http
    assert upload(c).status_code == 200
    original = saved(c)
    options = {
        "nonce": {"nonce": uuid.uuid4()},
        "hash": {"digest": "a" * 64},
        "size": {"payload": b"x"},
        "role": {"role": "result" if c.role == "pipeline" else "pipeline"},
    }
    assert upload(c, **options[change]).status_code == 401
    current = saved(c)
    assert (current.id, current.grant_digest, current.expires_at) == (
        original.id,
        original.grant_digest,
        original.expires_at,
    )
    assert len(c.writes) == 1


@pytest.mark.parametrize("fault", ["short", "hash", "length", "chunked", "nonce"])
def test_invalid_transport_cannot_start_a_native_write(output_http, fault):
    c = output_http
    options = {
        "short": {"payload": c.payload[:-1]},
        "hash": {"digest": "f" * 64},
        "length": {"headers": {"Content-Length": "-1"}},
        "chunked": {"headers": {"Transfer-Encoding": "chunked"}},
        "nonce": {"headers": {"X-Request-ID": "invalid"}},
    }
    assert upload(c, **options[fault]).status_code == 422
    row = saved(c)
    assert row is None or (row.status == "pending" and row.issuance_started_at is None)
    assert not c.writes


def test_accepted_unknown_native_output_never_reuploads_or_retires_from_expiry(
    output_http, monkeypatch
):
    c = output_http
    native = outputs.write_attempt_output
    original = []

    def lost(*args, **kwargs):
        result = native(*args, **kwargs)
        row = saved(c)
        assert row.status == "issuing" and row.issuance_owner is not None
        original.append((row.id, row.grant_digest, row.expires_at))
        assert c.backend.exists(result.uri)
        raise OSError("Accepted write response lost")

    monkeypatch.setattr(outputs, "write_attempt_output", lost)
    assert upload(c).status_code == 503
    row = saved(c)
    assert row.status == "unknown"
    assert (row.id, row.grant_digest, row.expires_at) == original[0]
    assert upload(c).status_code == 503
    assert c.client.get(c.base + f"/receipts/{c.nonce}", headers=c.headers).status_code == 503
    assert upload(c, nonce=uuid.uuid4()).status_code == 401
    assert len(c.writes) == 1

    class AfterExpiry(datetime):
        @classmethod
        def now(cls, tz=None):
            return row.expires_at + timedelta(days=1)

    monkeypatch.setattr(grants, "datetime", AfterExpiry)
    with c.case.factory() as db:
        assert grants.unretired_grant_deadline(db, row.attempt_id) is None
        assert grants.has_unretired_attempt_credentials(db, row.attempt_id)
        assert grants.output_completion(db, db.get(WorkflowAttemptCredentialGrant, row.id)) is None


@pytest.mark.parametrize("mutation", ["revocation", "cancel", "intent"])
def test_native_completion_is_retained_but_stale_acknowledgement_is_denied(
    output_http, monkeypatch, mutation
):
    c = output_http
    native = outputs.write_attempt_output

    def finish_then_change(*args, **kwargs):
        result = native(*args, **kwargs)
        with c.case.factory() as db, db.begin():
            if mutation == "revocation":
                grants.revoke_attempt_grants(db, c.case.plan.attempt_id)
            elif mutation == "cancel":
                db.get(PromotionalScope, c.case.plan.scope_id).status = ScopeStatus.CANCELLED
            else:
                key = "refit_output" if c.role == "pipeline" else "evaluation_output"
                event = db.scalar(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == c.case.plan.attempt_id,
                        WorkflowEvent.event_key == key,
                    )
                )
                event.payload = {**event.payload, "sha256": "f" * 64}
                event.result_digest = canonical_request_hash(event.payload)
        return result

    monkeypatch.setattr(outputs, "write_attempt_output", finish_then_change)
    assert upload(c).status_code == 503
    row = saved(c)
    assert c.backend.exists(row.object_uri) and len(c.writes) == 1
    with c.case.factory() as db:
        assert (
            grants.output_completion(db, db.get(WorkflowAttemptCredentialGrant, row.id)) is not None
        )
    assert c.client.get(c.base + f"/receipts/{c.nonce}", headers=c.headers).status_code in {
        401,
        503,
    }


def test_duplicate_delivery_during_native_write_sees_committed_owner_and_never_writes(
    output_http, monkeypatch
):
    c = output_http
    native = outputs.write_attempt_output
    duplicate = []

    def while_writing(*args, **kwargs):
        row = saved(c)
        assert row.status == "issuing" and row.issuance_started_at is not None
        duplicate.append(upload(c).status_code)
        return native(*args, **kwargs)

    monkeypatch.setattr(outputs, "write_attempt_output", while_writing)
    result = upload(c)
    assert result.status_code == 200, result.text
    assert duplicate == [503] and len(c.writes) == 1


def test_revocation_during_actual_asgi_body_stream_prevents_native_claim(output_http):
    c = output_http

    async def send():
        async def body():
            yield c.payload[:1]
            row = saved(c)
            assert row.status == "pending" and row.issuance_started_at is None
            with c.case.factory() as db, db.begin():
                grants.revoke_attempt_grants(db, c.case.plan.attempt_id)
            yield c.payload[1:]

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=c.client.app), base_url="https://broker.test"
        ) as client:
            return await client.put(
                f"{c.base}/{c.role}/{c.digest}",
                content=body(),
                headers={
                    **c.headers,
                    "X-Request-ID": str(c.nonce),
                    "Content-Length": str(len(c.payload)),
                },
            )

    result = asyncio.run(send())
    assert result.status_code == 401, result.text
    row = saved(c)
    assert row.status == "revoked" and row.issuance_started_at is None and not c.writes


@pytest.mark.parametrize("fault", ["other_attempt", "other_pod", "user_token"])
def test_output_issuance_rejects_another_principal_before_native_io(output_http, fault):
    c = output_http
    if fault == "other_attempt":
        c.base = c.base.replace(str(c.case.plan.attempt_id), str(uuid.uuid4()))
    elif fault == "other_pod":
        c.case.auth.create_token_review.return_value.status.user.extra[
            "authentication.kubernetes.io/pod-uid"
        ] = [str(uuid.uuid4())]
    else:
        c.headers = {"Authorization": "Bearer user-session-token"}
    assert upload(c).status_code == 401
    assert not c.writes


def test_same_pod_token_rotation_replays_receipt_without_changing_original_grant(output_http):
    import jwt

    c = output_http
    initial = upload(c)
    assert initial.status_code == 200
    before = saved(c)
    token = jwt.encode(
        {"exp": int(datetime.now(UTC).timestamp()) + 600, "nonce": "rotated"},
        "synthetic-signing-secret-long-enough",
        algorithm="HS256",
    )
    c.headers = {"Authorization": "Bearer " + token}
    reply = c.client.get(c.base + f"/receipts/{c.nonce}", headers=c.headers)
    assert reply.status_code == 200 and reply.json() == initial.json()
    after = saved(c)
    assert (before.grant_digest, before.token_sha256, before.expires_at) == (
        after.grant_digest,
        after.token_sha256,
        after.expires_at,
    )
    assert len(c.writes) == 1


def test_known_native_completion_after_logical_deadline_is_retained_without_disclosure(
    output_http, monkeypatch
):
    c = output_http
    native = outputs.write_attempt_output

    def late(*args, **kwargs):
        result = native(*args, **kwargs)
        deadline = saved(c).expires_at

        class AfterDeadline(datetime):
            @classmethod
            def now(cls, tz=None):
                return deadline + timedelta(seconds=1)

        monkeypatch.setattr(grants, "datetime", AfterDeadline)
        return result

    monkeypatch.setattr(outputs, "write_attempt_output", late)
    assert upload(c).status_code == 503
    row = saved(c)
    with c.case.factory() as db:
        assert (
            grants.output_completion(db, db.get(WorkflowAttemptCredentialGrant, row.id)) is not None
        )
        assert not grants.has_unretired_attempt_credentials(db, row.attempt_id)
    assert c.client.get(c.base + f"/receipts/{c.nonce}", headers=c.headers).status_code == 401
    assert upload(c).status_code == 401 and len(c.writes) == 1


def test_legacy_worker_upload_routes_are_retired(output_http, request):
    c = output_http
    old = request.getfixturevalue(c.case.stage + "_control_case")[0]
    path = f"model/{c.digest}" if c.role == "pipeline" else "result"
    response = old.put(path, content=c.payload)
    assert response.status_code == 410 and response.headers["cache-control"] == "no-store"
    assert saved(c) is None and not c.writes


@pytest.mark.parametrize("same_nonce", [False, True])
def test_committed_preparation_serializes_one_output_role_across_connections(
    output_http, monkeypatch, same_nonce
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from automl_api.services.workflow_state import StaleFence

    c = output_http
    proof = outputs.admit_attempt(
        c.case.factory, c.case.plan.attempt_id, c.case.token, c.case.native
    )
    barrier = Barrier(2)

    def admitted(*_):
        barrier.wait(timeout=10)
        return proof

    monkeypatch.setattr(outputs, "admit_attempt", admitted)

    def prepare(nonce):
        try:
            value = outputs.prepare_output_grant(
                c.case.factory,
                c.case.plan.attempt_id,
                c.case.token,
                c.case.native,
                {
                    "request_id": nonce,
                    "role": c.role,
                    "byte_size": len(c.payload),
                    "sha256": c.digest,
                },
                store_factory=lambda: c.case.store,
            )
            return value["grant_id"]
        except StaleFence:
            return None

    nonces = [c.nonce, c.nonce if same_nonce else uuid.uuid4()]
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(prepare, nonces))
    assert sum(value is not None for value in outcomes) == (2 if same_nonce else 1)
    assert len({value for value in outcomes if value is not None}) == 1
    row = saved(c)
    assert row.status == "pending" and row.issuance_owner is None and not c.writes


def test_read_v1_plan_can_read_but_cannot_use_either_upload_route(output_http):
    c = output_http
    with c.case.factory() as db, db.begin():
        for key in (c.case.stage + "_plan", c.case.stage + "_submission"):
            event = db.scalar(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.case.plan.attempt_id,
                    WorkflowEvent.event_key == key,
                )
            )
            field = "credential_protocol" if key.endswith("_plan") else "object_grant_protocol"
            event.payload = {**event.payload, field: "pod-bound-read-v1"}
            event.result_digest = canonical_request_hash(event.payload)
    read = c.client.post(
        c.base + "/read",
        headers=c.headers,
        json={**c.case.request, "request_id": str(uuid.uuid4())},
    )
    assert read.status_code == 200, read.text
    assert upload(c).status_code == 401
    assert saved(c) is None and not c.writes


def test_s3_single_put_limit_is_checked_before_receiving_large_body(output_http):
    c = output_http
    response = upload(c, headers={"Content-Length": "5000000001"})
    assert response.status_code == 503
    assert saved(c) is None and not c.writes


def test_grant_declaration_does_not_apply_the_s3_limit_to_other_providers():
    request = outputs.OutputGrantRequest(
        request_id=uuid.uuid4(), role="pipeline", byte_size=5_000_000_001, sha256="f" * 64
    )
    assert request.byte_size == 5_000_000_001


def publication(c, request):
    original = request.getfixturevalue(c.case.stage + "_control_case")
    if c.role == "pipeline":
        plan = c.case.plan
        return original[0], {
            "frozen_pipeline_uri": c.case.store.uri_for_key(
                f"projects/{plan.project_id}/scopes/{plan.scope_id}/refit/{plan.attempt_id}/{c.digest}/pipeline.joblib"
            ),
            "frozen_pipeline_digest": c.digest,
            "byte_size": len(c.payload),
            "refit_policy_digest": plan.policy_digest,
            "row_count": sum(p.rows for p in plan.partitions),
        }
    authority, token = original[6:8]
    path = f"allocations/{c.case.plan.allocation_id}"
    opened = {"provider_manifest_digest": c.case.plan.final_manifest.digest}
    headers = {"Authorization": "Bearer " + token}
    assert authority.post(path + "/open", json=opened, headers=headers).status_code == 200
    committed = authority.post(
        path + "/commit", json={**opened, "result_digest": c.digest}, headers=headers
    )
    assert committed.status_code == 200, committed.text
    return original[0], committed.json()


@pytest.mark.parametrize("versioned", [False, True])
def test_worker_publication_requires_the_committed_native_completion(
    output_http, monkeypatch, request, versioned
):
    from automl_api.storage.contracts import ObjectMetadata

    c = output_http
    control, payload = publication(c, request)
    native = outputs.write_attempt_output
    premature = []

    def before_completion(*args, **kwargs):
        stored = native(*args, **kwargs)
        premature.append(control.post("publish", json=payload).status_code)
        if not versioned:
            return ObjectMetadata(uri=stored.uri, byte_size=stored.byte_size)
        return stored

    monkeypatch.setattr(outputs, "write_attempt_output", before_completion)
    uploaded = upload(c)
    assert premature == [409], (uploaded.status_code, uploaded.text, c.writes)
    assert uploaded.status_code == (200 if versioned else 503), uploaded.text
    if versioned:
        assert control.post("publish", json=payload).status_code == 200
    else:
        assert control.post("publish", json=payload).status_code == 409


def test_worker_revocation_blocks_metadata_publication_after_native_completion(
    output_http, request
):
    c = output_http
    control, payload = publication(c, request)
    assert upload(c).status_code == 200
    with c.case.factory() as db, db.begin():
        grants.revoke_attempt_grants(db, c.case.plan.attempt_id)
    assert control.post("publish", json=payload).status_code == 409


def test_publication_rechecks_revocation_after_native_read_and_preserves_trusted_recovery(
    output_http, monkeypatch, request
):
    from contextlib import contextmanager

    from automl_api.models.enums import AttemptStatus
    from automl_api.models.workflows import WorkflowAttempt

    c = output_http
    control, payload = publication(c, request)
    assert upload(c).status_code == 200
    original = c.backend.open_stream
    revoked = []
    observer_connection = request.getfixturevalue(c.case.stage + "_control_case")[1].get_bind()

    @contextmanager
    def read_then_revoke(*args, **kwargs):
        assert c.case.engine.pool.checkedout() == 0
        assert not observer_connection.in_transaction()
        with original(*args, **kwargs) as stream:
            yield stream
        if not revoked:
            with c.case.factory() as db, db.begin():
                grants.revoke_attempt_grants(db, c.case.plan.attempt_id)
            revoked.append(True)

    monkeypatch.setattr(c.backend, "open_stream", read_then_revoke)
    assert control.post("publish", json=payload).status_code == 409
    assert revoked == [True]
    with c.case.factory() as db:
        assert db.get(WorkflowAttempt, c.case.plan.attempt_id).status == AttemptStatus.RUNNING
    if c.role == "result":
        from automl_api.api.routes import evaluation_control
        from automl_api.services.evaluation_publication import recover_evaluation_result

        result = recover_evaluation_result(
            c.case.factory,
            c.case.plan,
            saved(c).fencing_token,
            c.backend,
            commit=lambda _: payload,
            authority_public_key=evaluation_control.authority_public_key(),
        )
        assert result["sha256"] == c.digest


def test_owned_database_connection_loss_after_native_write_preserves_unknown_custody(
    output_http, monkeypatch
):
    from sqlalchemy import text

    c = output_http
    append = outputs._append
    killed = []

    def lose_completion_connection(db, attempt, key, payload):
        if key.startswith("attempt.output.completed."):
            # Terminate only this test's active connection to the explicitly owned database.
            assert c.case.engine.url.port == 55439
            target_database, target_pid = db.execute(
                text("SELECT current_database(), pg_backend_pid()")
            ).one()
            assert target_database == "automl_branch_0e6a232bc7_tests"
            with c.case.engine.connect() as control:
                own_database, own_pid = control.execute(
                    text("SELECT current_database(), pg_backend_pid()")
                ).one()
                assert own_database == target_database and own_pid != target_pid
                assert control.scalar(
                    text("SELECT pg_terminate_backend(:pid)"), {"pid": target_pid}
                )
            killed.append(target_pid)
        return append(db, attempt, key, payload)

    monkeypatch.setattr(outputs, "_append", lose_completion_connection)
    assert upload(c).status_code == 503
    row = saved(c)
    assert len(killed) == len(c.writes) == 1 and row.status == "unknown"
    assert c.backend.exists(row.object_uri)
    assert upload(c).status_code == 503
    assert c.client.get(c.base + f"/receipts/{c.nonce}", headers=c.headers).status_code == 503
    assert len(c.writes) == 1
    with c.case.factory() as db:
        assert grants.output_completion(db, db.get(WorkflowAttemptCredentialGrant, row.id)) is None
