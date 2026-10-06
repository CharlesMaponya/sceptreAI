from __future__ import annotations

import hashlib
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from automl_api.models.qualification import FinalTestAllocation
from automl_api.qualification_control import Principal, create_app
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, event, func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker


@pytest.fixture
def authority_api():
    url = os.environ.get(
        "DATABASE_URL", "postgresql+psycopg://automl:automl@127.0.0.1:55432/automl"
    )
    engine = create_engine(url, pool_size=6, max_overflow=0)
    factory = sessionmaker(engine, expire_on_commit=False)
    scope = uuid.uuid4()
    project = f"authority-test-{scope}"
    identities = []
    for name, role, provider, project_name, scope_id in (
        ("allocator", "allocator", None, project, None),
        ("aws", "provider", "aws", project, scope),
        ("gcp", "provider", "gcp", project, scope),
        ("azure", "provider", "azure", project, scope),
        ("other-project", "provider", "aws", "other", scope),
        ("other-scope", "provider", "aws", project, uuid.uuid4()),
    ):
        identities.append(
            Principal(
                token_sha256=hashlib.sha256(name.encode()).hexdigest(),
                role=role,
                provider=provider,
                project_reference=project_name,
                scope_id=scope_id,
                evaluator_attempt_id=scope,
                frozen_pipeline_digest="d" * 64,
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )
        )
    kwargs = dict(
        session_factory=factory, principals=identities, signing_secret=Ed25519PrivateKey.generate()
    )
    with TestClient(create_app(**kwargs)) as client:
        yield client, scope, kwargs
    with factory() as db, db.begin():
        db.execute(
            delete(FinalTestAllocation).where(FinalTestAllocation.project_reference == project)
        )
    engine.dispose()


def headers(identity):
    return {"Authorization": f"Bearer {identity}"}


def allocate(client, scope):
    payload = dict(
        split_digest=hashlib.sha256(str(scope).encode()).hexdigest(),
        scope_id=str(scope),
        canonical_provider="aws",
        provider_manifest_digest="a" * 64,
    )
    response = client.post("/allocations", json=payload, headers=headers("allocator"))
    assert response.status_code == 200, response.text
    replay = client.post("/allocations", json=payload, headers=headers("allocator"))
    assert response.json() == replay.json()
    return response.json()["allocation_id"]


def test_scope_allocation_race_returns_conflicts_and_preserves_winner(authority_api):
    client, scope, kwargs = authority_api
    barrier = Barrier(6)
    project = f"authority-test-{scope}"

    def before_flush(session, *_args):
        if any(isinstance(row, FinalTestAllocation) and row.project_reference == project
               for row in session.new):
            # Force all initial lookups to miss before racing the unique scope constraint.
            barrier.wait(timeout=15)

    payloads = [dict(split_digest=hashlib.sha256(f"{scope}-{i}".encode()).hexdigest(),
                     scope_id=str(scope), canonical_provider="aws",
                     provider_manifest_digest="a" * 64) for i in range(6)]
    event.listen(Session, "before_flush", before_flush)
    try:
        with ThreadPoolExecutor(max_workers=6) as pool:
            responses = list(pool.map(lambda body: client.post(
                "/allocations", json=body, headers=headers("allocator")), payloads))
    finally:
        event.remove(Session, "before_flush", before_flush)
    assert sorted(r.status_code for r in responses) == [200, 409, 409, 409, 409, 409]
    winner = next(i for i, response in enumerate(responses) if response.status_code == 200)
    assert client.post("/allocations", json=payloads[winner],
                       headers=headers("allocator")).json() == responses[winner].json()
    # The same deterministic conflict is required outside the insert race as well.
    loser = (winner + 1) % len(payloads)
    assert client.post("/allocations", json=payloads[loser],
                       headers=headers("allocator")).status_code == 409
    with kwargs["session_factory"]() as db:
        assert db.scalar(select(func.count()).select_from(FinalTestAllocation).where(
            FinalTestAllocation.scope_id == scope)) == 1


def test_allocator_recovers_signed_terminal_receipt_without_worker_identity(authority_api):
    from types import SimpleNamespace

    from automl_api.services.final_test_authority import verify_receipt

    client, scope, kwargs = authority_api
    allocation_id = allocate(client, scope)
    path = f"/allocations/{allocation_id}"
    opened = {"provider_manifest_digest": "a" * 64}
    assert client.post(path + "/open", headers=headers("aws"), json=opened).status_code == 200
    committed = client.post(path + "/commit", headers=headers("aws"), json={
        **opened, "result_digest": "b" * 64})
    assert committed.status_code == 200
    assert client.get(path).status_code == 401
    assert client.get(path, headers=headers("aws")).status_code == 403
    # The replacement authority has no worker credentials at all.
    only_allocator = {**kwargs, "principals": [p for p in kwargs["principals"]
                                              if p.role == "allocator"]}
    with TestClient(create_app(**only_allocator)) as restarted:
        recovered = restarted.get(path, headers=headers("allocator"))
        assert recovered.status_code == 200 and recovered.headers["cache-control"] == "no-store"
        state = recovered.json()
        assert state["status"] == "committed" and state["cas_version"] == 2
        assert state["result_digest"] == "b" * 64
        receipt = next(r for r in state["receipts"] if r["operation"] == "commit")
        assert receipt == committed.json()
        assert verify_receipt(SimpleNamespace(**receipt), kwargs["signing_secret"].public_key())
        assert "urls" not in state and "access_token" not in state
    other = kwargs["principals"][0].model_copy(update={"project_reference": "other-project"})
    with TestClient(create_app(**{**kwargs, "principals": [other]})) as unrelated:
        assert unrelated.get(path, headers=headers("allocator")).status_code == 404


def test_http_identity_isolation_and_input_validation(authority_api):
    client, scope, _ = authority_api
    allocation_id = allocate(client, scope)
    path = f"/allocations/{allocation_id}/open"
    payload = {"provider_manifest_digest": "a" * 64}
    assert client.post(path, json=payload).status_code == 401
    for name, code in (
        ("allocator", 403),
        ("gcp", 403),
        ("azure", 403),
        ("other-project", 404),
        ("other-scope", 403),
    ):
        assert client.post(path, json=payload, headers=headers(name)).status_code == code
    assert (
        client.post(path, json={**payload, "provider": "aws"}, headers=headers("gcp")).status_code
        == 422
    )
    assert (
        client.post(
            path, json={"provider_manifest_digest": "invalid"}, headers=headers("aws")
        ).status_code
        == 422
    )
    assert client.post(path, json=payload, headers=headers("aws")).status_code == 200


def test_provider_race_restart_and_conflicting_terminal_writes(authority_api):
    client, scope, kwargs = authority_api
    allocation_id = allocate(client, scope)
    path = f"/allocations/{allocation_id}"
    payload = {"provider_manifest_digest": "a" * 64}

    def open_as(provider):
        return client.post(path + "/open", json=payload, headers=headers(provider))

    with ThreadPoolExecutor(max_workers=3) as pool:
        responses = list(pool.map(open_as, ("aws", "gcp", "azure")))
    assert [response.status_code for response in responses] == [200, 403, 403]
    # A fresh service instance recovers the same durable receipt, not a new open.
    with TestClient(create_app(**kwargs)) as restarted:
        replay = restarted.post(path + "/open", json=payload, headers=headers("aws"))
        assert replay.json() == responses[0].json()

        def commit(digest):
            return restarted.post(
                path + "/commit", json={**payload, "result_digest": digest}, headers=headers("aws")
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(commit, ("b" * 64, "c" * 64)))
        assert sorted(result.status_code for result in results) == [200, 409]
        assert (
            restarted.post(
                path + "/fail",
                json={**payload, "reason": "late failure", "expected_cas_version": 2},
                headers=headers("aws"),
            ).status_code
            == 409
        )


def test_store_failure_and_expired_identity_fail_closed(authority_api):
    _, scope, kwargs = authority_api

    def unavailable():
        raise OperationalError("connect", {}, Exception("unavailable"))

    with TestClient(create_app(**{**kwargs, "session_factory": unavailable})) as client:
        response = client.post(
            f"/allocations/{scope}/open",
            json={"provider_manifest_digest": "a" * 64},
            headers=headers("aws"),
        )
        assert response.status_code == 503
        assert "signature" not in response.json()
    expired = kwargs["principals"][1].model_copy(
        update={
            "expires_at": datetime.now(UTC) - timedelta(seconds=1),
        }
    )
    with TestClient(create_app(**{**kwargs, "principals": [expired]})) as client:
        assert (
            client.post(
                f"/allocations/{scope}/open",
                json={"provider_manifest_digest": "a" * 64},
                headers=headers("aws"),
            ).status_code
            == 401
        )


def test_preloaded_allocation_cannot_overwrite_new_terminal_state(authority_api):
    from automl_api.services import final_test_authority as authority
    from automl_api.services.workflow_state import InvalidTransition

    client, scope, kwargs = authority_api
    allocation_id = uuid.UUID(allocate(client, scope))
    payload = {"provider_manifest_digest": "a" * 64}
    path = f"/allocations/{allocation_id}"
    with kwargs["session_factory"]() as stale:
        cached = stale.get(FinalTestAllocation, allocation_id)
        assert cached.status == "allocated"
        assert client.post(path + "/open", json=payload, headers=headers("aws")).status_code == 200
        assert (
            client.post(
                path + "/commit",
                json={**payload, "result_digest": "b" * 64},
                headers=headers("aws"),
            ).status_code
            == 200
        )
        with pytest.raises(InvalidTransition):
            authority.fail_allocation(
                stale,
                allocation_id=allocation_id,
                provider="aws",
                provider_manifest_digest="a" * 64,
                request_digest="c" * 64,
                signing_secret=Ed25519PrivateKey.generate(),
                reason="stale writer",
                expected_cas_version=0,
                evaluator_attempt_id=scope,
                frozen_pipeline_digest="d" * 64,
            )
        assert cached.status == "committed"


def test_failure_before_commit_rolls_back_open_and_receipt(authority_api):
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    client, scope, kwargs = authority_api
    allocation_id = allocate(client, scope)

    class FailingSession(Session):
        pass

    def disconnect_before_commit(session):
        pid = session.connection().exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
        with kwargs["session_factory"].kw["bind"].connect() as killer:
            assert killer.exec_driver_sql("SELECT pg_terminate_backend(%s)", (pid,)).scalar_one()

    event.listen(FailingSession, "before_commit", disconnect_before_commit)
    failing_factory = sessionmaker(
        kwargs["session_factory"].kw["bind"], class_=FailingSession, expire_on_commit=False
    )
    payload = {"provider_manifest_digest": "a" * 64}
    path = f"/allocations/{allocation_id}/open"
    with TestClient(create_app(**{**kwargs, "session_factory": failing_factory})) as failing:
        assert failing.post(path, json=payload, headers=headers("aws")).status_code == 503
    with kwargs["session_factory"]() as db:
        row = db.get(FinalTestAllocation, uuid.UUID(allocation_id))
        assert row.status == "allocated"
        assert row.cas_version == 0
    assert client.post(path, json=payload, headers=headers("aws")).status_code == 200


def test_authority_chart_requires_digest_and_renders_isolation():
    import subprocess
    from pathlib import Path

    import yaml

    chart = str(Path(__file__).parents[1] / "services/qualification-control/chart")
    command = ["helm", "template", "authority", chart]
    assert subprocess.run(command, capture_output=True).returncode != 0
    values = {
        "image": "registry.example/authority@sha256:" + "a" * 64,
        "databaseSecret": "authority-db",
        "identitySecret": "authority-identities",
        "tlsSecret": "authority-tls",
        "databaseCaSecret": "authority-ca",
        "databaseCidr": "10.0.0.1/32",
    }
    for key, value in values.items():
        command.extend(["--set-string", f"{key}={value}"])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    resources = {item["kind"]: item for item in yaml.safe_load_all(result.stdout)}
    assert resources["Deployment"]["spec"]["strategy"] == {
        "type": "Recreate",
        "rollingUpdate": None,
    }
    pod = resources["Deployment"]["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    container = pod["containers"][0]
    assert "--ssl-keyfile" in container["command"]
    assert container["readinessProbe"]["httpGet"]["scheme"] == "HTTPS"
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    policy = resources["NetworkPolicy"]["spec"]
    assert policy["policyTypes"] == ["Ingress", "Egress"]
    assert policy["egress"][1]["to"][0]["ipBlock"]["cidr"] == "10.0.0.1/32"

    both = command + ["--set", "databasePodSelector.app=authority-db"]
    assert subprocess.run(both, capture_output=True).returncode != 0
    selected = subprocess.run(
        both + ["--set-string", "databaseCidr="], capture_output=True, text=True, check=True
    )
    selected_policy = next(
        item for item in yaml.safe_load_all(selected.stdout) if item["kind"] == "NetworkPolicy"
    )
    assert selected_policy["spec"]["egress"][1]["to"] == [
        {"podSelector": {"matchLabels": {"app": "authority-db"}}}
    ]


@pytest.mark.parametrize("invalid_key", ["hmac-only-secret" * 4, "public-key"])
def test_service_rejects_shared_secrets_and_public_signing_keys(invalid_key):
    from cryptography.hazmat.primitives import serialization

    principal = Principal(
        token_sha256="a" * 64,
        role="allocator",
        project_reference="test",
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    if invalid_key == "public-key":
        invalid_key = (
            Ed25519PrivateKey.generate()
            .public_key()
            .public_bytes(
                serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
            )
            .decode()
        )
    with pytest.raises(ValueError):
        create_app(session_factory=lambda: None, principals=[principal], signing_secret=invalid_key)


@pytest.mark.parametrize("changed_field", ["evaluator_attempt_id", "frozen_pipeline_digest"])
def test_opened_allocation_rejects_changed_evaluator_identity(authority_api, changed_field):
    from types import SimpleNamespace

    from automl_api.services.final_test_authority import verify_receipt

    client, scope, kwargs = authority_api
    allocation_id = allocate(client, scope)
    path = f"/allocations/{allocation_id}"
    body = {"provider_manifest_digest": "a" * 64}
    opened = client.post(path + "/open", json=body, headers=headers("aws"))
    assert opened.status_code == 200
    receipt = opened.json()
    assert receipt["payload"]["evaluator_attempt_id"] == str(scope)
    assert receipt["payload"]["frozen_pipeline_digest"] == "d" * 64
    assert verify_receipt(SimpleNamespace(**receipt), kwargs["signing_secret"].public_key())
    # Same provider/project/scope/token, but a newly registered evaluator or model.
    identity = kwargs["principals"][1].model_copy(
        update={
            changed_field: uuid.uuid4() if changed_field == "evaluator_attempt_id" else "e" * 64,
        }
    )
    with TestClient(create_app(**{**kwargs, "principals": [identity]})) as other:
        for operation, extra in (
            ("open", {}),
            ("commit", {"result_digest": "b" * 64}),
            ("fail", {"reason": "another evaluator failed"}),
        ):
            assert (
                other.post(
                    path + "/" + operation, json={**body, **extra}, headers=headers("aws")
                ).status_code
                == 403
            )
    # Identity fields are never accepted as a request-body override.
    assert (
        client.post(
            path + "/open",
            json={**body, "evaluator_attempt_id": str(scope)},
            headers=headers("aws"),
        ).status_code
        == 422
    )
    with kwargs["session_factory"]() as db:
        allocation = db.get(FinalTestAllocation, uuid.UUID(allocation_id))
        assert allocation.status == "opened" and allocation.cas_version == 1
    result = client.post(
        path + "/commit", json={**body, "result_digest": "b" * 64}, headers=headers("aws")
    )
    assert result.status_code == 200
    assert result.json()["payload"]["evaluator_attempt_id"] == str(scope)


def test_two_evaluators_race_for_one_open(authority_api):
    client, scope, kwargs = authority_api
    allocation_id = allocate(client, scope)
    other_identity = kwargs["principals"][1].model_copy(
        update={
            "evaluator_attempt_id": uuid.uuid4(),
        }
    )
    with TestClient(create_app(**{**kwargs, "principals": [other_identity]})) as other:

        def open_with(caller):
            return caller.post(
                f"/allocations/{allocation_id}/open",
                json={"provider_manifest_digest": "a" * 64},
                headers=headers("aws"),
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(open_with, (client, other)))
        assert sorted(r.status_code for r in results) == [200, 403]
        winner = next(r for r in results if r.status_code == 200).json()
        assert winner["payload"]["evaluator_attempt_id"] in {
            str(scope),
            str(other_identity.evaluator_attempt_id),
        }
