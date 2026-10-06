from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import jwt
import pytest
from automl_api.qualification_control import Principal, create_app
from automl_api.services.final_evaluator_identity import AUDIENCE, ISSUER
from automl_api.services.final_test_authority import ProviderRejected, verify_receipt
from automl_api.services.final_test_credentials import issue_final_grant
from fastapi.testclient import TestClient
from test_champion_credentials import grant_client
from test_qualification_control import headers

pytest_plugins = ["test_qualification_control"]


def publish(client, path):
    payload = {
        "refit_attempt_id": str(uuid.uuid4()),
        "frozen_pipeline_digest": "c" * 64,
        "frozen_pipeline_uri": "s3://models/frozen/pipeline.joblib",
        "refit_policy_digest": "e" * 64,
    }
    response = client.post(path + "/refit", headers=headers("allocator"), json=payload)
    assert response.status_code == 200, response.text
    return payload, response.json()


def register(client, path, attempt=None, generation=0):
    payload = {
        "evaluator_attempt_id": str(attempt or uuid.uuid4()),
        "expected_generation": generation,
    }
    return payload, client.post(path + "/evaluators", headers=headers("allocator"), json=payload)


def test_refit_registration_grant_and_restart(authority_api):
    _, scope, kwargs = authority_api
    mint = Mock(return_value=["https://objects.test/inputs", "https://objects.test/labels"])
    client, manifest, path = grant_client(scope, kwargs, mint)
    assert register(client, path)[1].status_code == 409
    payload, receipt = publish(client, path)
    assert verify_receipt(SimpleNamespace(**receipt), kwargs["signing_secret"].public_key())
    assert (
        client.post(path + "/refit", headers=headers("allocator"), json=payload).json() == receipt
    )
    assert client.post(path + "/refit", headers=headers("aws"), json=payload).status_code == 403
    assert (
        client.post(
            path + "/refit",
            headers=headers("allocator"),
            json={
                **payload,
                "frozen_pipeline_digest": "f" * 64,
            },
        ).status_code
        == 409
    )
    opened = {"provider_manifest_digest": manifest.digest}
    assert client.post(path + "/open", headers=headers("aws"), json=opened).status_code == 403
    registration, response = register(client, path)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    token = response.json()["access_token"]
    claims = jwt.decode(
        token,
        kwargs["signing_secret"].public_key(),
        algorithms=["EdDSA"],
        audience=AUDIENCE,
        issuer=ISSUER,
    )
    assert claims["frozen_pipeline_digest"] == payload["frozen_pipeline_digest"]
    assert claims["evaluator_attempt_id"] == registration["evaluator_attempt_id"]
    assert claims["scope_id"] == str(scope)
    assert claims["exp"] - claims["iat"] <= 7200
    with TestClient(create_app(**kwargs, manifests=[manifest], mint=mint)) as restarted:
        assert (
            restarted.post(
                path + "/evaluators", headers=headers("allocator"), json=registration
            ).json()
            == response.json()
        )
        assert (
            restarted.post(path + "/credentials", headers=headers("aws"), json=opened).status_code
            == 403
        )
        granted = restarted.post(path + "/credentials", headers=headers(token), json=opened)
        assert granted.status_code == 200, granted.text
        assert granted.json()["receipt"]["payload"]["frozen_pipeline_digest"] == "c" * 64
        assert register(restarted, path, generation=1)[1].status_code == 409
        assert (
            restarted.post(
                path + "/commit",
                headers=headers(token),
                json={
                    **opened,
                    "result_digest": "b" * 64,
                },
            ).status_code
            == 200
        )
        assert (
            restarted.post(path + "/credentials", headers=headers(token), json=opened).status_code
            == 409
        )
    mint.assert_called_once()


def test_registration_race_revokes_preauthenticated_old_generation(authority_api):
    _, scope, kwargs = authority_api
    mint = Mock(return_value=["https://objects.test/inputs", "https://objects.test/labels"])
    client, manifest, path = grant_client(scope, kwargs, mint)
    publish(client, path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        registrations = list(pool.map(lambda _: register(client, path), range(2)))
    assert sorted(response.status_code for _, response in registrations) == [200, 409]
    old_payload, old_response = next(pair for pair in registrations if pair[1].status_code == 200)
    old_token = old_response.json()["access_token"]
    old_claims = jwt.decode(
        old_token,
        kwargs["signing_secret"].public_key(),
        algorithms=["EdDSA"],
        audience=AUDIENCE,
        issuer=ISSUER,
    )
    stale = Principal(
        token_sha256="a" * 64,
        role="provider",
        project_reference=manifest.project_reference,
        provider="aws",
        scope_id=scope,
        evaluator_attempt_id=old_payload["evaluator_attempt_id"],
        frozen_pipeline_digest="c" * 64,
        expires_at=datetime.fromtimestamp(old_claims["exp"], UTC),
    )
    _, replacement = register(client, path, generation=1)
    assert replacement.status_code == 200
    assert (
        client.post(
            path + "/evaluators", headers=headers("allocator"), json=old_payload
        ).status_code
        == 409
    )
    assert register(client, path, generation=1)[1].status_code == 409
    request = {"provider_manifest_digest": manifest.digest}
    assert (
        client.post(path + "/credentials", headers=headers(old_token), json=request).status_code
        == 401
    )
    # Authentication could have completed just before replacement: the row-lock
    # check must still reject that old principal before consuming or minting.
    with pytest.raises(ProviderRejected):
        issue_final_grant(
            kwargs["session_factory"],
            allocation_id=uuid.UUID(path.rsplit("/", 1)[1]),
            principal=stale,
            manifest=manifest,
            signing_key=kwargs["signing_secret"],
            mint=mint,
        )
    mint.assert_not_called()
    token = replacement.json()["access_token"]
    assert (
        client.post(path + "/credentials", headers=headers(token), json=request).status_code == 200
    )
    mint.assert_called_once()


def test_invalid_dynamic_tokens_never_mint(authority_api):
    _, scope, kwargs = authority_api
    mint = Mock(return_value=["https://objects.test/inputs", "https://objects.test/labels"])
    client, manifest, path = grant_client(scope, kwargs, mint)
    publish(client, path)
    _, registered = register(client, path)
    token = registered.json()["access_token"]
    claims = jwt.decode(
        token,
        kwargs["signing_secret"].public_key(),
        algorithms=["EdDSA"],
        audience=AUDIENCE,
        issuer=ISSUER,
    )
    variants = [jwt.encode(claims, "not-the-authority-key-but-long-enough", algorithm="HS256")]
    for change in (
        {"exp": 1},
        {"aud": "another-service"},
        {"generation": 2},
        {"frozen_pipeline_digest": "d" * 64},
        {"project_reference": "another-project"},
    ):
        variants.append(
            jwt.encode({**claims, **change}, kwargs["signing_secret"], algorithm="EdDSA")
        )
    for bad_token in variants:
        response = client.post(
            path + "/credentials",
            headers=headers(bad_token),
            json={
                "provider_manifest_digest": manifest.digest,
            },
        )
        assert response.status_code == 401
    mint.assert_not_called()
