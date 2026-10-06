"""Recovery uses allocator authority, never renews a final-data capability."""

import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import jwt
import pytest
from automl_api.qualification_control import create_app
from automl_api.services.final_test_authority import verify_receipt
from fastapi.testclient import TestClient
from test_champion_credentials import grant_client
from test_champion_identity import publish, register
from test_qualification_control import authority_api as authority_api
from test_qualification_control import headers


@pytest.mark.parametrize("operation", ["commit", "fail"])
def test_expired_worker_recovery_is_bound_and_replayable(authority_api, operation, monkeypatch):
    _, scope, kwargs = authority_api
    mint = Mock(return_value=["https://objects.test/inputs", "https://objects.test/labels"])
    client, manifest, path = grant_client(scope, kwargs, mint)
    publish(client, path)
    registration, response = register(client, path)
    token = response.json()["access_token"]
    body = {"provider_manifest_digest": manifest.digest}
    assert client.post(path + "/credentials", json=body, headers=headers(token)).status_code == 200

    class FutureClock:
        @staticmethod
        def now(tz):
            return datetime.now(tz) + timedelta(hours=3)

    request = {**body, "expected_cas_version": 1}
    request.update(
        {"result_digest": "b" * 64} if operation == "commit" else {"reason": "worker lost"}
    )
    with monkeypatch.context() as clock:
        clock.setattr(jwt.api_jwt, "datetime", FutureClock)
        assert (
            client.post(path + "/" + operation, json=request, headers=headers(token)).status_code
            == 401
        )
    recovery = {
        **request,
        "evaluator_attempt_id": registration["evaluator_attempt_id"],
        "frozen_pipeline_digest": "c" * 64,
        "generation": 1,
    }
    endpoint = path + "/recovery/" + operation
    assert client.post(endpoint, json=recovery, headers=headers(token)).status_code == 403
    for change in ({"generation": 2}, {"frozen_pipeline_digest": "f" * 64}):
        assert (
            client.post(
                endpoint, json={**recovery, **change}, headers=headers("allocator")
            ).status_code
            == 409
        )
    other = kwargs["principals"][0].model_copy(
        update={
            "project_reference": "unrelated",
            "token_sha256": hashlib.sha256(b"other-allocator").hexdigest(),
            "expires_at": datetime.now(UTC) + timedelta(minutes=5),
        }
    )
    with TestClient(
        create_app(
            **{**kwargs, "principals": [*kwargs["principals"], other]},
            manifests=[manifest],
            mint=mint,
        )
    ) as restarted:
        assert (
            restarted.post(endpoint, json=recovery, headers=headers("other-allocator")).status_code
            == 404
        )
        with monkeypatch.context() as clock:
            clock.setattr(jwt.api_jwt, "datetime", FutureClock)
            result = restarted.post(endpoint, json=recovery, headers=headers("allocator"))
        assert result.status_code == 200, result.text
        assert verify_receipt(
            SimpleNamespace(**result.json()), kwargs["signing_secret"].public_key()
        )
        assert (
            restarted.post(endpoint, json=recovery, headers=headers("allocator")).json()
            == result.json()
        )
        # The same worker request can recover a lost acknowledgement too.
        assert (
            restarted.post(path + "/" + operation, json=request, headers=headers(token)).json()
            == result.json()
        )
        changed = {"result_digest": "f" * 64} if operation == "commit" else {"reason": "changed"}
        assert (
            restarted.post(
                endpoint, json={**recovery, **changed}, headers=headers("allocator")
            ).status_code
            == 409
        )
        assert (
            restarted.post(path + "/credentials", json=body, headers=headers(token)).status_code
            == 409
        )
    mint.assert_called_once()


def test_recovery_cannot_fail_replacement_or_commit_unopened_data(authority_api):
    _, scope, kwargs = authority_api
    mint = Mock()
    client, manifest, path = grant_client(scope, kwargs, mint)
    publish(client, path)
    old, _ = register(client, path)
    replacement, response = register(client, path, generation=1)
    assert response.status_code == 200
    failure = dict(
        provider_manifest_digest=manifest.digest,
        expected_cas_version=0,
        evaluator_attempt_id=old["evaluator_attempt_id"],
        frozen_pipeline_digest="c" * 64,
        generation=1,
        reason="worker lost",
    )
    assert (
        client.post(path + "/recovery/fail", json=failure, headers=headers("allocator")).status_code
        == 409
    )
    failure.update(evaluator_attempt_id=replacement["evaluator_attempt_id"], generation=2)
    commit = {key: value for key, value in failure.items() if key != "reason"}
    commit.update(expected_cas_version=1, result_digest="b" * 64)
    assert (
        client.post(
            path + "/recovery/commit", json=commit, headers=headers("allocator")
        ).status_code
        == 409
    )
    assert (
        client.post(path + "/recovery/fail", json=failure, headers=headers("allocator")).status_code
        == 200
    )
    assert (
        client.post(
            path + "/credentials",
            json={"provider_manifest_digest": manifest.digest},
            headers=headers(response.json()["access_token"]),
        ).status_code
        == 409
    )
    mint.assert_not_called()
