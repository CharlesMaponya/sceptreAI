"""Worker uses actual broker/control HTTP for copy verification and bounded publication."""

import uuid
from datetime import UTC, datetime
from itertools import count
from types import SimpleNamespace

import httpx
import jwt
import pytest
from automl_api.api.routes import attempt_control
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import DatasetStatus
from automl_api.training import provider_import_worker as worker
from automl_api.training.attempt_rpc import AttemptControlUnavailable
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_control import control_case as control_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "changed_bytes",
        "lost_result_ack",
        "rotating_token",
        "unavailable_receipt",
    ],
)
def test_worker_verifies_before_ready_and_never_opens_an_ambient_store(
    control_case, tmp_path, monkeypatch, fault
):
    c = control_case
    token_file = tmp_path / "identity"
    token_file.write_text(c.token)
    for name, value in {
        "AUTOML_ATTEMPT_ID": str(c.attempt_id),
        "AUTOML_PROJECT_ID": str(c.project_id),
        "AUTOML_FENCING_TOKEN": c.fence,
        "AUTOML_IDENTITY_TOKEN_FILE": str(token_file),
        "ATTEMPT_CONTROL_BASE_URL": "https://control.invalid/internal/attempts",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("ATTEMPT_CONTROL_CA_FILE", raising=False)
    if fault is None:
        ticks = count(0, 16)
        monkeypatch.setattr(worker, "time", SimpleNamespace(monotonic=lambda: next(ticks)))
    selected = [c.store]
    monkeypatch.setattr(attempt_control, "get_object_store", lambda: selected[0])
    import automl_api.storage.object_store as ambient

    monkeypatch.setattr(ambient, "get_object_store", lambda: pytest.fail("Worker ambient store"))
    original_client = httpx.Client
    actions, reads, errors = [], [], []

    def dispatch(request):
        if request.url.host == "control.invalid":
            action = request.url.path.rsplit("/", 1)[1]
            actions.append(action)
            selected[0] = c.capability_store if action == "read" else c.store
            if action == "result" and actions.count("result") == 1:
                with c.factory() as db:
                    assert db.get(DatasetVersion, c.version_id).status == DatasetStatus.UPLOADED
                assert not c.store.writes
                assert len(reads) == len(c.value.safe_data.objects)
            response = c.api.post(
                request.url.path,
                headers={
                    "Authorization": request.headers["authorization"],
                    "Content-Type": "application/json",
                },
                content=request.content,
            )
            if response.status_code != 200 and action != "failure":
                errors.append((action, response.status_code, response.text))
            if (
                action in {"result", "receipt"}
                and fault in {"lost_result_ack", "rotating_token", "unavailable_receipt"}
                and (action == "result" or fault == "unavailable_receipt")
            ):
                assert response.status_code == 200
                if fault == "rotating_token":
                    c.token = jwt.encode(
                        {"exp": int(datetime.now(UTC).timestamp()) + 599, "jti": str(uuid.uuid4())},
                        "unit-import-token-at-least-32-bytes",
                        algorithm="HS256",
                    )
                    token_file.write_text(c.token)
                raise httpx.ReadError("private-control-detail")
            return httpx.Response(response.status_code, content=response.content)
        assert request.method == "GET" and request.url.host == "objects.invalid"
        assert "authorization" not in request.headers
        params = c.signer.call_args.kwargs["Params"]
        uri = c.capability_store.uri_for_key(params["Key"])
        version = request.url.params["versionId"]
        assert version == params["VersionId"]
        reads.append((uri, version))
        content = c.store.objects[(uri, version)]
        if fault == "changed_bytes":
            content = b"x" * len(content)
        return httpx.Response(
            200,
            headers={"Content-Length": str(len(content)), "x-amz-version-id": version},
            stream=httpx.ByteStream(content),
        )

    def client(**kwargs):
        return original_client(**kwargs, transport=httpx.MockTransport(dispatch))

    monkeypatch.setattr(httpx, "Client", client)
    try:
        if fault in {"changed_bytes", "unavailable_receipt"}:
            with pytest.raises(
                ValueError if fault == "changed_bytes" else AttemptControlUnavailable
            ):
                worker.run_verification()
        else:
            result = worker.run_verification()
            assert result["dataset_version_id"] == str(c.version_id)
    finally:
        assert not errors, errors
    with c.factory() as db:
        status = db.get(DatasetVersion, c.version_id).status
        assert status == (DatasetStatus.FAILED if fault == "changed_bytes" else DatasetStatus.READY)
    assert actions[0] == "start"
    assert ("failure" in actions) == (fault in {"changed_bytes", "unavailable_receipt"})
    assert actions.count("result") == (0 if fault == "changed_bytes" else 1)
    assert actions.count("receipt") == (0 if fault in {None, "changed_bytes"} else 1)
    assert c.store.writes == []
    if fault is None:
        assert "heartbeat" in actions
        assert set(reads) == {(o.uri, o.version) for o in c.value.safe_data.objects}


def test_worker_main_sanitizes_failures(monkeypatch, capsys):
    def fail():
        raise RuntimeError("provider secret and signed URL")

    monkeypatch.setattr(worker, "run_verification", fail)
    with pytest.raises(SystemExit) as error:
        worker.main()
    assert error.value.code == 1
    assert capsys.readouterr().err == "Provider verification failed\n"


@pytest.mark.parametrize("protocol", [None, "provider-import-verifier-v0"])
def test_worker_rejects_an_old_control_protocol_before_reading_any_copy(
    control_case, monkeypatch, protocol
):
    from unittest.mock import Mock

    c = control_case
    for name, value in {
        "AUTOML_ATTEMPT_ID": str(c.attempt_id),
        "AUTOML_PROJECT_ID": str(c.project_id),
        "AUTOML_FENCING_TOKEN": c.fence,
        "ATTEMPT_CONTROL_BASE_URL": "https://control.invalid/internal/attempts",
    }.items():
        monkeypatch.setenv(name, value)
    payload = {
        "attempt_id": str(c.attempt_id),
        "project_id": str(c.project_id),
        "dataset_version_id": str(c.version_id),
        "fencing_token": c.fence,
        "manifest": c.value.model_dump(mode="json"),
        "budget": c.limits.model_dump(mode="json"),
    }
    if protocol is not None:
        payload["protocol"] = protocol
    control = Mock()
    control.request.return_value = payload
    monkeypatch.setattr(worker, "AttemptRPC", lambda *_a, **_kw: control)
    reader = Mock(side_effect=AssertionError("An old control cannot authorize copy reads"))
    monkeypatch.setattr(worker, "ProviderGrantReader", reader)
    with pytest.raises(ValueError):
        worker.run_verification()
    reader.assert_not_called()
    assert control.request.call_count == 1
