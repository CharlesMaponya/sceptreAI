"""Pending verification reads real signed copy bytes through the actual HTTP broker."""

import json

import httpx
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from automl_api.models.datasets import DatasetProviderImport, DatasetVersion
from automl_api.models.enums import DatasetStatus
from automl_api.services.provider_copy_verification import verify_provider_objects
from automl_api.training import object_capabilities as caps
from fastapi import FastAPI
from fastapi.testclient import TestClient
from test_provider_import_attempts import bind_identity
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401
from test_provider_import_registration import run

pytest_plugins = ["test_phase1_postgres_contracts"]


def test_pending_worker_reader_verifies_copies_over_broker_without_a_storage_driver(
    pending_case, tmp_path, monkeypatch
):
    c = pending_case
    copied, observed = [], []
    original_client = httpx.Client

    def verify_before_ready():
        if copied:
            return
        bind_identity(c, monkeypatch)
        c.k8s._configured = True
        token_file = tmp_path / "projected-token"
        token_file.write_text(c.token)
        app = FastAPI()
        app.include_router(attempt_control.router)
        app.dependency_overrides[get_session_factory] = lambda: c.factory
        monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: c.k8s)
        monkeypatch.setattr(attempt_control, "get_object_store", lambda: c.capability_store)
        with TestClient(app) as api:

            def dispatch(request):
                if request.url.host == "control.invalid":
                    response = api.post(
                        request.url.path,
                        headers={"Authorization": request.headers["authorization"]},
                        json=json.loads(request.content),
                    )
                    assert response.status_code == 200, response.text
                    assert response.headers["cache-control"] == "no-store"
                    return httpx.Response(
                        response.status_code,
                        content=response.content,
                        headers=dict(response.headers),
                    )
                assert request.method == "GET" and request.url.host == "objects.invalid"
                assert "authorization" not in request.headers
                params = c.signer.call_args.kwargs["Params"]
                uri = c.capability_store.uri_for_key(params["Key"])
                version = request.url.params["versionId"]
                assert version == params["VersionId"]
                observed.append((uri, version))
                content = c.store.objects[(uri, version)]
                return httpx.Response(
                    200,
                    headers={"Content-Length": str(len(content)), "x-amz-version-id": version},
                    stream=httpx.ByteStream(content),
                )

            def client(**kwargs):
                return original_client(**kwargs, transport=httpx.MockTransport(dispatch))

            monkeypatch.setattr(caps.httpx, "Client", client)
            reader = caps.ProviderGrantReader(
                c.value,
                c.attempt_id,
                base_url="https://control.invalid/internal/attempts",
                token_file=token_file,
            )
            proof = verify_provider_objects(c.value, reader, c.limits)
            copied.append(proof)
        with c.factory() as db:
            row = db.get(DatasetProviderImport, c.import_id)
            assert row.status == "pending" and row.local_split_revision_id is None
            assert db.get(DatasetVersion, c.version_id).status == DatasetStatus.UPLOADED
            assert not c.store.writes

    c.store.before_read = verify_before_ready
    assert run(c, c.version_id)
    assert len(copied) == 1
    assert copied[0].byte_size == sum(item.byte_size for item in c.value.safe_data.objects)
    assert set(observed) == {(item.uri, item.version) for item in c.value.safe_data.objects}
    assert c.signer.call_count == len(c.value.safe_data.objects)
