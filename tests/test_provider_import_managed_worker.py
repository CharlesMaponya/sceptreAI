"""The maintained controller's actual managed pod verifies through the actual HTTP path."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from automl_api.models.iam import User
from automl_api.models.workflows import WorkflowAttemptCredentialGrant
from automl_api.services import attempt_object_grants as grants
from automl_api.services.deletion import _require_stopped_workflows, request_deletion
from automl_api.training import provider_import_worker as worker
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_jobs import job_case as job_case  # noqa: F401
from test_provider_import_jobs import snapshot, tick
from test_provider_import_managed_admission import managed_case as managed_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


def test_real_managed_handoff_publishes_ready_and_retains_isolation_until_physical_cleanup(
    managed_case, monkeypatch, tmp_path
):
    c = managed_case
    job = next(iter(c.k8s.objects["job"].values()))
    for item in job["spec"]["template"]["spec"]["containers"][0]["env"]:
        monkeypatch.setenv(item["name"], item["value"])
    token = tmp_path / "projected-token"
    token.write_text(c.token)
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(token))
    monkeypatch.delenv("ATTEMPT_CONTROL_CA_FILE", raising=False)
    app = FastAPI()
    app.include_router(attempt_control.router)
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: c.k8s)
    selected = [c.store]
    monkeypatch.setattr(attempt_control, "get_object_store", lambda: selected[0])
    native_reads = []
    original = httpx.Client
    with TestClient(app) as api:

        def dispatch(request):
            if request.url.host == "control.invalid":
                action = request.url.path.rsplit("/", 1)[1]
                selected[0] = c.capability_store if action == "read" else c.store
                response = api.post(
                    request.url.path,
                    content=request.content,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": request.headers["authorization"],
                    },
                )
                assert response.status_code == 200, response.text
                return httpx.Response(response.status_code, content=response.content)
            assert request.method == "GET" and request.url.host == "objects.invalid"
            assert "authorization" not in request.headers
            params = c.signer.call_args.kwargs["Params"]
            uri, version = (
                c.capability_store.uri_for_key(params["Key"]),
                request.url.params["versionId"],
            )
            assert version == params["VersionId"]
            native_reads.append((uri, version))
            content = c.store.objects[(uri, version)]
            return httpx.Response(
                200,
                headers={"Content-Length": str(len(content)), "x-amz-version-id": version},
                stream=httpx.ByteStream(content),
            )

        monkeypatch.setattr(
            httpx, "Client", lambda **kw: original(**kw, transport=httpx.MockTransport(dispatch))
        )
        result = worker.run_verification()
    assert result["dataset_version_id"] == str(c.version_id)
    assert set(native_reads) == {(o.uri, o.version) for o in c.value.safe_data.objects}
    with c.factory() as db, db.begin():
        with pytest.raises(HTTPException, match="provider verification"):
            request_deletion(db, db.get(User, c.actor_id), c.project_id)
    c.k8s.pods = [c.pod_data]
    tick(c)
    tick(c)
    assert c.k8s.objects["network"] and c.k8s.objects["service_account"]
    c.k8s.pods = []
    tick(c)
    assert c.k8s.objects["network"] and c.k8s.objects["service_account"]
    with c.factory() as db, db.begin():
        issued = list(
            db.scalars(
                select(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == c.attempt_id,
                )
            )
        )
        assert issued and all(row.status == "revoked" for row in issued)
        # Doubled native capabilities: expire their synthetic deadlines without sleeping.
        for row in issued:
            row.created_at = datetime.now(UTC) - timedelta(minutes=2)
            row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
            row.grant_digest = grants._digest(row)
    for _ in range(3):
        tick(c)
    assert snapshot(c).cleanup == "complete"
    assert all(not objects for objects in c.k8s.objects.values())
    with c.factory() as db:
        _require_stopped_workflows(db, c.project_id)
