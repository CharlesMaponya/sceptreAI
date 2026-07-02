from __future__ import annotations

import base64
import json
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import delete

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api"))

from automl_api.db.session import get_session_factory  # noqa: E402
from automl_api.models.iam import User  # noqa: E402
from automl_api.models.projects import Project  # noqa: E402
from automl_api.storage.object_store import get_object_store  # noqa: E402

API_BASE = "http://127.0.0.1:8000/api/v1"


def request(
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    token: str | None = None,
) -> tuple[int, dict[str, Any]]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        f"{API_BASE}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        return response.status, json.loads(response.read())


def wait_for_job(
    job_base_path: str,
    job_id: str,
    token: str,
    timeout_seconds: int = 90,
) -> tuple[dict[str, Any], set[str]]:
    job_path = f"{job_base_path}/{job_id}"
    _, job = request("GET", job_path, token=token)
    seen_stages = {job["current_stage"]}
    deadline = time.monotonic() + timeout_seconds
    while job["status"] not in {"succeeded", "failed", "cancelled"}:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Profiling job {job_id} did not finish within {timeout_seconds} seconds."
            )
        time.sleep(0.5)
        _, job = request("GET", job_path, token=token)
        seen_stages.add(job["current_stage"])
    return job, seen_stages


def start_target_job(
    job_base_path: str,
    target_column: str | None,
    token: str,
) -> tuple[int, dict[str, Any]]:
    return request(
        "POST",
        job_base_path,
        payload={"target_column": target_column, "force": False},
        token=token,
    )


def main() -> None:
    marker = uuid.uuid4().hex[:10]
    email = f"profile-smoke-{marker}@example.com"
    project_id = None
    user_id = None
    object_prefix = None
    try:
        _, registration = request(
            "POST",
            "/auth/register",
            payload={
                "full_name": "Profiling Smoke Test",
                "email": email,
                "password": "Smoke-test-password-123!",
            },
        )
        token = registration["tokens"]["access_token"]
        user_id = registration["user"]["id"]

        _, project = request(
            "POST",
            "/projects",
            payload={
                "name": f"Profiling smoke {marker}",
                "description": "Disposable staged profiling validation",
                "settings": {},
            },
            token=token,
        )
        project_id = project["id"]
        csv_content = "Unnamed: 0,amount,segment,target\n" + "".join(
            f"{index - 1},{index},{'small' if index % 2 else 'large'},{index * 3}\n"
            for index in range(1, 301)
        )

        started = time.monotonic()
        upload_status, upload = request(
            "POST",
            f"/projects/{project_id}/datasets/upload",
            payload={
                "dataset_name": "smoke-data",
                "description": "staged profile validation",
                "filename": "smoke.csv",
                "content_base64": base64.b64encode(csv_content.encode()).decode(),
                "tags": {},
            },
            token=token,
        )
        upload_seconds = time.monotonic() - started
        assert upload_status == 201
        assert upload_seconds < 15
        job_id = upload["profiling_job_id"]
        dataset_id = upload["dataset"]["id"]
        version_id = upload["version"]["id"]
        object_prefix = f"projects/{project_id}/"
        job_base_path = (
            f"/projects/{project_id}/datasets/{dataset_id}/versions/{version_id}"
            "/profile-jobs"
        )

        first_status, first_job = request(
            "GET",
            f"{job_base_path}/{job_id}",
            token=token,
        )
        assert first_status == 200
        assert first_job["overview_json"]["stages"]["overview"] == "completed"

        clustering_job, seen_stages = wait_for_job(job_base_path, job_id, token)
        assert clustering_job["status"] == "succeeded"
        assert clustering_job["overview_json"]["task_inference"]["task_type"] == "clustering"
        assert clustering_job["completed_columns"] == 4
        assert {"features", "relationships", "preparation", "complete"}.issubset(
            clustering_job["artifact_uris_json"]
        )
        assert clustering_job["row_count"] == 300
        shared_features_uri = clustering_job["artifact_uris_json"]["features"]
        clustering_relationships_uri = clustering_job["artifact_uris_json"][
            "relationships"
        ]
        clustering_preparation_uri = clustering_job["artifact_uris_json"]["preparation"]

        classification_status, classification_started = start_target_job(
            job_base_path,
            "segment",
            token,
        )
        assert classification_status == 202
        assert classification_started["overview_json"]["stages"]["features"] == "reused"
        assert classification_started["artifact_uris_json"]["features"] == shared_features_uri
        classification_job, _ = wait_for_job(
            job_base_path,
            classification_started["id"],
            token,
        )
        assert classification_job["status"] == "succeeded"
        assert (
            classification_job["overview_json"]["task_inference"]["task_type"]
            == "classification"
        )
        assert classification_job["artifact_uris_json"]["features"] == shared_features_uri
        assert (
            classification_job["artifact_uris_json"]["relationships"]
            != clustering_relationships_uri
        )
        assert (
            classification_job["artifact_uris_json"]["preparation"]
            != clustering_preparation_uri
        )

        regression_status, regression_started = start_target_job(
            job_base_path,
            "target",
            token,
        )
        assert regression_status == 202
        regression_job, _ = wait_for_job(
            job_base_path,
            regression_started["id"],
            token,
        )
        assert regression_job["status"] == "succeeded"
        assert regression_job["overview_json"]["task_inference"]["task_type"] == "regression"
        assert regression_job["artifact_uris_json"]["features"] == shared_features_uri
        assert (
            regression_job["overview_json"]["features_reused_from_job_id"]
            is not None
        )

        obsolete_status, obsolete_started = start_target_job(
            job_base_path,
            "amount",
            token,
        )
        replacement_status, replacement_started = start_target_job(
            job_base_path,
            "segment",
            token,
        )
        assert obsolete_status == 202
        assert replacement_status == 202
        _, obsolete_job = request(
            "GET",
            f"{job_base_path}/{obsolete_started['id']}",
            token=token,
        )
        assert obsolete_job["status"] == "cancelled"
        replacement_job, _ = wait_for_job(
            job_base_path,
            replacement_started["id"],
            token,
        )
        assert replacement_job["status"] == "succeeded"

        no_target_status, no_target_started = start_target_job(
            job_base_path,
            None,
            token,
        )
        assert no_target_status == 202
        no_target_job, _ = wait_for_job(
            job_base_path,
            no_target_started["id"],
            token,
        )
        assert no_target_job["overview_json"]["task_inference"]["task_type"] == "clustering"
        cached_status, cached_job = start_target_job(job_base_path, None, token)
        assert cached_status == 200
        assert cached_job["id"] == no_target_job["id"]
        _, latest_job = request("GET", f"{job_base_path}/latest", token=token)
        assert latest_job["id"] == no_target_job["id"]

        job_path = f"{job_base_path}/{no_target_job['id']}"
        result_status, result = request(
            "GET",
            f"{job_path}/result",
            token=token,
        )
        assert result_status == 200
        assert len(result["feature_profiles_json"]) == 4
        feature_status, feature = request(
            "GET",
            f"{job_path}/features/amount",
            token=token,
        )
        assert feature_status == 200
        assert feature["status"] == "completed"
        assert feature["profile"]["statistics"]["count"] == 300
        unsafe_feature_query = urllib.parse.urlencode({"column": "Unnamed: 0"})
        unsafe_feature_status, unsafe_feature = request(
            "GET",
            f"{job_path}/feature?{unsafe_feature_query}",
            token=token,
        )
        assert unsafe_feature_status == 200
        assert unsafe_feature["column"] == "Unnamed: 0"
        assert unsafe_feature["profile"]["statistics"]["count"] == 300

        relationship_status, relationship_result = request(
            "GET",
            f"{job_path}/relationships",
            token=token,
        )
        preparation_status, preparation_result = request(
            "GET",
            f"{job_path}/preparation",
            token=token,
        )
        assert relationship_status == 200
        assert relationship_result["status"] == "completed"
        assert preparation_status == 200
        assert preparation_result["status"] == "completed"

        event_request = urllib.request.Request(
            f"{API_BASE}{job_path}/events",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(event_request, timeout=10) as event_response:
            event_payload = event_response.read().decode()
        assert "event: progress" in event_payload
        assert '"status": "succeeded"' in event_payload
        print(
            json.dumps(
                {
                    "upload_seconds": round(upload_seconds, 3),
                    "job_status": no_target_job["status"],
                    "rows": no_target_job["row_count"],
                    "features": no_target_job["completed_columns"],
                    "stages_seen": sorted(seen_stages),
                    "artifacts": sorted(no_target_job["artifact_uris_json"]),
                    "target_transitions": [
                        "clustering",
                        "classification",
                        "regression",
                        "classification",
                        "clustering",
                    ],
                },
                indent=2,
            )
        )
    finally:
        if object_prefix:
            store = get_object_store()
            client = getattr(store, "client", None)
            bucket = getattr(store, "bucket", None)
            if client and bucket:
                for item in client.list_objects(bucket, prefix=object_prefix, recursive=True):
                    client.remove_object(bucket, item.object_name)
        if project_id or user_id:
            with get_session_factory()() as db:
                if project_id:
                    db.execute(delete(Project).where(Project.id == uuid.UUID(project_id)))
                if user_id:
                    db.execute(delete(User).where(User.id == uuid.UUID(user_id)))
                db.commit()


if __name__ == "__main__":
    main()
