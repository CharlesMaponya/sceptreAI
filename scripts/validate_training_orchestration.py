from __future__ import annotations

import base64
import json
import sys
import time
import urllib.error
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
from automl_api.services.kubernetes_training import (  # noqa: E402
    KubernetesTrainingClient,
)
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
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def wait_for_status(
    path: str,
    token: str,
    *,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status, result = request("GET", path, token=token)
        assert status == 200, result
        if result["status"] in {"succeeded", "failed", "cancelled"}:
            return result
        time.sleep(2)
    raise TimeoutError(f"{path} did not complete within {timeout_seconds} seconds.")


def main() -> None:
    marker = uuid.uuid4().hex[:10]
    project_id = None
    user_id = None
    object_prefix = None
    k8s_job_name = None
    try:
        _, registration = request(
            "POST",
            "/auth/register",
            payload={
                "full_name": "Training Smoke Test",
                "email": f"training-smoke-{marker}@example.com",
                "password": "Smoke-test-password-123!",
            },
        )
        token = registration["tokens"]["access_token"]
        user_id = registration["user"]["id"]
        _, project = request(
            "POST",
            "/projects",
            payload={
                "name": f"Training smoke {marker}",
                "description": "Disposable Kubernetes training validation",
                "settings": {},
            },
            token=token,
        )
        project_id = project["id"]
        object_prefix = f"projects/{project_id}/"
        csv_content = "amount,ratio,segment,target\n" + "".join(
            (
                f"{index},{(index % 17) / 17:.4f},"
                f"{'small' if index % 3 else 'large'},"
                f"{'approve' if index % 4 in {0, 1} else 'decline'}\n"
            )
            for index in range(1, 241)
        )
        upload_status, upload = request(
            "POST",
            f"/projects/{project_id}/datasets/upload",
            payload={
                "dataset_name": "training-smoke-data",
                "description": "training orchestration validation",
                "filename": "training-smoke.csv",
                "content_base64": base64.b64encode(csv_content.encode()).decode(),
                "tags": {},
            },
            token=token,
        )
        assert upload_status == 201, upload
        dataset_id = upload["dataset"]["id"]
        version_id = upload["version"]["id"]
        profile_base = (
            f"/projects/{project_id}/datasets/{dataset_id}/versions/{version_id}"
            "/profile-jobs"
        )
        wait_for_status(
            f"{profile_base}/{upload['profiling_job_id']}",
            token,
            timeout_seconds=120,
        )
        profile_status, profile = request(
            "POST",
            profile_base,
            payload={"target_column": "target", "force": False},
            token=token,
        )
        assert profile_status in {200, 202}, profile
        profile = wait_for_status(
            f"{profile_base}/{profile['id']}",
            token,
            timeout_seconds=120,
        )
        assert profile["overview_json"]["task_inference"]["task_type"] == "classification"

        training_payload = {
            "dataset_version_id": version_id,
            "target_column": "target",
            "task_type": "classification",
            "prefer_gpu": False,
            "expected_minutes": 5,
            "candidate_limit": 3,
            "optimization_iterations": 1,
        }
        estimate_status, estimate = request(
            "POST",
            f"/projects/{project_id}/training/estimate",
            payload=training_payload,
            token=token,
        )
        assert estimate_status == 200 and estimate["can_launch"], estimate
        started = time.monotonic()
        launch_status, launch = request(
            "POST",
            f"/projects/{project_id}/training/runs",
            payload={
                **training_payload,
                "run_name": f"smoke-{marker}",
                "params": {},
            },
            token=token,
        )
        launch_seconds = time.monotonic() - started
        assert launch_status == 202, launch
        assert launch_seconds < 30
        run = launch["run"]
        run_id = run["id"]
        k8s_job_name = run["k8s_job_name"]

        fairness_status, fairness = request(
            "POST",
            f"/projects/{project_id}/training/estimate",
            payload=training_payload,
            token=token,
        )
        assert fairness_status == 200
        assert not fairness["can_launch"]
        assert any("already has an active" in blocker for blocker in fairness["blockers"])

        completed = wait_for_status(
            f"/projects/{project_id}/training/runs/{run_id}",
            token,
            timeout_seconds=420,
        )
        logs_status, logs = request(
            "GET",
            f"/projects/{project_id}/training/runs/{run_id}/logs",
            token=token,
        )
        leaderboard_status, leaderboard = request(
            "GET",
            f"/projects/{project_id}/training/runs/{run_id}/leaderboard",
            token=token,
        )
        assert completed["status"] == "succeeded", {"run": completed, "logs": logs}
        assert logs_status == 200
        assert leaderboard_status == 200
        assert leaderboard["winner"]
        assert len(leaderboard["entries"]) == 3
        assert leaderboard["entries"][0]["rank"] == 1
        assert completed["mlflow_run_id"]
        print(
            json.dumps(
                {
                    "launch_seconds": round(launch_seconds, 3),
                    "status": completed["status"],
                    "job": k8s_job_name,
                    "winner": leaderboard["winner"],
                    "primary_metric": leaderboard["primary_metric"],
                    "candidates": [
                        {
                            "rank": entry["rank"],
                            "model": entry["model"],
                            "status": entry["status"],
                            "score": entry["primary_score"],
                        }
                        for entry in leaderboard["entries"]
                    ],
                    "log_lines": len(logs["lines"]),
                    "fairness_guard": True,
                },
                indent=2,
            )
        )
    finally:
        if k8s_job_name:
            try:
                KubernetesTrainingClient().delete_job(k8s_job_name)
            except Exception:
                pass
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
