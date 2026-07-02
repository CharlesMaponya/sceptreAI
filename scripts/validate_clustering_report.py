from __future__ import annotations

import base64
import json
import sys
import time
import urllib.error
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
from automl_api.services.kubernetes_training import (  # noqa: E402
    KubernetesTrainingClient,
)
from automl_api.storage.object_store import get_object_store  # noqa: E402

API_BASE = "http://127.0.0.1:8000/api/v1"
EXPECTED_METRICS = {
    "silhouette",
    "davies_bouldin",
    "calinski_harabasz",
    "adjusted_rand",
    "normalized_mutual_info",
    "adjusted_mutual_info",
    "fowlkes_mallows",
    "homogeneity",
}


def request(
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    token: str | None = None,
) -> tuple[int, Any]:
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
                "full_name": "Clustering Report Smoke",
                "email": f"cluster-report-{marker}@example.com",
                "password": "Smoke-test-password-123!",
            },
        )
        token = registration["tokens"]["access_token"]
        user_id = registration["user"]["id"]
        _, project = request(
            "POST",
            "/projects",
            payload={
                "name": f"Cluster report {marker}",
                "description": "Disposable clustering metric validation",
                "settings": {},
            },
            token=token,
        )
        project_id = project["id"]
        object_prefix = f"projects/{project_id}/"
        rows = ["x1,x2,segment"]
        for index in range(240):
            group = index % 3
            rows.append(
                f"{group * 5 + (index % 7) / 20:.3f},"
                f"{group * 4 + (index % 11) / 25:.3f},"
                f"group-{group}"
            )
        upload_status, upload = request(
            "POST",
            f"/projects/{project_id}/datasets/upload",
            payload={
                "dataset_name": "cluster-report-data",
                "description": "clustering evaluation validation",
                "filename": "clusters.csv",
                "content_base64": base64.b64encode(
                    "\n".join(rows).encode()
                ).decode(),
                "tags": {},
            },
            token=token,
        )
        assert upload_status == 201, upload
        dataset_id = upload["dataset"]["id"]
        version_id = upload["version"]["id"]
        profile_path = (
            f"/projects/{project_id}/datasets/{dataset_id}/versions/{version_id}"
            f"/profile-jobs/{upload['profiling_job_id']}"
        )
        profile = wait_for_status(profile_path, token, 120)
        assert profile["overview_json"]["task_inference"]["task_type"] == "clustering"

        query = urllib.parse.urlencode({"task_type": "clustering"})
        catalog_status, catalog = request(
            "GET",
            f"/projects/{project_id}/training/estimators?{query}",
            token=token,
        )
        assert catalog_status == 200
        catalog_names = {item["name"] for item in catalog}
        selected_models = ["KMeans", "Birch", "AgglomerativeClustering"]
        assert set(selected_models).issubset(catalog_names)

        payload = {
            "dataset_version_id": version_id,
            "target_column": None,
            "evaluation_column": "segment",
            "task_type": "clustering",
            "prefer_gpu": False,
            "expected_minutes": 5,
            "candidate_limit": len(selected_models),
            "candidate_models": selected_models,
            "optimization_iterations": 1,
            "cv_folds": 3,
        }
        estimate_status, estimate = request(
            "POST",
            f"/projects/{project_id}/training/estimate",
            payload=payload,
            token=token,
        )
        assert estimate_status == 200 and estimate["can_launch"], estimate
        assert estimate["capacity"]["source"] == "metrics_api_conservative_headroom"
        assert not estimate["capacity"]["warnings"]
        launch_status, launch = request(
            "POST",
            f"/projects/{project_id}/training/runs",
            payload={
                **payload,
                "run_name": f"cluster-report-{marker}",
                "params": {},
            },
            token=token,
        )
        assert launch_status == 202, launch
        run = launch["run"]
        k8s_job_name = run["k8s_job_name"]
        completed = wait_for_status(
            f"/projects/{project_id}/training/runs/{run['id']}",
            token,
            420,
        )
        leaderboard_status, leaderboard = request(
            "GET",
            f"/projects/{project_id}/training/runs/{run['id']}/leaderboard",
            token=token,
        )
        assert completed["status"] == "succeeded", completed
        assert leaderboard_status == 200
        successful = [
            entry
            for entry in leaderboard["entries"]
            if entry["status"] == "succeeded"
        ]
        assert successful
        assert EXPECTED_METRICS.issubset(successful[0]["metrics"])
        assert successful[0]["diagnostics"]["external_evaluation"]
        assert successful[0]["diagnostics"]["cross_validation"]["folds"] == 3
        print(
            json.dumps(
                {
                    "catalog_size": len(catalog),
                    "capacity_source": estimate["capacity"]["source"],
                    "memory_request_mb": estimate["memory_request_mb"],
                    "winner": leaderboard["winner"],
                    "metrics": successful[0]["metrics"],
                    "metric_directions": leaderboard["metric_directions"],
                    "cluster_sizes": successful[0]["diagnostics"]["cluster_sizes"],
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
