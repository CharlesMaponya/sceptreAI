from __future__ import annotations

from automl_api.main import app


def test_phase_two_routes_are_registered() -> None:
    routes = {route.path for route in app.routes}

    expected_routes = {
        "/api/v1/auth/register",
        "/api/v1/auth/login",
        "/api/v1/auth/refresh",
        "/api/v1/auth/me",
        "/api/v1/projects",
        "/api/v1/projects/{project_id}",
        "/api/v1/projects/{project_id}/members",
        "/api/v1/projects/{project_id}/share-links",
        "/api/v1/projects/share-links/accept",
        "/api/v1/projects/{project_id}/datasets",
        "/api/v1/projects/{project_id}/datasets/upload",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/latest",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/{job_id}",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/{job_id}/result",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/{job_id}/features/{column}",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/{job_id}/feature",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/{job_id}/relationships",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/{job_id}/preparation",
        "/api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile-jobs/{job_id}/events",
        "/api/v1/projects/{project_id}/training/estimate",
        "/api/v1/projects/{project_id}/training/estimators",
        "/api/v1/projects/{project_id}/training/runs",
        "/api/v1/projects/{project_id}/training/runs/{run_id}",
        "/api/v1/projects/{project_id}/training/runs/{run_id}/models",
        "/api/v1/projects/{project_id}/training/runs/{run_id}/restart",
        "/api/v1/projects/{project_id}/training/runs/{run_id}/leaderboard",
        "/api/v1/projects/{project_id}/training/runs/{run_id}/logs",
        "/api/v1/projects/{project_id}/training/runs/{run_id}/logs/ws",
        "/api/v1/projects/{project_id}/training/runs/{training_run_id}/validations",
        "/api/v1/projects/{project_id}/training/runs/{training_run_id}/explanations",
        "/api/v1/projects/{project_id}/training/runs/{training_run_id}/analyses",
        "/api/v1/projects/{project_id}/training/runs/{training_run_id}/analyses/{analysis_run_id}",
    }

    assert expected_routes.issubset(routes)
