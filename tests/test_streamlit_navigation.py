from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest
from streamlit_app import workspace

STREAMLIT_ROOT = Path("apps/ui/streamlit_app")


def test_project_navigation_uses_native_streamlit_page() -> None:
    entrypoint = (STREAMLIT_ROOT / "app.py").read_text(encoding="utf-8")
    project_page = STREAMLIT_ROOT / "pages" / "1_Project.py"

    assert project_page.exists()
    assert 'st.switch_page("pages/1_Project.py")' in entrypoint
    assert "st.link_button" not in entrypoint
    assert "?project_id=" not in entrypoint


def test_project_page_guards_auth_with_shared_session_state() -> None:
    project_page = (STREAMLIT_ROOT / "pages" / "1_Project.py").read_text(
        encoding="utf-8"
    )

    assert '"access_token" not in st.session_state' in project_page
    assert 'st.session_state.get("selected_project_id")' in project_page
    assert 'st.switch_page("app.py")' in project_page


def test_profile_display_value_handles_integers_larger_than_int64() -> None:
    oversized = 10**30

    assert workspace.profile_display_value(oversized) == str(oversized)


def test_profile_renders_statistics_larger_than_int64(tmp_path: Path) -> None:
    app_path = tmp_path / "profile_app.py"
    app_path.write_text(
        """
from streamlit_app.workspace import render_profile

render_profile({
    "row_count_analyzed": 3,
    "column_count": 1,
    "target_column": None,
    "task_inference": {
        "task_type": "clustering",
        "confidence": 0.9,
        "rationale": "Test profile.",
    },
    "columns": [{
        "name": "event_epoch",
        "semantic_type": "numerical_continuous",
        "missing_count": 0,
        "missing_ratio": 0.0,
        "distinct_count": 3,
        "statistics": {
            "count": 3,
            "variance": 10**30,
            "max": 1_704_240_000_000_000_000,
        },
        "distribution_type": "histogram",
        "distribution": [],
        "quality_flags": [],
    }],
    "relationships": [],
    "preparation_plan": [],
    "warnings": [],
})
""",
        encoding="utf-8",
    )

    app = AppTest.from_file(str(app_path)).run(timeout=10)

    assert len(app.exception) == 0


def test_api_request_refreshes_an_expired_access_token(
    monkeypatch,
) -> None:
    session_state = {
        "access_token": "expired-access",
        "refresh_token": "valid-refresh",
    }
    calls = []

    def fake_request(method, path, *, payload=None, token=None, timeout=None):
        calls.append((method, path, payload, token, timeout))
        if path == "/auth/refresh":
            return 200, {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 86_400,
            }
        if token == "expired-access":
            return 401, {"detail": "Invalid or expired access token."}
        return 200, {"ok": True}

    monkeypatch.setattr(workspace.st, "session_state", session_state)
    monkeypatch.setattr(workspace, "_api_request_once", fake_request)

    status_code, response = workspace.api_request(
        "GET",
        "/projects",
        token="expired-access",
    )

    assert status_code == 200
    assert response == {"ok": True}
    assert session_state["access_token"] == "new-access"
    assert session_state["refresh_token"] == "new-refresh"
    assert [call[1] for call in calls] == [
        "/projects",
        "/auth/refresh",
        "/projects",
    ]
