from __future__ import annotations

import importlib
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
    project_page = (STREAMLIT_ROOT / "pages" / "1_Project.py").read_text(encoding="utf-8")

    assert '"access_token" not in st.session_state' in project_page
    assert 'st.session_state.get("selected_project_id")' in project_page
    assert 'st.switch_page("app.py")' in project_page


def test_profile_display_value_handles_integers_larger_than_int64() -> None:
    oversized = 10**30

    assert workspace.profile_display_value(oversized) == str(oversized)


def test_chart_fallback_renders_dataframe_when_streamlit_charting_fails(monkeypatch) -> None:
    calls = []

    def broken_bar_chart(*args, **kwargs):
        calls.append((args, kwargs))
        raise TypeError("altair error")

    def fake_dataframe(data, **kwargs):
        calls.append(("dataframe", data, kwargs))
        return None

    monkeypatch.setattr(workspace.st, "bar_chart", broken_bar_chart)
    monkeypatch.setattr(workspace.st, "dataframe", fake_dataframe)
    workspace._install_streamlit_chart_fallbacks()

    workspace.st.bar_chart([{"label": "a", "count": 1}])

    assert calls[0][0] == ([{"label": "a", "count": 1}],)
    assert calls[1][0] == "dataframe"


def test_historical_shap_importance_is_normalized_for_display() -> None:
    normalized = workspace.normalize_shap_importance_rows(
        [
            {"feature": "income", "mean_absolute_shap": 3},
            {"feature": "age", "mean_absolute_shap": 1},
        ]
    )

    assert [item["feature"] for item in normalized] == ["income", "age"]
    assert normalized[0]["contribution_percent"] == 75.0
    assert normalized[1]["contribution_percent"] == 25.0
    assert sum(item["contribution_percent"] for item in normalized) == 100.0


def test_shap_display_normalization_handles_invalid_values() -> None:
    normalized = workspace.normalize_shap_importance_rows(
        [
            {"feature": "missing", "mean_absolute_shap": None},
            {"feature": "infinite", "mean_absolute_shap": float("inf")},
        ]
    )

    assert all(item["contribution_percent"] == 0.0 for item in normalized)


def test_shap_chart_orders_largest_contribution_first() -> None:
    chart = workspace.shap_importance_chart(
        [
            {"feature": "income", "contribution_percent": 75.0},
            {"feature": "age", "contribution_percent": 25.0},
        ]
    ).to_dict()

    assert chart["encoding"]["y"]["sort"] == {
        "field": "contribution_percent",
        "order": "descending",
    }


def test_shap_chart_falls_back_when_altair_is_unavailable(monkeypatch) -> None:
    monkeypatch.setitem(__import__("sys").modules, "altair", None)
    reloaded_workspace = importlib.reload(workspace)
    chart = reloaded_workspace.shap_importance_chart(
        [{"feature": "income", "contribution_percent": 75.0}]
    )

    assert chart.to_dict()["encoding"]["y"]["sort"] == {
        "field": "contribution_percent",
        "order": "descending",
    }


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
