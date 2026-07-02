from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import pandas as pd
import streamlit as st

API_BASE_URL = os.getenv("AUTOML_API_URL", "http://127.0.0.1:8000/api/v1")
DEFAULT_API_TIMEOUT_SECONDS = int(os.getenv("AUTOML_API_TIMEOUT_SECONDS", "30"))
UPLOAD_TIMEOUT_SECONDS = int(os.getenv("AUTOML_UPLOAD_TIMEOUT_SECONDS", "300"))


def profile_display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.8g}"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, default=str)
    return str(value)


def stable_selectbox(
    label: str,
    options: list[str],
    *,
    key: str,
    format_func: Any,
) -> str:
    if st.session_state.get(key) not in options:
        st.session_state[key] = options[0]
    return st.selectbox(
        label,
        options,
        key=key,
        format_func=format_func,
    )


def api_request(
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    token: str | None = None,
    timeout: int = DEFAULT_API_TIMEOUT_SECONDS,
) -> tuple[int, dict[str, Any] | list[Any] | str]:
    status_code, response = _api_request_once(
        method,
        path,
        payload=payload,
        token=token,
        timeout=timeout,
    )
    if (
        status_code != 401
        or not token
        or path == "/auth/refresh"
        or token != st.session_state.get("access_token")
    ):
        return status_code, response

    refresh_token = st.session_state.get("refresh_token")
    if not refresh_token:
        return status_code, response
    refresh_status, refreshed = _api_request_once(
        "POST",
        "/auth/refresh",
        payload={"refresh_token": refresh_token},
        timeout=DEFAULT_API_TIMEOUT_SECONDS,
    )
    if refresh_status != 200 or not isinstance(refreshed, dict):
        return status_code, response

    st.session_state["access_token"] = refreshed["access_token"]
    st.session_state["refresh_token"] = refreshed["refresh_token"]
    return _api_request_once(
        method,
        path,
        payload=payload,
        token=refreshed["access_token"],
        timeout=timeout,
    )


def _api_request_once(
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    token: str | None = None,
    timeout: int = DEFAULT_API_TIMEOUT_SECONDS,
) -> tuple[int, dict[str, Any] | list[Any] | str]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(
        f"{API_BASE_URL}{path}",
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            detail = json.loads(raw)
        except json.JSONDecodeError:
            detail = raw
        return exc.code, detail
    except urllib.error.URLError as exc:
        return 0, f"Could not reach API at {API_BASE_URL}: {exc.reason}"
    except TimeoutError:
        return 0, f"Request timed out after {timeout} seconds."


def set_auth(response: dict[str, Any]) -> None:
    st.session_state["access_token"] = response["tokens"]["access_token"]
    st.session_state["refresh_token"] = response["tokens"]["refresh_token"]
    st.session_state["user"] = response["user"]


def render_session_sidebar() -> None:
    with st.sidebar:
        st.subheader("Session")
        user = st.session_state.get("user")
        if not user:
            st.write("Not signed in")
            return
        st.write(user["email"])
        if st.button("Log out", use_container_width=True):
            token = st.session_state.get("access_token")
            refresh_token = st.session_state.get("refresh_token")
            api_request(
                "POST",
                "/auth/logout",
                payload={"refresh_token": refresh_token},
                token=token,
            )
            st.session_state.clear()
            st.switch_page("app.py")


def render_profile(profile: dict[str, Any]) -> None:
    inference = profile["task_inference"]
    summary_columns = st.columns(4)
    summary_columns[0].metric("Rows analyzed", f"{profile['row_count_analyzed']:,}")
    summary_columns[1].metric("Features", profile["column_count"])
    summary_columns[2].metric("Task", inference["task_type"].replace("_", " ").title())
    summary_columns[3].metric("Confidence", f"{inference['confidence']:.0%}")
    st.caption(inference["rationale"])

    if profile["warnings"]:
        st.warning("\n".join(profile["warnings"]))

    preparation_by_column: dict[str, list[dict[str, Any]]] = {}
    for step in profile["preparation_plan"]:
        preparation_by_column.setdefault(step["column"], []).append(step)
    relationships_by_column = {
        relationship["source_column"]: relationship for relationship in profile["relationships"]
    }

    st.subheader("Feature profiles")
    for column in profile["columns"]:
        flags = ", ".join(flag.replace("_", " ") for flag in column["quality_flags"])
        expander_label = f"{column['name']} - {column['semantic_type'].replace('_', ' ')}"
        if flags:
            expander_label += f" - {flags}"
        with st.expander(expander_label):
            metrics = st.columns(3)
            metrics[0].metric("Distinct", f"{column['distinct_count']:,}")
            metrics[1].metric("Missing", f"{column['missing_count']:,}")
            metrics[2].metric("Missing rate", f"{column['missing_ratio']:.1%}")

            chart_column, details_column = st.columns([3, 2])
            with chart_column:
                st.markdown("**Distribution**")
                if column["distribution"]:
                    x_label = "Value range"
                    if column["semantic_type"] == "text":
                        x_label = "Text length"
                    elif column["distribution_type"] == "bar":
                        x_label = column["name"]
                    st.bar_chart(
                        column["distribution"],
                        x="label",
                        y="count",
                        x_label=x_label,
                        y_label="Count",
                        color="#2563eb",
                    )
                else:
                    st.info("A distribution is not available for this feature.")

            with details_column:
                st.markdown("**Profile statistics**")
                statistics_rows = []
                preferred_order = [
                    "count",
                    "min",
                    "q1",
                    "median",
                    "q3",
                    "max",
                    "mean",
                    "stddev",
                    "variance",
                    "skewness",
                    "kurtosis",
                    "timestamp_unit",
                    "avg_length",
                    "max_length",
                ]
                for statistic in preferred_order:
                    if statistic in column["statistics"]:
                        statistics_rows.append(
                            {
                                "Statistic": statistic.replace("_", " ").title(),
                                "Value": profile_display_value(column["statistics"][statistic]),
                            }
                        )
                if statistics_rows:
                    st.dataframe(statistics_rows, use_container_width=True, hide_index=True)
                elif column["statistics"].get("top_values"):
                    st.dataframe(
                        [
                            {"Value": value, "Count": count}
                            for value, count in column["statistics"]["top_values"]
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.caption("No descriptive statistics available.")

            st.markdown("**Preprocessing mechanism**")
            preprocessing_steps = preparation_by_column.get(column["name"], [])
            if preprocessing_steps:
                st.dataframe(
                    [
                        {
                            "Action": step["action"].replace("_", " ").title(),
                            "Strategy": step["strategy"].replace("_", " "),
                            "Reason": step["reason"],
                        }
                        for step in preprocessing_steps
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
            elif column["name"] == profile["target_column"]:
                st.caption("Selected target; feature preprocessing is not applied.")
            else:
                st.caption("No preprocessing step is currently required.")

            relationship = relationships_by_column.get(column["name"])
            if relationship:
                st.caption(
                    f"{relationship['method'].replace('_', ' ').title()} association with "
                    f"{relationship['target_column']}: {relationship['value']:.4f}"
                )

    global_steps = preparation_by_column.get("__all_features__", [])
    if global_steps:
        st.subheader("Pipeline-wide preprocessing")
        st.dataframe(global_steps, use_container_width=True, hide_index=True)


def render_staged_profile_job(
    job: dict[str, Any],
    job_path: str,
    token: str,
) -> None:
    status_label = job["status"].replace("_", " ").title()
    st.progress(
        min(1.0, max(0.0, float(job["progress"]))),
        text=f"{status_label}: {job['current_stage'].replace('_', ' ')}",
    )
    if job.get("failure_message"):
        st.error(job["failure_message"])

    stages = job.get("overview_json", {}).get("stages", {})
    if stages:
        st.dataframe(
            [
                {
                    "Stage": stage.replace("_", " ").title(),
                    "Status": stage_status.replace("_", " ").title(),
                }
                for stage, stage_status in stages.items()
            ],
            use_container_width=True,
            hide_index=True,
        )

    overview_columns = job.get("overview_json", {}).get("columns", [])
    if overview_columns:
        st.subheader("Dataset structure")
        st.dataframe(
            [
                {
                    "Feature": column.get("name"),
                    "Type": column.get("semantic_type", "pending").replace("_", " "),
                    "Missing": column.get("missing_count", 0),
                    "Distinct": column.get("distinct_count", 0),
                }
                for column in overview_columns
            ],
            use_container_width=True,
            hide_index=True,
        )

    feature_profiles = {}
    for feature_name in job.get("available_features", []):
        cache_key = f"profile_feature:{job['id']}:{feature_name}"
        if cache_key not in st.session_state:
            feature_query = urllib.parse.urlencode({"column": feature_name})
            status_code, feature_response = api_request(
                "GET",
                f"{job_path}/profile-jobs/{job['id']}/feature?{feature_query}",
                token=token,
            )
            if status_code == 200 and isinstance(feature_response, dict):
                st.session_state[cache_key] = feature_response.get("profile")
        cached_feature = st.session_state.get(cache_key)
        if cached_feature:
            feature_profiles[feature_name] = cached_feature

    if not feature_profiles:
        st.info("Feature batches will appear here as they complete.")
        return

    relationships: list[dict[str, Any]] = []
    preparation: list[dict[str, Any]] = []
    if stages.get("relationships") == "completed":
        relationship_cache_key = f"profile_relationships:{job['id']}"
        if relationship_cache_key not in st.session_state:
            status_code, relationship_response = api_request(
                "GET",
                f"{job_path}/profile-jobs/{job['id']}/relationships",
                token=token,
            )
            if status_code == 200 and isinstance(relationship_response, dict):
                st.session_state[relationship_cache_key] = relationship_response.get(
                    "relationships",
                    [],
                )
        relationships = st.session_state.get(relationship_cache_key, [])
    if stages.get("preparation") == "completed":
        preparation_cache_key = f"profile_preparation:{job['id']}"
        if preparation_cache_key not in st.session_state:
            status_code, preparation_response = api_request(
                "GET",
                f"{job_path}/profile-jobs/{job['id']}/preparation",
                token=token,
            )
            if status_code == 200 and isinstance(preparation_response, dict):
                st.session_state[preparation_cache_key] = preparation_response.get(
                    "preparation",
                    [],
                )
        preparation = st.session_state.get(preparation_cache_key, [])

    inference = job.get("overview_json", {}).get("task_inference") or {
        "task_type": "pending",
        "confidence": 0.0,
        "rationale": "Task inference completes after feature profiling.",
    }
    partial_profile = {
        "row_count_analyzed": job.get("row_count") or 0,
        "column_count": job.get("total_columns") or len(feature_profiles),
        "target_column": job.get("target_column"),
        "task_inference": inference,
        "columns": list(feature_profiles.values()),
        "relationships": relationships,
        "preparation_plan": preparation,
        "warnings": job.get("warnings_json", []),
    }
    render_profile(partial_profile)


@st.fragment(run_every="2s")
def poll_profile_job(
    job_path: str,
    job_id: str,
    token: str,
) -> None:
    status_code, job = api_request(
        "GET",
        f"{job_path}/profile-jobs/{job_id}",
        token=token,
    )
    if status_code != 200 or not isinstance(job, dict):
        st.error(job)
        return
    render_staged_profile_job(job, job_path, token)
    if job["status"] in {"succeeded", "failed", "cancelled"}:
        st.rerun()


def _render_model_evaluation(
    entry: dict[str, Any],
    metric_directions: dict[str, str],
) -> None:
    metrics_tab, diagnostics_tab, parameters_tab = st.tabs(["Metrics", "Diagnostics", "Parameters"])
    with metrics_tab:
        st.dataframe(
            [
                {
                    "Metric": name.replace("_", " ").title(),
                    "Value": value,
                    "Objective": metric_directions.get(name, "review"),
                }
                for name, value in entry.get("metrics", {}).items()
            ],
            use_container_width=True,
            hide_index=True,
        )
    diagnostics = entry.get("diagnostics", {})
    with diagnostics_tab:
        cross_validation = diagnostics.get("cross_validation", {})
        standard_deviations = cross_validation.get(
            "metric_standard_deviations",
            {},
        )
        if standard_deviations:
            st.bar_chart(
                pd.DataFrame(
                    {
                        "Standard deviation": standard_deviations,
                    }
                ),
                horizontal=True,
            )
            fold_metrics = cross_validation.get("fold_metrics", [])
            if fold_metrics:
                fold_rows = [
                    {
                        "Fold": fold_number,
                        "Metric": metric.replace("_", " ").title(),
                        "Value": value,
                    }
                    for fold_number, fold in enumerate(fold_metrics, start=1)
                    for metric, value in fold.items()
                ]
                st.vega_lite_chart(
                    pd.DataFrame(fold_rows),
                    {
                        "mark": {"type": "line", "point": True},
                        "encoding": {
                            "x": {"field": "Fold", "type": "ordinal"},
                            "y": {"field": "Value", "type": "quantitative"},
                            "color": {"field": "Metric", "type": "nominal"},
                            "tooltip": [
                                {"field": "Fold"},
                                {"field": "Metric"},
                                {"field": "Value", "format": ".4f"},
                            ],
                        },
                    },
                    use_container_width=True,
                )
        elif cross_validation:
            cv_values = {
                "Mean": cross_validation.get("mean"),
                "Standard deviation": cross_validation.get("standard_deviation"),
            }
            st.bar_chart(
                pd.DataFrame(
                    [
                        {"Statistic": name, "Value": value}
                        for name, value in cv_values.items()
                        if value is not None
                    ]
                ),
                x="Statistic",
                y="Value",
            )
        if diagnostics.get("confusion_matrix") is not None:
            labels = diagnostics.get("labels", [])
            matrix_rows = [
                {
                    "Actual": labels[row_index],
                    "Predicted": labels[column_index],
                    "Count": value,
                }
                for row_index, row in enumerate(diagnostics["confusion_matrix"])
                for column_index, value in enumerate(row)
            ]
            st.vega_lite_chart(
                pd.DataFrame(matrix_rows),
                {
                    "layer": [
                        {
                            "mark": "rect",
                            "encoding": {
                                "x": {"field": "Predicted", "type": "nominal"},
                                "y": {"field": "Actual", "type": "nominal"},
                                "color": {
                                    "field": "Count",
                                    "type": "quantitative",
                                    "scale": {"scheme": "blues"},
                                },
                                "tooltip": [
                                    {"field": "Actual"},
                                    {"field": "Predicted"},
                                    {"field": "Count"},
                                ],
                            },
                        },
                        {
                            "mark": {"type": "text", "color": "black"},
                            "encoding": {
                                "x": {"field": "Predicted", "type": "nominal"},
                                "y": {"field": "Actual", "type": "nominal"},
                                "text": {"field": "Count", "type": "quantitative"},
                            },
                        },
                    ]
                },
                use_container_width=True,
            )
        classification_report = diagnostics.get("classification_report", {})
        if classification_report:
            report_rows = [
                {
                    "Class": str(label),
                    "Metric": metric.replace("-", " ").title(),
                    "Value": value,
                }
                for label, values in classification_report.items()
                if isinstance(values, dict)
                for metric, value in values.items()
                if metric in {"precision", "recall", "f1-score"}
            ]
            st.vega_lite_chart(
                pd.DataFrame(report_rows),
                {
                    "mark": "bar",
                    "encoding": {
                        "x": {"field": "Class", "type": "nominal"},
                        "xOffset": {"field": "Metric"},
                        "y": {
                            "field": "Value",
                            "type": "quantitative",
                            "scale": {"domain": [0, 1]},
                        },
                        "color": {"field": "Metric", "type": "nominal"},
                        "tooltip": [
                            {"field": "Class"},
                            {"field": "Metric"},
                            {"field": "Value", "format": ".4f"},
                        ],
                    },
                },
                use_container_width=True,
            )
        if diagnostics.get("prediction_distribution"):
            st.bar_chart(
                pd.DataFrame(
                    {
                        "Predictions": diagnostics["prediction_distribution"],
                    }
                )
            )
        if diagnostics.get("cluster_sizes"):
            st.bar_chart(pd.DataFrame({"Rows": diagnostics["cluster_sizes"]}))
        prediction_samples = diagnostics.get("prediction_samples", [])
        if prediction_samples:
            sample_frame = pd.DataFrame(prediction_samples)
            st.scatter_chart(
                sample_frame,
                x="actual",
                y="predicted",
            )
            st.vega_lite_chart(
                sample_frame,
                {
                    "mark": "bar",
                    "encoding": {
                        "x": {
                            "field": "residual",
                            "type": "quantitative",
                            "bin": {"maxbins": 30},
                            "title": "Residual",
                        },
                        "y": {
                            "aggregate": "count",
                            "type": "quantitative",
                            "title": "Rows",
                        },
                        "tooltip": [{"aggregate": "count", "title": "Rows"}],
                    },
                },
                use_container_width=True,
            )
            if diagnostics.get("chronological_holdout"):
                st.line_chart(sample_frame.set_index("order")[["actual", "predicted"]])
        summaries = [
            (name, values)
            for name, values in diagnostics.items()
            if name.endswith("_summary") and isinstance(values, dict)
        ]
        if summaries:
            summary_rows = [
                {
                    "Statistic": statistic.replace("_", " ").title(),
                    "Series": name.replace("_summary", "").title(),
                    "Value": value,
                }
                for name, values in summaries
                for statistic, value in values.items()
            ]
            st.vega_lite_chart(
                pd.DataFrame(summary_rows),
                {
                    "mark": {"type": "line", "point": True},
                    "encoding": {
                        "x": {
                            "field": "Statistic",
                            "type": "ordinal",
                            "sort": [
                                "Minimum",
                                "Q1",
                                "Median",
                                "Mean",
                                "Q3",
                                "Maximum",
                                "Standard Deviation",
                            ],
                        },
                        "y": {"field": "Value", "type": "quantitative"},
                        "color": {"field": "Series", "type": "nominal"},
                        "tooltip": [
                            {"field": "Series"},
                            {"field": "Statistic"},
                            {"field": "Value", "format": ".4f"},
                        ],
                    },
                },
                use_container_width=True,
            )
        scalar_diagnostics = {
            key.replace("_", " ").title(): value
            for key, value in diagnostics.items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
        if scalar_diagnostics:
            st.bar_chart(
                pd.DataFrame({"Value": scalar_diagnostics}),
                horizontal=True,
            )
    with parameters_tab:
        st.json(entry.get("best_params", {}))


def render_validation_workspace(
    project_id: str,
    access_token: str,
    training_run: dict[str, Any],
    leaderboard: dict[str, Any],
    datasets: list[dict[str, Any]],
) -> None:
    successful_models = [
        entry["model"]
        for entry in leaderboard.get("entries", [])
        if entry.get("status") == "succeeded"
    ]
    if not successful_models:
        return

    validation_tab, explain_tab, results_tab = st.tabs(
        ["External validation", "Explainability", "Analysis results"]
    )
    with validation_tab:
        version_options: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        for dataset in datasets:
            status_code, versions = api_request(
                "GET",
                (f"/projects/{project_id}/datasets/{dataset['id']}/versions"),
                token=access_token,
            )
            if status_code != 200 or not isinstance(versions, list):
                continue
            for version in versions:
                version_options[str(version["id"])] = (dataset, version)
        if version_options:
            validation_model = st.selectbox(
                "Model",
                successful_models,
                key=f"validation_model:{training_run['id']}",
            )
            validation_version_id = stable_selectbox(
                "External dataset version",
                list(version_options),
                key=f"validation_version:{training_run['id']}",
                format_func=lambda version_id: (
                    f"{version_options[version_id][0]['name']} - "
                    f"v{version_options[version_id][1]['version_number']}"
                ),
            )
            evaluation_column = None
            selected_version = version_options[validation_version_id][1]
            if training_run["task_type"] == "clustering":
                schema = selected_version.get("schema_json", {})
                columns = [
                    column["name"] for column in schema.get("columns", []) if column.get("name")
                ]
                evaluation_choice = st.selectbox(
                    "Reference label",
                    ["No reference label", *columns],
                    key=f"validation_label:{training_run['id']}",
                )
                if evaluation_choice != "No reference label":
                    evaluation_column = evaluation_choice
            if st.button(
                "Run external validation",
                use_container_width=True,
                key=f"validate_model:{training_run['id']}",
            ):
                status_code, response = api_request(
                    "POST",
                    (f"/projects/{project_id}/training/runs/{training_run['id']}/validations"),
                    payload={
                        "model_name": validation_model,
                        "dataset_version_id": validation_version_id,
                        "evaluation_column": evaluation_column,
                        "expected_minutes": 5,
                    },
                    token=access_token,
                )
                if status_code == 202:
                    st.success(f"Validation job {response['run']['id']} queued.")
                    st.rerun()
                else:
                    st.error(response)

    with explain_tab:
        explanation_model = st.selectbox(
            "Model",
            successful_models,
            key=f"explanation_model:{training_run['id']}",
        )
        max_rows = st.slider(
            "SHAP sample rows",
            min_value=20,
            max_value=1000,
            value=200,
            step=20,
            key=f"shap_rows:{training_run['id']}",
        )
        if st.button(
            "Calculate SHAP",
            use_container_width=True,
            key=f"explain_model:{training_run['id']}",
        ):
            status_code, response = api_request(
                "POST",
                (f"/projects/{project_id}/training/runs/{training_run['id']}/explanations"),
                payload={
                    "model_name": explanation_model,
                    "max_rows": max_rows,
                    "expected_minutes": 10,
                },
                token=access_token,
            )
            if status_code == 202:
                st.success(f"Explainability job {response['run']['id']} queued.")
                st.rerun()
            else:
                st.error(response)

    analyses_status, analyses = api_request(
        "GET",
        (f"/projects/{project_id}/training/runs/{training_run['id']}/analyses"),
        token=access_token,
    )
    if analyses_status != 200 or not isinstance(analyses, list):
        with results_tab:
            st.error(analyses)
        return
    with results_tab:
        if not analyses:
            st.info("No validation or explainability results yet.")
        else:
            st.dataframe(
                [
                    {
                        "Analysis": run["run_name"] or run["id"],
                        "Type": run["run_kind"],
                        "Status": run["status"],
                        "Created": run["created_at"],
                    }
                    for run in analyses
                ],
                use_container_width=True,
                hide_index=True,
            )
            analysis_options = {str(run["id"]): run for run in analyses}
            analysis_id = stable_selectbox(
                "Analysis result",
                list(analysis_options),
                key=f"analysis_result:{training_run['id']}",
                format_func=lambda run_id: (
                    f"{analysis_options[run_id]['run_name']} - {analysis_options[run_id]['status']}"
                ),
            )
            result_status, result = api_request(
                "GET",
                (
                    f"/projects/{project_id}/training/runs/"
                    f"{training_run['id']}/analyses/{analysis_id}"
                ),
                token=access_token,
            )
            selected_analysis = analysis_options[analysis_id]
            if selected_analysis.get("plain_english_failure"):
                st.error(selected_analysis["plain_english_failure"])
            if result_status == 200 and isinstance(result, dict):
                if result.get("metrics") or result.get("diagnostics"):
                    _render_model_evaluation(
                        {
                            "metrics": result.get("metrics", {}),
                            "diagnostics": result.get("diagnostics", {}),
                            "best_params": {},
                        },
                        {name: "review" for name in result.get("metrics", {})},
                    )
                if result.get("feature_importance"):
                    importance = pd.DataFrame(result["feature_importance"][:30]).set_index(
                        "feature"
                    )
                    st.bar_chart(
                        importance,
                        y="mean_absolute_shap",
                        horizontal=True,
                    )
                if result.get("artifacts"):
                    st.dataframe(
                        [
                            {
                                "Artifact": artifact["name"],
                                "Type": artifact["kind"],
                                "Size": artifact["byte_size"],
                            }
                            for artifact in result["artifacts"]
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )
    if any(run["status"] in {"queued", "precheck_running", "running"} for run in analyses):
        time.sleep(2)
        st.rerun()


def render_training_workspace(project_id: str, access_token: str) -> None:
    status_code, datasets = api_request(
        "GET",
        f"/projects/{project_id}/datasets",
        token=access_token,
    )
    if status_code != 200 or not isinstance(datasets, list):
        st.error(datasets)
        return
    if not datasets:
        st.info("Upload and profile a dataset before training.")
        return

    dataset_options = {str(dataset["id"]): dataset for dataset in datasets}
    selected_dataset_id = stable_selectbox(
        "Training dataset",
        list(dataset_options),
        key=f"training_dataset:{project_id}",
        format_func=lambda dataset_id: dataset_options[dataset_id]["name"],
    )
    selected_dataset = dataset_options[selected_dataset_id]
    status_code, versions = api_request(
        "GET",
        f"/projects/{project_id}/datasets/{selected_dataset['id']}/versions",
        token=access_token,
    )
    if status_code != 200 or not isinstance(versions, list) or not versions:
        st.error(versions if status_code != 200 else "No dataset versions available.")
        return

    version_options = {str(version["id"]): version for version in versions}
    selected_version_id = stable_selectbox(
        "Training dataset version",
        list(version_options),
        key=f"training_version:{selected_dataset['id']}",
        format_func=lambda version_id: (
            f"v{version_options[version_id]['version_number']} - "
            f"{version_options[version_id]['status']}"
        ),
    )
    selected_version = version_options[selected_version_id]
    profile_path = (
        f"/projects/{project_id}/datasets/{selected_dataset['id']}"
        f"/versions/{selected_version['id']}/profile-jobs/latest"
    )
    profile_status, latest_profile = api_request(
        "GET",
        profile_path,
        token=access_token,
    )
    if (
        profile_status != 200
        or not isinstance(latest_profile, dict)
        or latest_profile.get("status") != "succeeded"
    ):
        st.warning("A completed dataset profile is required before training.")
        return

    inference = latest_profile.get("overview_json", {}).get("task_inference", {})
    inferred_task_type = inference.get("task_type", "classification")
    schema = selected_version.get(
        "schema_json",
        selected_version.get("dataset_schema", {}),
    )
    column_names = [column["name"] for column in schema.get("columns", []) if column.get("name")]
    task_options = ["classification", "regression", "time_series", "clustering"]
    task_type = st.selectbox(
        "Task type",
        task_options,
        index=(task_options.index(inferred_task_type) if inferred_task_type in task_options else 0),
        format_func=lambda value: value.replace("_", " ").title(),
        key=f"training_task:{selected_version['id']}",
    )
    active_target = latest_profile.get("target_column")
    if task_type == "clustering":
        target_column = None
    else:
        if not column_names:
            st.error("The dataset schema does not contain any columns.")
            return
        target_column = st.selectbox(
            "Training target",
            column_names,
            index=(column_names.index(active_target) if active_target in column_names else 0),
            key=f"training_target:{selected_version['id']}:{task_type}",
        )
    st.caption(
        f"Task: {task_type.replace('_', ' ').title()} | Target: {target_column or 'No target'}"
    )

    estimator_query = urllib.parse.urlencode({"task_type": task_type})
    estimator_status, estimators = api_request(
        "GET",
        f"/projects/{project_id}/training/estimators?{estimator_query}",
        token=access_token,
    )
    if estimator_status != 200 or not isinstance(estimators, list):
        st.error(estimators)
        return
    estimator_by_name = {estimator["name"]: estimator for estimator in estimators}
    selected_models = st.multiselect(
        "Models",
        options=list(estimator_by_name),
        default=[estimator["name"] for estimator in estimators if estimator["default_selected"]],
        format_func=lambda name: (
            f"{name} | {estimator_by_name[name]['cost_tier']} cost"
            f"{' | tuned' if estimator_by_name[name]['tunable'] else ''}"
        ),
        max_selections=12,
        key=f"training_models:{selected_version['id']}:{task_type}",
    )
    evaluation_column = None
    if task_type == "clustering":
        evaluation_selection = st.selectbox(
            "Evaluation label",
            ["No reference label", *column_names],
            key=f"clustering_evaluation:{selected_version['id']}",
        )
        if evaluation_selection != "No reference label":
            evaluation_column = evaluation_selection

    controls = st.columns(4)
    expected_minutes = controls[0].number_input(
        "Planned duration (minutes)",
        min_value=1,
        max_value=120,
        value=10,
    )
    prefer_gpu = controls[1].toggle("Prefer GPU", value=False)
    cv_folds = controls[2].selectbox(
        "Cross-validation folds",
        options=[2, 3, 4, 5],
        index=1,
    )
    estimate_clicked = controls[3].button(
        "Estimate resources",
        use_container_width=True,
        disabled=not selected_models,
    )
    optimization_iterations = st.slider(
        "Bayesian search iterations per model",
        min_value=1,
        max_value=15,
        value=5,
    )
    estimate_key = f"training_estimate:{selected_version['id']}"
    estimate_payload = {
        "dataset_version_id": selected_version["id"],
        "target_column": target_column,
        "evaluation_column": evaluation_column,
        "task_type": task_type,
        "prefer_gpu": prefer_gpu,
        "expected_minutes": int(expected_minutes),
        "candidate_limit": len(selected_models),
        "candidate_models": selected_models,
        "optimization_iterations": optimization_iterations,
        "cv_folds": cv_folds,
    }
    if estimate_clicked:
        status_code, estimate = api_request(
            "POST",
            f"/projects/{project_id}/training/estimate",
            payload=estimate_payload,
            token=access_token,
        )
        if status_code == 200 and isinstance(estimate, dict):
            st.session_state[estimate_key] = {
                "payload": estimate_payload,
                "estimate": estimate,
            }
        else:
            st.error(estimate)

    estimate_state = st.session_state.get(estimate_key, {})
    estimate = (
        estimate_state.get("estimate")
        if estimate_state.get("payload") == estimate_payload
        else None
    )
    if estimate:
        estimate_columns = st.columns(7)
        estimate_columns[0].metric("CPU request", estimate["cpu_request_cores"])
        estimate_columns[1].metric("Memory request", f"{estimate['memory_request_mb']} MiB")
        estimate_columns[2].metric(
            "Working set",
            f"{estimate['estimated_working_set_mb']} MiB",
        )
        estimate_columns[3].metric("Core-hours", estimate["estimated_core_hours"])
        estimate_columns[4].metric(
            "Cluster CPU free",
            f"{estimate['capacity']['available_cpu_cores']:.2f}",
        )
        estimate_columns[5].metric(
            "Cluster CPU used",
            f"{estimate['capacity'].get('used_cpu_cores', 0):.2f}",
        )
        estimate_columns[6].metric(
            "Runtime safety limit",
            f"{estimate['active_deadline_seconds'] // 3600} h",
        )
        if estimate["warnings"]:
            st.warning("\n".join(estimate["warnings"]))
        if estimate["blockers"]:
            st.error("\n".join(estimate["blockers"]))
        run_name = st.text_input(
            "Run name",
            value=f"{selected_dataset['name']}-v{selected_version['version_number']}",
        )
        if st.button(
            "Launch training",
            use_container_width=True,
            disabled=not estimate["can_launch"],
        ):
            launch_status, launch_response = api_request(
                "POST",
                f"/projects/{project_id}/training/runs",
                payload={
                    **estimate_payload,
                    "run_name": run_name,
                    "params": {},
                },
                token=access_token,
            )
            if launch_status == 202 and isinstance(launch_response, dict):
                st.success(f"Training job {launch_response['run']['id']} queued.")
                st.session_state.pop(estimate_key, None)
                st.rerun()
            else:
                st.error(launch_response)

    st.subheader("Training runs")
    runs_status, runs = api_request(
        "GET",
        f"/projects/{project_id}/training/runs",
        token=access_token,
    )
    if runs_status != 200 or not isinstance(runs, list):
        st.error(runs)
        return
    if not runs:
        st.info("No training runs yet.")
        return
    st.dataframe(
        [
            {
                "Run": run["run_name"] or run["id"],
                "Status": run["status"],
                "Task": run["task_type"],
                "CPU": run["cpu_request_cores"],
                "Memory MiB": run["memory_request_mb"],
                "Created": run["created_at"],
            }
            for run in runs
        ],
        use_container_width=True,
        hide_index=True,
    )
    run_options = {str(run["id"]): run for run in runs}
    run_selector_key = f"training_run:{project_id}"
    pending_run_id = st.session_state.pop(
        f"pending_training_run:{project_id}",
        None,
    )
    if pending_run_id in run_options:
        st.session_state[run_selector_key] = pending_run_id
    selected_run_id = stable_selectbox(
        "Run details",
        list(run_options),
        key=run_selector_key,
        format_func=lambda run_id: (
            f"{run_options[run_id]['run_name'] or run_id} - {run_options[run_id]['status']}"
        ),
    )
    selected_run = run_options[selected_run_id]
    detail_status, run_detail = api_request(
        "GET",
        f"/projects/{project_id}/training/runs/{selected_run['id']}",
        token=access_token,
    )
    if detail_status == 200 and isinstance(run_detail, dict):
        st.caption(
            f"Status: {run_detail['status']} | Job: "
            f"{run_detail.get('k8s_job_name') or 'not submitted'}"
        )
        if run_detail.get("plain_english_failure"):
            st.error(run_detail["plain_english_failure"])
        if run_detail.get("failure_message"):
            st.code(run_detail["failure_message"], language="text")
        leaderboard = None
        leaderboard_status, leaderboard = api_request(
            "GET",
            f"/projects/{project_id}/training/runs/{selected_run['id']}/leaderboard",
            token=access_token,
        )
        if leaderboard_status == 200 and isinstance(leaderboard, dict):
            entries = leaderboard.get("entries", [])
            if entries:
                winner = leaderboard.get("winner")
                primary_metric = leaderboard.get("primary_metric")
                if winner:
                    st.subheader("Model leaderboard")
                    st.caption(
                        f"Winner: {winner} | Ranked by: "
                        f"{str(primary_metric).replace('_', ' ').title()}"
                    )
                metric_names = sorted(
                    {metric for entry in entries for metric in entry.get("metrics", {})}
                )
                st.dataframe(
                    [
                        {
                            "Rank": entry.get("rank"),
                            "Model": entry["model"],
                            "Status": entry["status"],
                            "Cost": entry.get("cost_tier"),
                            **{
                                (
                                    f"{metric.replace('_', ' ').title()} "
                                    f"({leaderboard.get('metric_directions', {}).get(metric, '')})"
                                ): entry.get("metrics", {}).get(metric)
                                for metric in metric_names
                            },
                            "Duration (s)": entry.get("duration_seconds"),
                        }
                        for entry in entries
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
                for entry in entries:
                    with st.expander(f"{entry['model']} details"):
                        if entry.get("mlflow_run_id"):
                            st.caption(f"MLflow run: {entry['mlflow_run_id']}")
                        if entry.get("extension_run_id"):
                            st.caption(f"Added by run: {entry['extension_run_id']}")
                        if entry.get("error"):
                            st.error(entry["error"])
                        else:
                            _render_model_evaluation(
                                entry,
                                leaderboard.get("metric_directions", {}),
                            )
        if run_detail["status"] == "succeeded" and isinstance(leaderboard, dict):
            add_query = urllib.parse.urlencode({"task_type": run_detail["task_type"]})
            add_catalog_status, add_catalog = api_request(
                "GET",
                (f"/projects/{project_id}/training/estimators?{add_query}"),
                token=access_token,
            )
            completed_models = {
                entry["model"]
                for entry in leaderboard.get("entries", [])
                if entry.get("status") == "succeeded"
            }
            if add_catalog_status == 200 and isinstance(add_catalog, list):
                available_estimators = {
                    estimator["name"]: estimator
                    for estimator in add_catalog
                    if estimator["name"] not in completed_models
                }
                if available_estimators:
                    st.subheader("Add models")
                    added_models = st.multiselect(
                        "Additional models",
                        options=list(available_estimators),
                        format_func=lambda name: (
                            f"{name} | "
                            f"{available_estimators[name]['cost_tier']} cost"
                            f"{' | tuned' if available_estimators[name]['tunable'] else ''}"
                        ),
                        max_selections=12,
                        key=f"add_models:{selected_run['id']}",
                    )
                    add_controls = st.columns(4)
                    add_iterations = add_controls[0].number_input(
                        "Search iterations",
                        min_value=1,
                        max_value=25,
                        value=int(
                            run_detail.get("params", {}).get(
                                "optimization_iterations",
                                5,
                            )
                        ),
                        key=f"add_iterations:{selected_run['id']}",
                    )
                    add_folds = add_controls[1].selectbox(
                        "CV folds",
                        options=[2, 3, 4, 5],
                        index=max(
                            0,
                            min(
                                3,
                                int(
                                    run_detail.get("params", {}).get(
                                        "cv_folds",
                                        3,
                                    )
                                )
                                - 2,
                            ),
                        ),
                        key=f"add_folds:{selected_run['id']}",
                    )
                    add_minutes = add_controls[2].number_input(
                        "Planned duration (minutes)",
                        min_value=1,
                        max_value=120,
                        value=int(
                            run_detail.get("params", {}).get(
                                "expected_minutes",
                                10,
                            )
                        ),
                        key=f"add_minutes:{selected_run['id']}",
                    )
                    add_gpu = add_controls[3].toggle(
                        "Prefer GPU",
                        value=bool(
                            run_detail.get("params", {}).get(
                                "prefer_gpu",
                                False,
                            )
                        ),
                        key=f"add_gpu:{selected_run['id']}",
                    )
                    if st.button(
                        "Train additional models",
                        use_container_width=True,
                        disabled=not added_models,
                        key=f"add_models_button:{selected_run['id']}",
                    ):
                        add_status, add_response = api_request(
                            "POST",
                            (f"/projects/{project_id}/training/runs/{selected_run['id']}/models"),
                            payload={
                                "candidate_models": added_models,
                                "optimization_iterations": int(add_iterations),
                                "cv_folds": int(add_folds),
                                "expected_minutes": int(add_minutes),
                                "prefer_gpu": add_gpu,
                            },
                            token=access_token,
                        )
                        if add_status == 202 and isinstance(
                            add_response,
                            dict,
                        ):
                            extension = add_response["run"]
                            st.session_state[f"pending_training_run:{project_id}"] = str(
                                extension["id"]
                            )
                            st.success(f"Training job {extension['id']} queued.")
                            st.rerun()
                        else:
                            st.error(add_response)
            render_validation_workspace(
                project_id,
                access_token,
                run_detail,
                leaderboard,
                datasets,
            )
        if run_detail["status"] in {"queued", "precheck_running", "running"}:
            if st.button("Cancel training", use_container_width=True):
                api_request(
                    "POST",
                    f"/projects/{project_id}/training/runs/{selected_run['id']}/cancel",
                    token=access_token,
                )
                st.rerun()
            time.sleep(2)
            st.rerun()
        elif run_detail["status"] in {"failed", "cancelled", "preempted"}:
            if st.button("Restart training", use_container_width=True):
                restart_status, restart_response = api_request(
                    "POST",
                    (f"/projects/{project_id}/training/runs/{selected_run['id']}/restart"),
                    token=access_token,
                )
                if restart_status == 202:
                    restarted_run = restart_response["run"]
                    st.session_state[f"pending_training_run:{project_id}"] = str(
                        restarted_run["id"]
                    )
                    st.success(f"Training job {restarted_run['id']} queued.")
                    st.rerun()
                else:
                    st.error(restart_response)


def render_project_detail(project_id: str, access_token: str) -> None:
    status_code, project = api_request("GET", f"/projects/{project_id}", token=access_token)
    if status_code != 200 or not isinstance(project, dict):
        st.error(project)
        return

    st.header(project["name"])
    if project.get("description"):
        st.write(project["description"])
    st.caption(f"Project status: {project['status']}")

    datasets_tab, upload_tab, training_tab = st.tabs(
        ["Datasets and profiling", "Upload dataset", "Training"]
    )

    with upload_tab:
        upload_file = st.file_uploader(
            "Dataset file",
            type=["csv", "parquet", "xlsx", "xls", "json", "jsonl"],
        )
        dataset_name = st.text_input("Dataset name", value=project["name"])
        description = st.text_area("Dataset description")
        if st.button("Upload dataset", use_container_width=True, disabled=upload_file is None):
            assert upload_file is not None
            file_bytes = upload_file.getvalue()
            upload_timeout = max(UPLOAD_TIMEOUT_SECONDS, int(len(file_bytes) / 1024 / 1024) * 8)
            with st.spinner("Uploading and inspecting dataset..."):
                status_code, upload_response = api_request(
                    "POST",
                    f"/projects/{project_id}/datasets/upload",
                    payload={
                        "dataset_name": dataset_name,
                        "description": description,
                        "filename": upload_file.name,
                        "content_base64": base64.b64encode(file_bytes).decode("ascii"),
                        "tags": {},
                    },
                    token=access_token,
                    timeout=upload_timeout,
                )
            if status_code == 201 and isinstance(upload_response, dict):
                uploaded_dataset = upload_response["dataset"]
                uploaded_version = upload_response["version"]
                st.session_state[f"dataset_selector:{project_id}"] = str(uploaded_dataset["id"])
                st.session_state[f"version_selector:{project_id}:{uploaded_dataset['id']}"] = str(
                    uploaded_version["id"]
                )
                st.session_state[f"upload_notice:{project_id}"] = (
                    f"Uploaded version {uploaded_version['version_number']} "
                    f"for {uploaded_dataset['name']}. Profiling started in the background."
                )
                st.session_state[f"profile_job:{uploaded_version['id']}"] = str(
                    upload_response["profiling_job_id"]
                )
                st.rerun()
            else:
                st.error(upload_response)

    with datasets_tab:
        upload_notice = st.session_state.pop(f"upload_notice:{project_id}", None)
        if upload_notice:
            st.success(upload_notice)
        status_code, datasets = api_request(
            "GET",
            f"/projects/{project_id}/datasets",
            token=access_token,
        )
        if status_code != 200 or not isinstance(datasets, list):
            st.error(datasets)
            return
        if not datasets:
            st.info("No datasets in this project yet. Use Upload dataset to add one.")
            return

        st.dataframe(
            [
                {
                    "Name": dataset["name"],
                    "Latest version": dataset["latest_version_number"],
                    "Created": dataset["created_at"],
                }
                for dataset in datasets
            ],
            use_container_width=True,
            hide_index=True,
        )

        dataset_options = {str(dataset["id"]): dataset for dataset in datasets}
        selected_dataset_id = stable_selectbox(
            "Dataset",
            list(dataset_options),
            key=f"dataset_selector:{project_id}",
            format_func=lambda dataset_id: (
                f"{dataset_options[dataset_id]['name']} ({dataset_id[:8]})"
            ),
        )
        selected_dataset = dataset_options[selected_dataset_id]
        status_code, versions = api_request(
            "GET",
            f"/projects/{project_id}/datasets/{selected_dataset['id']}/versions",
            token=access_token,
        )
        if status_code != 200 or not isinstance(versions, list):
            st.error(versions)
            return
        if not versions:
            st.info("No versions are available for this dataset.")
            return

        version_options = {str(version["id"]): version for version in versions}
        selected_version_id = stable_selectbox(
            "Dataset version",
            list(version_options),
            key=f"version_selector:{project_id}:{selected_dataset['id']}",
            format_func=lambda version_id: (
                f"v{version_options[version_id]['version_number']} - "
                f"{version_options[version_id]['status']}"
            ),
        )
        selected_version = version_options[selected_version_id]
        columns = [
            column["name"]
            for column in selected_version.get("schema_json", {}).get("columns", [])
            if "name" in column
        ]

        job_path = (
            f"/projects/{project_id}/datasets/{selected_dataset['id']}"
            f"/versions/{selected_version['id']}"
        )
        latest_status_code, latest_job = api_request(
            "GET",
            f"{job_path}/profile-jobs/latest",
            token=access_token,
        )
        if latest_status_code != 200:
            st.error(latest_job)
            latest_job = None

        target_options = ["No target"] + columns
        active_target = latest_job.get("target_column") if isinstance(latest_job, dict) else None
        default_target = active_target or "No target"
        target_state_key = f"target_selector:{selected_version['id']}"
        if target_state_key not in st.session_state:
            st.session_state[target_state_key] = default_target

        controls = st.columns([3, 1])
        selected_target = controls[0].selectbox(
            "Target column",
            target_options,
            key=target_state_key,
        )
        normalized_target = None if selected_target == "No target" else selected_target
        target_changed = isinstance(latest_job, dict) and normalized_target != latest_job.get(
            "target_column"
        )
        action_label = "Reprofile with target" if target_changed else "Start profile"
        start_profile = controls[1].button(
            action_label,
            use_container_width=True,
        )
        if isinstance(latest_job, dict):
            active_target_label = latest_job.get("target_column") or "No target"
            st.caption(f"Active profile target: {active_target_label}")
        if start_profile:
            status_code, started_job = api_request(
                "POST",
                f"{job_path}/profile-jobs",
                payload={
                    "target_column": normalized_target,
                    "force": False,
                },
                token=access_token,
            )
            if status_code in {200, 202} and isinstance(started_job, dict):
                st.session_state[f"profile_job:{selected_version['id']}"] = str(started_job["id"])
                st.rerun()
            else:
                st.error(started_job)

        if isinstance(latest_job, dict):
            st.session_state[f"profile_job:{selected_version['id']}"] = str(latest_job["id"])
            if latest_job["status"] in {"queued", "running"}:
                poll_profile_job(job_path, str(latest_job["id"]), access_token)
            else:
                render_staged_profile_job(latest_job, job_path, access_token)
        else:
            st.info("Start profiling to inspect the dataset and each feature.")

    with training_tab:
        render_training_workspace(project_id, access_token)
