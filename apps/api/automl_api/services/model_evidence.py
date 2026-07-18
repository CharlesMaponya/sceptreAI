from __future__ import annotations

from typing import Any

from automl_api.models.enums import TaskType


def feature_processing_contract(model_name: str) -> dict[str, Any]:
    """Describe the executable preprocessing branch used by the training worker."""
    if model_name == "CategoricalNB":
        numeric = [
            "Median imputation",
            "Quantile discretization into 10 ordinal bins",
        ]
        categorical = [
            "Most-frequent imputation",
            "Ordinal encoding with an unknown-value sentinel",
            "Shift encoded values to a non-negative domain",
        ]
        branch = "categorical_naive_bayes"
    elif model_name in {"ComplementNB", "MultinomialNB"}:
        numeric = ["Median imputation", "Min-max scaling to [0, 1]"]
        categorical = [
            "Most-frequent imputation",
            "Ordinal encoding with an unknown-value sentinel",
            "Shift encoded values to a non-negative domain",
        ]
        branch = "non_negative_naive_bayes"
    else:
        numeric = ["Median imputation", "Standard scaling"]
        categorical = [
            "Most-frequent imputation",
            "Ordinal encoding with an unknown-value sentinel",
        ]
        branch = "standard_tabular"
    return {
        "branch": branch,
        "numeric_features": numeric,
        "categorical_and_text_features": categorical,
        "temporal_features": ["Normalize detected Unix timestamps to epoch days"],
        "supervised_feature_selection": (
            "Keep the top 80% of transformed features by mutual information"
        ),
        "remainder": "Drop columns outside the fitted feature contract",
    }


def model_mathematics(model_name: str, task_type: TaskType) -> dict[str, str]:
    """Return a concise mathematical contract for discovered and external estimators."""
    lower = model_name.lower()
    if "logistic" in lower:
        return _math(
            "Generalized linear classifier",
            "P(y=k | x) = softmax(βₖ₀ + βₖᵀx)",
            "Minimize regularized negative log-likelihood.",
            "Choose the class with the highest estimated probability.",
        )
    if lower in {"ridge", "ridgecv"}:
        return _math(
            "L2-regularized linear model",
            "ŷ = β₀ + xᵀβ",
            "minβ Σᵢ(yᵢ − β₀ − xᵢᵀβ)² + α‖β‖²₂",
            "Apply the fitted linear function to the processed feature vector.",
        )
    if "lasso" in lower:
        return _math(
            "L1-regularized linear model",
            "ŷ = β₀ + xᵀβ",
            "minβ Σᵢ(yᵢ − β₀ − xᵢᵀβ)² + α‖β‖₁",
            "Apply the sparse fitted linear function.",
        )
    if "elasticnet" in lower:
        return _math(
            "Elastic-net linear model",
            "ŷ = β₀ + xᵀβ",
            "minβ loss + α[ρ‖β‖₁ + (1−ρ)‖β‖²₂/2]",
            "Apply the fitted linear function after combined L1/L2 shrinkage.",
        )
    if lower in {"linearregression", "huberregressor", "ransacregressor"}:
        return _math(
            "Linear regression family",
            "ŷ = β₀ + Σⱼβⱼxⱼ",
            "Estimate coefficients that minimize the estimator-specific residual loss.",
            "Sum the intercept and weighted processed features.",
        )
    if any(
        token in lower
        for token in ("xgb", "lgbm", "catboost", "gradientboost", "histgradient")
    ):
        return _math(
            "Gradient-boosted decision trees",
            "Fₘ(x) = Fₘ₋₁(x) + ηhₘ(x)",
            "Add trees sequentially to reduce the differentiable training loss "
            "with regularization.",
            "Sum the base score and each tree contribution; map the score to a "
            "value or class probability.",
        )
    if "adaboost" in lower:
        return _math(
            "Adaptive boosting ensemble",
            "F(x) = Σₘ αₘhₘ(x)",
            "Increase the weight of poorly predicted observations between weak learners.",
            "Use the weighted learner vote or weighted regression estimate.",
        )
    if "randomforest" in lower or "extratrees" in lower or "bagging" in lower:
        rule = (
            "Use the majority class vote or averaged class probabilities."
            if task_type == TaskType.CLASSIFICATION
            else "Average the predictions produced by the fitted trees."
        )
        return _math(
            "Bagged decision-tree ensemble",
            "f̂(x) = (1/B) Σᵦ Tᵦ(x)",
            "Fit multiple randomized trees to reduce estimator variance.",
            rule,
        )
    if "decisiontree" in lower or "extratree" in lower:
        return _math(
            "Decision tree",
            "f̂(x) = Σₘ cₘ · 1[x ∈ Rₘ]",
            "Recursively choose feature thresholds that maximize impurity reduction.",
            "Return the class distribution or response value stored in the reached leaf.",
        )
    if lower.endswith("nb") or "naivebayes" in lower:
        return _math(
            "Naive Bayes classifier",
            "ŷ = argmaxₖ P(y=k) Πⱼ P(xⱼ | y=k)",
            "Estimate class priors and conditional feature distributions under "
            "conditional independence.",
            "Choose the class with the largest posterior score.",
        )
    if "kneighbors" in lower or "nearestcentroid" in lower:
        return _math(
            "Nearest-neighbour method",
            "Nₖ(x) = k closest training points under the configured distance",
            "Store the fitted reference feature space and distance configuration.",
            "Aggregate neighbour votes or responses, optionally weighted by distance.",
        )
    if lower in {"svc", "linearsvc", "nusvc", "svr", "linearsvr", "nusvr"}:
        return _math(
            "Support-vector machine",
            "f(x) = Σᵢ αᵢyᵢK(xᵢ,x) + b",
            "Optimize a maximum-margin objective with estimator-specific slack "
            "or ε-insensitive loss.",
            "Use the sign/class score for classification or the margin value for regression.",
        )
    if "discriminantanalysis" in lower:
        return _math(
            "Discriminant analysis",
            "δₖ(x) = xᵀΣₖ⁻¹μₖ − ½μₖᵀΣₖ⁻¹μₖ + log πₖ",
            "Estimate class means, priors, and shared or class-specific covariance.",
            "Choose the class with the highest discriminant score.",
        )
    if "mlp" in lower:
        return _math(
            "Feed-forward neural network",
            "hˡ = φ(Wˡhˡ⁻¹ + bˡ)",
            "Use back-propagation to minimize the task loss over network weights.",
            "Apply the fitted output activation to the final hidden representation.",
        )
    if "kmeans" in lower:
        return _math(
            "Centroid clustering",
            "min Σᵢ ‖xᵢ − μcᵢ‖²₂",
            "Alternate between assigning observations and updating cluster centroids.",
            "Assign each observation to its nearest fitted centroid.",
        )
    if "dbscan" in lower or "optics" in lower:
        return _math(
            "Density-based clustering",
            "A core point has at least min_samples neighbours within ε.",
            "Expand connected dense regions and mark isolated observations as noise.",
            "Return the density-connected cluster label or the noise label.",
        )
    if any(token in lower for token in ("agglomerative", "birch", "spectral", "affinity")):
        return _math(
            "Structure-based clustering",
            "Partition X by the estimator's affinity, hierarchy, or clustering-feature criterion.",
            "Construct the estimator-specific similarity structure and optimize its partition.",
            "Return the fitted cluster assignment.",
        )
    if "mixture" in lower or "bayesian" in lower and task_type == TaskType.CLUSTERING:
        return _math(
            "Probabilistic mixture model",
            "p(x) = Σₖ πₖ p(x | θₖ)",
            "Estimate component weights and parameters from the observed feature likelihood.",
            "Choose the component with the largest posterior responsibility.",
        )
    return _math(
        "Estimator-specific predictive model",
        "f̂(x; θ) maps the processed feature vector x to a task output.",
        "Fit θ using the estimator implementation and the recorded hyperparameters.",
        "Apply the fitted estimator's predict or fit_predict contract.",
    )


def build_model_pipeline(
    model_name: str,
    task_type: TaskType,
    state: str,
    *,
    parameters: dict[str, Any] | None = None,
    excluded_columns: list[str] | None = None,
    current_phase: str | None = None,
) -> dict[str, Any]:
    processing = feature_processing_contract(model_name)
    statuses = _pipeline_statuses(state, current_phase)
    split = {
        TaskType.CLASSIFICATION: "Stratified 80/20 holdout plus cross-validation",
        TaskType.REGRESSION: "Seeded 80/20 holdout plus cross-validation",
        TaskType.TIME_SERIES: "Ordered 80/20 holdout plus time-series cross-validation",
        TaskType.CLUSTERING: "K-fold stability evaluation on transformed features",
    }.get(task_type, "Task-aware holdout and cross-validation")
    stages = [
        _stage(
            "data",
            "Immutable data",
            statuses[0],
            "Load the selected dataset version and freeze its content hash.",
        ),
        _stage(
            "leakage",
            "Leakage gate",
            statuses[1],
            (
                f"Remove {len(excluded_columns or [])} detected leakage feature(s) before training."
                if excluded_columns
                else "Apply the profiling leakage contract before training."
            ),
        ),
        _stage("split", "Validation design", statuses[2], split),
        _stage(
            "processing",
            "Feature processing",
            statuses[3],
            f"Use the {processing['branch'].replace('_', ' ')} preprocessing branch.",
        ),
        _stage(
            "selection",
            "Feature selection",
            statuses[4],
            (
                processing["supervised_feature_selection"]
                if task_type != TaskType.CLUSTERING
                else "Retain the transformed clustering feature space."
            ),
        ),
        _stage(
            "fit",
            "Tune & fit",
            statuses[5],
            f"Fit {model_name} with the recorded search result and resource contract.",
        ),
        _stage(
            "evaluate",
            "Evaluate",
            statuses[6],
            "Calculate task-aware holdout metrics and diagnostic curves.",
        ),
        _stage(
            "persist",
            "Persist evidence",
            statuses[7],
            "Mirror the fitted pipeline to object storage and log the candidate to MLflow.",
        ),
    ]
    return {
        "model_name": model_name,
        "task_type": task_type.value,
        "state": state,
        "current_phase": current_phase,
        "stages": stages,
        "feature_processing": processing,
        "parameters": parameters or {},
        "diagram": _pipeline_diagram(model_name, task_type, processing, excluded_columns or []),
    }


def _pipeline_diagram(
    model_name: str,
    task_type: TaskType,
    processing: dict[str, Any],
    excluded_columns: list[str],
) -> dict[str, Any]:
    """Expose the fitted sklearn-style graph for UI and audit rendering."""
    return {
        "input_gates": [
            "Immutable dataset version",
            (
                f"Leakage gate · {len(excluded_columns)} feature(s) removed"
                if excluded_columns
                else "Leakage gate · no excluded features"
            ),
            "Temporal normalization",
        ],
        "transformer": {
            "name": "preprocessor",
            "type": "ColumnTransformer",
            "branches": [
                {
                    "key": "numeric",
                    "label": "Numeric",
                    "steps": list(processing["numeric_features"]),
                },
                {
                    "key": "categorical",
                    "label": "Categorical & text",
                    "steps": list(processing["categorical_and_text_features"]),
                },
            ],
        },
        "selector": (
            None
            if task_type == TaskType.CLUSTERING
            else {
                "name": "feature selector",
                "type": "SelectPercentile",
                "summary": processing["supervised_feature_selection"],
            }
        ),
        "estimator": {"name": "estimator", "type": model_name},
    }


def _pipeline_statuses(state: str, current_phase: str | None) -> list[str]:
    if state == "succeeded":
        return ["completed"] * 8
    if state in {"failed", "cancelled", "preempted"}:
        terminal = "cancelled" if state == "cancelled" else "failed"
        return [
            "completed",
            "completed",
            "review",
            "review",
            "review",
            terminal,
            "not_run",
            "not_run",
        ]
    if state == "running":
        phase_index = {
            "preparing_data": 3,
            "hyperparameter_search": 5,
            "cross_validating": 5,
            "fitting_final_model": 5,
            "evaluating": 6,
            "logging_to_mlflow": 7,
            "saving_model": 7,
        }.get(current_phase or "", 5)
        return [
            (
                "completed"
                if index < phase_index
                else "running"
                if index == phase_index
                else "planned"
            )
            for index in range(8)
        ]
    return ["ready", *(["planned"] * 7)]


def _stage(key: str, label: str, status: str, summary: str) -> dict[str, str]:
    return {"key": key, "label": label, "status": status, "summary": summary}


def _math(family: str, equation: str, objective: str, prediction: str) -> dict[str, str]:
    return {
        "family": family,
        "equation": equation,
        "training_objective": objective,
        "prediction_rule": prediction,
    }
