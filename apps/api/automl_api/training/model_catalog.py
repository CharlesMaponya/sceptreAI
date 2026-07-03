from __future__ import annotations

import inspect
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from sklearn.base import BaseEstimator, ClassifierMixin, ClusterMixin, RegressorMixin
from sklearn.preprocessing import LabelEncoder
from sklearn.utils import all_estimators
from skopt.space import Categorical, Integer, Real

from automl_api.models.enums import TaskType

try:
    from sklearn.utils import get_tags as sklearn_get_tags
except ImportError:  # scikit-learn < 1.6
    sklearn_get_tags = None


@dataclass(frozen=True)
class CandidateSpec:
    name: str
    estimator: Any
    search_space: dict[str, Any]
    cost_tier: str
    default_selected: bool

    @property
    def tunable(self) -> bool:
        return bool(self.search_space)


class XGBLabelEncodingClassifier(ClassifierMixin, BaseEstimator):
    def __init__(
        self,
        n_estimators: int = 100,
        learning_rate: float = 0.1,
        max_depth: int = 6,
        min_child_weight: float = 1.0,
        subsample: float = 1.0,
        colsample_bytree: float = 1.0,
        reg_alpha: float = 0.0,
        reg_lambda: float = 1.0,
        random_state: int = 42,
        n_jobs: int = 1,
    ) -> None:
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.max_depth = max_depth
        self.min_child_weight = min_child_weight
        self.subsample = subsample
        self.colsample_bytree = colsample_bytree
        self.reg_alpha = reg_alpha
        self.reg_lambda = reg_lambda
        self.random_state = random_state
        self.n_jobs = n_jobs

    def fit(self, features: Any, target: Any) -> XGBLabelEncodingClassifier:
        from xgboost import XGBClassifier

        self.label_encoder_ = LabelEncoder().fit(target)
        self.classes_ = self.label_encoder_.classes_
        self.model_ = XGBClassifier(
            n_estimators=self.n_estimators,
            learning_rate=self.learning_rate,
            max_depth=self.max_depth,
            min_child_weight=self.min_child_weight,
            subsample=self.subsample,
            colsample_bytree=self.colsample_bytree,
            reg_alpha=self.reg_alpha,
            reg_lambda=self.reg_lambda,
            random_state=self.random_state,
            n_jobs=self.n_jobs,
            tree_method="hist",
            eval_metric="logloss",
            verbosity=0,
        )
        self.model_.fit(features, self.label_encoder_.transform(target))
        return self

    def predict(self, features: Any) -> Any:
        encoded = self.model_.predict(features).astype(int)
        return self.label_encoder_.inverse_transform(encoded)

    def predict_proba(self, features: Any) -> Any:
        return self.model_.predict_proba(features)


DEFAULT_MODELS = {
    TaskType.CLASSIFICATION: {
        "LogisticRegression",
        "DecisionTreeClassifier",
        "RandomForestClassifier",
        "ExtraTreesClassifier",
        "HistGradientBoostingClassifier",
    },
    TaskType.REGRESSION: {
        "Ridge",
        "ElasticNet",
        "DecisionTreeRegressor",
        "RandomForestRegressor",
        "ExtraTreesRegressor",
    },
    TaskType.TIME_SERIES: {
        "Ridge",
        "ElasticNet",
        "DecisionTreeRegressor",
        "RandomForestRegressor",
        "HistGradientBoostingRegressor",
    },
    TaskType.CLUSTERING: {
        "KMeans",
        "MiniBatchKMeans",
        "Birch",
        "AgglomerativeClustering",
        "DBSCAN",
    },
}

EXCLUDED_MODELS = {
    "ClassifierChain",
    "FixedThresholdClassifier",
    "MultiOutputClassifier",
    "MultiOutputRegressor",
    "OneVsOneClassifier",
    "OneVsRestClassifier",
    "OutputCodeClassifier",
    "RegressorChain",
    "StackingClassifier",
    "StackingRegressor",
    "TunedThresholdClassifierCV",
    "VotingClassifier",
    "VotingRegressor",
    "FeatureAgglomeration",
}

HIGH_COST_MODELS = {
    "AffinityPropagation",
    "GaussianProcessClassifier",
    "GaussianProcessRegressor",
    "MLPClassifier",
    "MLPRegressor",
    "NuSVC",
    "NuSVR",
    "SpectralClustering",
    "SVC",
    "SVR",
    "TheilSenRegressor",
}

MEDIUM_COST_MODELS = {
    "BaggingClassifier",
    "BaggingRegressor",
    "ExtraTreesClassifier",
    "ExtraTreesRegressor",
    "GradientBoostingClassifier",
    "GradientBoostingRegressor",
    "HistGradientBoostingClassifier",
    "HistGradientBoostingRegressor",
    "KNeighborsClassifier",
    "KNeighborsRegressor",
    "RandomForestClassifier",
    "RandomForestRegressor",
    "LGBMClassifier",
    "LGBMRegressor",
    "XGBClassifier",
    "XGBRegressor",
}

HIGH_COST_MODELS.update({"CatBoostClassifier", "CatBoostRegressor"})


@lru_cache
def candidate_catalog(task_type: TaskType) -> tuple[CandidateSpec, ...]:
    type_filter, mixin = _discovery_contract(task_type)
    candidates = []
    for name, estimator_class in all_estimators(type_filter=type_filter):
        if name in EXCLUDED_MODELS or not issubclass(estimator_class, mixin):
            continue
        if _required_constructor_parameters(estimator_class):
            continue
        try:
            estimator = _instantiate_estimator(estimator_class)
        except Exception:
            continue
        if _is_multioutput_only(estimator) or not hasattr(estimator, "fit"):
            continue
        if task_type == TaskType.CLUSTERING:
            if name == "FeatureAgglomeration":
                continue
        elif not hasattr(estimator, "predict"):
            continue
        candidates.append(
            CandidateSpec(
                name=name,
                estimator=estimator,
                search_space=_search_spaces().get(name, {}),
                cost_tier=_cost_tier(name),
                default_selected=name in DEFAULT_MODELS[task_type],
            )
        )
    for name, estimator in _external_estimators(task_type):
        candidates.append(
            CandidateSpec(
                name=name,
                estimator=estimator,
                search_space=_search_spaces()[name],
                cost_tier=_cost_tier(name),
                default_selected=False,
            )
        )
    return tuple(candidates)


def _external_estimators(task_type: TaskType) -> list[tuple[str, Any]]:
    try:
        from catboost import CatBoostClassifier, CatBoostRegressor
        from lightgbm import LGBMClassifier, LGBMRegressor
        from xgboost import XGBRegressor
    except ImportError:
        return []

    if task_type == TaskType.CLASSIFICATION:
        return [
            ("XGBClassifier", XGBLabelEncodingClassifier()),
            (
                "LGBMClassifier",
                LGBMClassifier(random_state=42, n_jobs=1, verbosity=-1),
            ),
            (
                "CatBoostClassifier",
                CatBoostClassifier(
                    random_seed=42,
                    thread_count=1,
                    verbose=False,
                    allow_writing_files=False,
                ),
            ),
        ]
    if task_type in {TaskType.REGRESSION, TaskType.TIME_SERIES}:
        return [
            (
                "XGBRegressor",
                XGBRegressor(
                    random_state=42,
                    n_jobs=1,
                    tree_method="hist",
                    verbosity=0,
                ),
            ),
            (
                "LGBMRegressor",
                LGBMRegressor(random_state=42, n_jobs=1, verbosity=-1),
            ),
            (
                "CatBoostRegressor",
                CatBoostRegressor(
                    random_seed=42,
                    thread_count=1,
                    verbose=False,
                    allow_writing_files=False,
                ),
            ),
        ]
    return []


def select_candidates(
    task_type: TaskType,
    requested_names: list[str] | None,
    limit: int,
) -> list[CandidateSpec]:
    catalog = candidate_catalog(task_type)
    by_name = {candidate.name: candidate for candidate in catalog}
    if requested_names:
        selected = [by_name[name] for name in requested_names if name in by_name]
    else:
        selected = [candidate for candidate in catalog if candidate.default_selected]
    return selected[: max(1, min(limit, len(selected)))]


def estimator_catalog_payload(task_type: TaskType) -> list[dict[str, Any]]:
    _, mixin = _discovery_contract(task_type)
    return [
        {
            "name": candidate.name,
            "task_type": task_type.value,
            "mixin": mixin.__name__,
            "tunable": candidate.tunable,
            "cost_tier": candidate.cost_tier,
            "default_selected": candidate.default_selected,
        }
        for candidate in candidate_catalog(task_type)
    ]


def _discovery_contract(task_type: TaskType) -> tuple[str, type]:
    if task_type == TaskType.CLASSIFICATION:
        return "classifier", ClassifierMixin
    if task_type in {TaskType.REGRESSION, TaskType.TIME_SERIES}:
        return "regressor", RegressorMixin
    if task_type == TaskType.CLUSTERING:
        return "cluster", ClusterMixin
    raise ValueError(f"Unsupported estimator task type: {task_type.value}")


def _required_constructor_parameters(estimator_class: type) -> list[str]:
    return [
        parameter.name
        for parameter in inspect.signature(estimator_class).parameters.values()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind
        in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }
    ]


def _instantiate_estimator(estimator_class: type[BaseEstimator]) -> BaseEstimator:
    parameters = inspect.signature(estimator_class).parameters
    kwargs: dict[str, Any] = {}
    if "random_state" in parameters:
        kwargs["random_state"] = 42
    if "n_jobs" in parameters:
        kwargs["n_jobs"] = 1
    return estimator_class(**kwargs)


def _is_multioutput_only(estimator: BaseEstimator) -> bool:
    if sklearn_get_tags is not None:
        tags = sklearn_get_tags(estimator)
        target_tags = getattr(tags, "target_tags", None)
        if target_tags is not None:
            return bool(
                getattr(target_tags, "multi_output", False)
                and not getattr(target_tags, "single_output", True)
            )
    legacy_get_tags = getattr(estimator, "_get_tags", None)
    return bool(legacy_get_tags and legacy_get_tags().get("multioutput_only", False))


def _cost_tier(name: str) -> str:
    if name in HIGH_COST_MODELS:
        return "high"
    if name in MEDIUM_COST_MODELS:
        return "medium"
    return "low"


@lru_cache
def _search_spaces() -> dict[str, dict[str, Any]]:
    return {
        "LogisticRegression": {
            "model__C": Real(0.01, 10.0, prior="log-uniform"),
            "model__solver": Categorical(["lbfgs", "liblinear"]),
            "model__class_weight": Categorical(["balanced", None]),
            "model__max_iter": Integer(200, 1000),
        },
        "DecisionTreeClassifier": {
            "model__criterion": Categorical(["gini", "entropy", "log_loss"]),
            "model__max_depth": Integer(3, 20),
            "model__min_samples_split": Integer(2, 20),
            "model__min_samples_leaf": Integer(1, 20),
            "model__class_weight": Categorical(["balanced", None]),
        },
        "RandomForestClassifier": {
            "model__n_estimators": Integer(50, 300),
            "model__max_depth": Integer(3, 24),
            "model__min_samples_split": Integer(2, 20),
            "model__min_samples_leaf": Integer(1, 12),
            "model__max_features": Categorical(["sqrt", "log2", None]),
            "model__class_weight": Categorical(["balanced", "balanced_subsample", None]),
        },
        "ExtraTreesClassifier": {
            "model__n_estimators": Integer(50, 300),
            "model__max_depth": Integer(3, 30),
            "model__min_samples_split": Integer(2, 20),
            "model__min_samples_leaf": Integer(1, 12),
            "model__max_features": Categorical(["sqrt", "log2", None]),
            "model__class_weight": Categorical(["balanced", "balanced_subsample", None]),
        },
        "HistGradientBoostingClassifier": {
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__max_iter": Integer(50, 300),
            "model__max_leaf_nodes": Integer(15, 100),
            "model__max_depth": Integer(3, 12),
            "model__l2_regularization": Real(0.0, 10.0),
        },
        "GradientBoostingClassifier": {
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__n_estimators": Integer(50, 300),
            "model__max_depth": Integer(2, 10),
            "model__subsample": Real(0.6, 1.0),
        },
        "AdaBoostClassifier": {
            "model__n_estimators": Integer(50, 300),
            "model__learning_rate": Real(0.01, 1.0, prior="log-uniform"),
        },
        "KNeighborsClassifier": {
            "model__n_neighbors": Integer(2, 20),
            "model__weights": Categorical(["uniform", "distance"]),
            "model__p": Integer(1, 2),
        },
        "XGBClassifier": {
            "model__n_estimators": Integer(50, 300),
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__max_depth": Integer(3, 10),
            "model__min_child_weight": Real(0.5, 10.0, prior="log-uniform"),
            "model__subsample": Real(0.6, 1.0),
            "model__colsample_bytree": Real(0.6, 1.0),
        },
        "LGBMClassifier": {
            "model__n_estimators": Integer(50, 300),
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__num_leaves": Integer(15, 127),
            "model__max_depth": Integer(3, 12),
            "model__min_child_samples": Integer(10, 100),
            "model__subsample": Real(0.6, 1.0),
        },
        "CatBoostClassifier": {
            "model__iterations": Integer(50, 300),
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__depth": Integer(4, 10),
            "model__l2_leaf_reg": Real(1.0, 20.0, prior="log-uniform"),
            "model__random_strength": Real(0.0, 2.0),
        },
        "Ridge": {
            "model__alpha": Real(0.001, 100.0, prior="log-uniform"),
            "model__fit_intercept": Categorical([True, False]),
        },
        "ElasticNet": {
            "model__alpha": Real(0.0001, 10.0, prior="log-uniform"),
            "model__l1_ratio": Real(0.0, 1.0),
            "model__max_iter": Integer(500, 3000),
        },
        "DecisionTreeRegressor": {
            "model__max_depth": Integer(3, 24),
            "model__min_samples_split": Integer(2, 20),
            "model__min_samples_leaf": Integer(1, 12),
            "model__max_features": Categorical(["sqrt", "log2", None]),
        },
        "RandomForestRegressor": {
            "model__n_estimators": Integer(50, 300),
            "model__max_depth": Integer(3, 24),
            "model__min_samples_split": Integer(2, 20),
            "model__min_samples_leaf": Integer(1, 12),
            "model__max_features": Categorical(["sqrt", "log2", None]),
        },
        "ExtraTreesRegressor": {
            "model__n_estimators": Integer(50, 300),
            "model__max_depth": Integer(3, 30),
            "model__min_samples_split": Integer(2, 20),
            "model__min_samples_leaf": Integer(1, 12),
            "model__max_features": Categorical(["sqrt", "log2", None]),
        },
        "HistGradientBoostingRegressor": {
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__max_iter": Integer(50, 300),
            "model__max_leaf_nodes": Integer(15, 100),
            "model__max_depth": Integer(3, 12),
            "model__l2_regularization": Real(0.0, 10.0),
        },
        "GradientBoostingRegressor": {
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__n_estimators": Integer(50, 300),
            "model__max_depth": Integer(2, 10),
            "model__subsample": Real(0.6, 1.0),
        },
        "AdaBoostRegressor": {
            "model__n_estimators": Integer(50, 300),
            "model__learning_rate": Real(0.01, 1.0, prior="log-uniform"),
            "model__loss": Categorical(["linear", "square", "exponential"]),
        },
        "KNeighborsRegressor": {
            "model__n_neighbors": Integer(2, 20),
            "model__weights": Categorical(["uniform", "distance"]),
            "model__p": Integer(1, 2),
        },
        "XGBRegressor": {
            "model__n_estimators": Integer(50, 300),
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__max_depth": Integer(3, 10),
            "model__min_child_weight": Real(0.5, 10.0, prior="log-uniform"),
            "model__subsample": Real(0.6, 1.0),
            "model__colsample_bytree": Real(0.6, 1.0),
        },
        "LGBMRegressor": {
            "model__n_estimators": Integer(50, 300),
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__num_leaves": Integer(15, 127),
            "model__max_depth": Integer(3, 12),
            "model__min_child_samples": Integer(10, 100),
            "model__subsample": Real(0.6, 1.0),
        },
        "CatBoostRegressor": {
            "model__iterations": Integer(50, 300),
            "model__learning_rate": Real(0.01, 0.3, prior="log-uniform"),
            "model__depth": Integer(4, 10),
            "model__l2_leaf_reg": Real(1.0, 20.0, prior="log-uniform"),
            "model__random_strength": Real(0.0, 2.0),
        },
    }
