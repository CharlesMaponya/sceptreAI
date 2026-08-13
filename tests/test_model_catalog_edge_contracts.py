from __future__ import annotations

import sys
from types import SimpleNamespace

import numpy as np
import pytest
from automl_api.models.enums import TaskType
from automl_api.training import model_catalog as catalog
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.linear_model import Ridge


class _UnknownTask:
    value = "unknown"

    def __hash__(self) -> int:
        return hash(self.value)


def test_xgb_label_wrapper_encodes_and_decodes_classes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class XGB:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def fit(self, features, target):
            captured["target"] = np.asarray(target).tolist()

        def predict(self, features):
            return np.asarray([1, 0])

        def predict_proba(self, features):
            return np.asarray([[0.1, 0.9], [0.8, 0.2]])

    monkeypatch.setitem(sys.modules, "xgboost", SimpleNamespace(XGBClassifier=XGB))
    wrapper = catalog.XGBLabelEncodingClassifier(n_estimators=3, device="cuda")
    assert wrapper.fit([[0], [1]], ["no", "yes"]) is wrapper
    assert captured["tree_method"] == "hist" and captured["device"] == "cuda"
    assert wrapper.predict([[0], [1]]).tolist() == ["yes", "no"]
    assert wrapper.predict_proba([[0], [1]]).shape == (2, 2)


def test_candidate_spec_tunable_and_selection_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    candidates = (
        catalog.CandidateSpec("A", Ridge(), {}, "low", True),
        catalog.CandidateSpec("B", Ridge(), {"alpha": [1]}, "low", True),
    )
    assert candidates[0].tunable is False and candidates[1].tunable is True
    monkeypatch.setattr(catalog, "candidate_catalog", lambda _task: candidates)
    assert [item.name for item in catalog.select_candidates(TaskType.REGRESSION, None, 100)] == [
        "A",
        "B",
    ]
    assert [
        item.name for item in catalog.select_candidates(TaskType.REGRESSION, ["missing", "B"], 0)
    ] == ["B"]
    assert catalog.select_candidates(TaskType.REGRESSION, ["missing"], 3) == []


class ThreadEstimator(BaseEstimator):
    def __init__(
        self, n_jobs=1, thread_count=1, device="cpu", device_type="cpu", task_type="CPU", devices=""
    ):
        self.n_jobs = n_jobs
        self.thread_count = thread_count
        self.device = device
        self.device_type = device_type
        self.task_type = task_type
        self.devices = devices


@pytest.mark.parametrize(
    ("name", "vendor", "rapids", "accelerator", "expected"),
    [
        ("RandomForestClassifier", "nvidia", True, "rapids_cuml", {}),
        ("XGBClassifier", "nvidia", False, "nvidia", {"device": "cuda"}),
        ("LGBMClassifier", "nvidia", False, "nvidia", {"device_type": "gpu"}),
        ("CatBoostClassifier", "nvidia", False, "nvidia", {"task_type": "GPU", "devices": "0"}),
        ("LGBMRegressor", "intel", False, "intel", {"device_type": "gpu"}),
        ("Ridge", "intel", False, "cpu", {}),
    ],
)
def test_accelerator_configuration_branches(name, vendor, rapids, accelerator, expected) -> None:
    candidate = catalog.CandidateSpec(name, ThreadEstimator(), {}, "low", False)
    estimator, selected = catalog.configure_estimator_for_training(
        candidate, cpu_threads=0, gpu_vendor=vendor, rapids_active=rapids
    )
    assert selected == accelerator
    assert estimator.n_jobs == 1 and estimator.thread_count == 1
    for key, value in expected.items():
        assert getattr(estimator, key) == value


@pytest.mark.parametrize(
    ("name", "vendors"),
    [
        ("Ridge", {"nvidia"}),
        ("LGBMClassifier", {"nvidia", "intel"}),
        ("XGBRegressor", {"nvidia"}),
        ("CatBoostClassifier", {"nvidia"}),
        ("Unknown", set()),
    ],
)
def test_supported_gpu_vendor_matrix(name: str, vendors: set[str]) -> None:
    assert catalog.supported_gpu_vendors(name) == vendors


def test_catalog_payload_and_discovery_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = catalog.CandidateSpec("Ridge", Ridge(), {"alpha": [1]}, "low", True)
    monkeypatch.setattr(catalog, "candidate_catalog", lambda _task: (candidate,))
    assert catalog.estimator_catalog_payload(TaskType.REGRESSION) == [
        {
            "name": "Ridge",
            "task_type": "regression",
            "mixin": "RegressorMixin",
            "tunable": True,
            "cost_tier": "low",
            "default_selected": True,
        }
    ]
    assert catalog._discovery_contract(TaskType.CLASSIFICATION)[0] == "classifier"
    assert catalog._discovery_contract(TaskType.TIME_SERIES)[0] == "regressor"
    assert catalog._discovery_contract(TaskType.CLUSTERING)[0] == "cluster"
    with pytest.raises(ValueError, match="Unsupported estimator task"):
        catalog._discovery_contract(_UnknownTask())  # type: ignore[arg-type]


def test_constructor_introspection_and_instantiation() -> None:
    class Required(BaseEstimator):
        def __init__(self, required, *, keyword, optional=2):
            self.required = required
            self.keyword = keyword
            self.optional = optional

    class Seeded(BaseEstimator):
        def __init__(self, random_state=None, n_jobs=None):
            self.random_state = random_state
            self.n_jobs = n_jobs

    assert catalog._required_constructor_parameters(Required) == ["required", "keyword"]
    seeded = catalog._instantiate_estimator(Seeded)
    assert seeded.random_state == 42 and seeded.n_jobs == 1


def test_multioutput_tag_detection_new_legacy_and_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    estimator = Ridge()
    monkeypatch.setattr(
        catalog,
        "sklearn_get_tags",
        lambda _estimator: SimpleNamespace(
            target_tags=SimpleNamespace(multi_output=True, single_output=False)
        ),
    )
    assert catalog._is_multioutput_only(estimator) is True
    monkeypatch.setattr(
        catalog, "sklearn_get_tags", lambda _estimator: (_ for _ in ()).throw(TypeError())
    )
    legacy = SimpleNamespace(_get_tags=lambda: {"multioutput_only": True})
    assert catalog._is_multioutput_only(legacy) is True  # type: ignore[arg-type]
    broken = SimpleNamespace(_get_tags=lambda: (_ for _ in ()).throw(AttributeError()))
    assert catalog._is_multioutput_only(broken) is False  # type: ignore[arg-type]


def test_cost_tiers_are_explicit() -> None:
    assert catalog._cost_tier("SVC") == "high"
    assert catalog._cost_tier("RandomForestClassifier") == "medium"
    assert catalog._cost_tier("Ridge") == "low"


def test_candidate_catalog_filters_unsafe_estimators(monkeypatch: pytest.MonkeyPatch) -> None:
    class ValidClassifier(ClassifierMixin, BaseEstimator):
        def fit(self, x, y):
            return self

        def predict(self, x):
            return np.zeros(len(x))

    class RequiredClassifier(ClassifierMixin, BaseEstimator):
        def __init__(self, required):
            self.required = required

    class ExplodingClassifier(ClassifierMixin, BaseEstimator):
        def __init__(self):
            raise RuntimeError("broken")

    catalog.candidate_catalog.cache_clear()
    monkeypatch.setattr(
        catalog,
        "all_estimators",
        lambda type_filter: [
            ("ValidClassifier", ValidClassifier),
            ("RequiredClassifier", RequiredClassifier),
            ("ExplodingClassifier", ExplodingClassifier),
            ("Ridge", Ridge),
        ],
    )
    monkeypatch.setattr(catalog, "_external_estimators", lambda _task: [])
    result = catalog.candidate_catalog(TaskType.CLASSIFICATION)
    assert [item.name for item in result] == ["ValidClassifier"]
    catalog.candidate_catalog.cache_clear()
