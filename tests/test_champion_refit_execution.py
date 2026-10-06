from __future__ import annotations

import hashlib
import io
import uuid

import joblib
import numpy as np
import pandas as pd
import pytest
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from automl_api.training.champion_refit import RefitPlan, execute_refit
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


@pytest.fixture
def refit_case(tmp_path):
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="test")
    project, scope, attempt, run, dataset = (uuid.uuid4() for _ in range(5))
    model = Pipeline([("scale", StandardScaler()), ("model", LinearRegression())])
    model.fit(pd.DataFrame({"x": [0, 1, 2]}), [0, 100, 200])
    buffer = io.BytesIO()
    joblib.dump(model, buffer)
    candidate_bytes = buffer.getvalue()
    candidate = store.put_bytes(f"projects/{project}/runs/{run}/candidate.joblib", candidate_bytes)
    partitions = []
    for role, values in (("train", range(5)), ("validation", range(5, 10))):
        rows = [f"row-{i}" for i in values]
        frame = pd.DataFrame(
            {
                "x": list(values),
                "target": [2 * i + 3 for i in values],
                "row_id": rows,
                "source_ordinal": list(values),
                "split_role": role,
            }
        )
        content = frame.to_parquet(index=False)
        obj = store.put_bytes(
            f"automl/projects/{project}/prepared/{dataset}/profiles/p/roles/split_role={role}/part.parquet",
            content,
        )
        row_xor = 0
        for row in rows:
            row_xor ^= int.from_bytes(hashlib.sha256(row.encode()).digest(), "big")
        partitions.append(
            dict(
                uri=obj.uri,
                sha256=hashlib.sha256(content).hexdigest(),
                byte_size=len(content),
                role=role,
                rows=len(rows),
                row_digest=f"{row_xor:064x}",
            )
        )
    plan = RefitPlan(
        project_id=project,
        scope_id=scope,
        attempt_id=attempt,
        run_id=run,
        dataset_version_id=dataset,
        candidate=dict(
            uri=candidate.uri,
            sha256=hashlib.sha256(candidate_bytes).hexdigest(),
            byte_size=len(candidate_bytes),
        ),
        partitions=partitions,
        target_column="target",
        task_type="regression",
        max_rows=10,
        max_decoded_bytes=1024 * 1024,
        max_model_bytes=1024 * 1024,
    )
    return store, plan


def test_refit_uses_all_registered_train_validation_rows(refit_case, monkeypatch):
    store, plan = refit_case
    opened = []
    original = store.open_stream

    def read(uri, *args, **kwargs):
        opened.append(uri)
        return original(uri, *args, **kwargs)

    monkeypatch.setattr(store, "open_stream", read)
    result = execute_refit(plan, store)
    assert set(opened) == {plan.candidate.uri, *(p.uri for p in plan.partitions)}
    assert result["row_count"] == 10
    assert result["refit_policy_digest"] == plan.policy_digest
    payload = store.read_bytes(result["frozen_pipeline_uri"])
    assert hashlib.sha256(payload).hexdigest() == result["frozen_pipeline_digest"]
    model = joblib.load(io.BytesIO(payload))
    # StandardScaler's observed count proves validation rows participated in refit.
    assert model.named_steps["scale"].n_samples_seen_ == 10
    np.testing.assert_allclose(model.predict(pd.DataFrame({"x": [10, 11]})), [23, 25])
    # The original candidate's fitted state is unchanged.
    candidate = joblib.load(io.BytesIO(store.read_bytes(plan.candidate.uri)))
    np.testing.assert_allclose(candidate.predict(pd.DataFrame({"x": [3]})), [300])


@pytest.mark.parametrize(
    "mutation", ["wrong_bytes", "wrong_rows", "wrong_role", "cross_run", "memory_budget"]
)
def test_refit_rejects_changed_lineage_or_input_before_publication(
    refit_case, monkeypatch, mutation
):
    store, plan = refit_case
    values = plan.model_dump(mode="json")
    if mutation == "wrong_bytes":
        values["partitions"][0]["sha256"] = "f" * 64
    elif mutation == "wrong_rows":
        values["partitions"][0]["row_digest"] = "f" * 64
    elif mutation == "wrong_role":
        values["partitions"][0]["uri"] = values["partitions"][0]["uri"].replace(
            "split_role=train", "final-input"
        )
    elif mutation == "cross_run":
        values["run_id"] = str(uuid.uuid4())
    else:
        values["max_decoded_bytes"] = 1
    calls = []
    monkeypatch.setattr(store, "put_bytes", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        execute_refit(RefitPlan.model_validate(values), store)
    assert not calls


def test_stale_worker_stops_before_model_publication(refit_case, monkeypatch):
    from automl_api.services.workflow_state import StaleFence

    store, plan = refit_case
    calls = []
    monkeypatch.setattr(store, "put_bytes", lambda *args: calls.append(args))

    def progress():
        raise StaleFence("refit generation replaced")

    with pytest.raises(StaleFence):
        execute_refit(plan, store, progress=progress)
    assert not calls
