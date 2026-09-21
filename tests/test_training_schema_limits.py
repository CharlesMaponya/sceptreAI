from __future__ import annotations

import pytest
from automl_api.schemas.training import TrainingAddModelsRequest, TrainingEstimateRequest
from pydantic import ValidationError


def test_add_models_accepts_more_than_twenty_candidates() -> None:
    """P4-W03: the historical 20-model constraint is removed."""
    models = [f"Model_{index}" for index in range(25)]
    request = TrainingAddModelsRequest(candidate_models=models)
    assert len(request.candidate_models) == 25


def test_add_models_still_requires_at_least_one_candidate() -> None:
    with pytest.raises(ValidationError):
        TrainingAddModelsRequest(candidate_models=[])


@pytest.mark.parametrize("deadline", [None, 60, 7200, 604800])
def test_training_runtime_limit_accepts_explicit_unlimited_or_valid_seconds(deadline) -> None:
    assert TrainingEstimateRequest(deadline_seconds=deadline).deadline_seconds == deadline


@pytest.mark.parametrize("deadline", [0, -1, 59, 604801])
def test_training_runtime_limit_rejects_invalid_seconds(deadline) -> None:
    with pytest.raises(ValidationError):
        TrainingEstimateRequest(deadline_seconds=deadline)
