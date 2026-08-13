from __future__ import annotations

import pytest
from automl_shared.fencing import (
    ConflictingTerminalWriteError,
    FencedAttemptLedger,
    StaleGenerationError,
)


def test_run_and_trial_replacement_reject_stale_writers() -> None:
    ledger = FencedAttemptLedger()
    first_run = ledger.replace_run_attempt()
    first_trial = ledger.replace_trial_attempt("trial-1")
    second_trial = ledger.replace_trial_attempt("trial-1")

    with pytest.raises(StaleGenerationError):
        ledger.commit_trial_terminal(
            "trial-1",
            run_generation=first_run,
            trial_generation=first_trial,
            result={"status": "succeeded"},
        )

    second_run = ledger.replace_run_attempt()
    with pytest.raises(StaleGenerationError):
        ledger.commit_trial_terminal(
            "trial-1",
            run_generation=first_run,
            trial_generation=second_trial,
            result={"status": "succeeded"},
        )

    current_trial = ledger.replace_trial_attempt("trial-1")
    assert ledger.commit_trial_terminal(
        "trial-1",
        run_generation=second_run,
        trial_generation=current_trial,
        result={"status": "succeeded", "digest": "abc"},
    )


def test_terminal_compare_and_set_is_idempotent_and_conflict_safe() -> None:
    ledger = FencedAttemptLedger()
    run = ledger.replace_run_attempt()
    trial = ledger.replace_trial_attempt("trial-1")
    result = {"status": "succeeded", "digest": "abc"}

    assert ledger.commit_trial_terminal(
        "trial-1", run_generation=run, trial_generation=trial, result=result
    )
    assert not ledger.commit_trial_terminal(
        "trial-1", run_generation=run, trial_generation=trial, result=result
    )
    with pytest.raises(ConflictingTerminalWriteError):
        ledger.commit_trial_terminal(
            "trial-1",
            run_generation=run,
            trial_generation=trial,
            result={"status": "failed", "digest": "different"},
        )
