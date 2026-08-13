from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from automl_shared.evaluation_authority import (
    DelegatedCredential,
    EvaluationAuthorityError,
    EvaluationScope,
)

NOW = datetime(2026, 8, 13, tzinfo=UTC)


def credential(
    attempt_id: str,
    *,
    final: bool = False,
    expires: datetime | None = None,
    predecessors: frozenset[str] = frozenset(),
) -> DelegatedCredential:
    return DelegatedCredential(
        project_id="project-a",
        attempt_id=attempt_id,
        stage="evaluation" if final else "refit",
        expires_at=expires or NOW + timedelta(minutes=15),
        allowed_checkpoint_attempt_ids=predecessors,
        release_final_authority=final,
    )


def test_refit_retry_and_predecessor_allowlist_are_bounded() -> None:
    scope = EvaluationScope("project-a", promotional=True)
    scope.register_refit_attempt("refit-1")
    scope.register_refit_attempt("refit-2")
    with pytest.raises(EvaluationAuthorityError, match="budget"):
        scope.register_refit_attempt("refit-3")

    current = credential("refit-2", predecessors=frozenset({"refit-1"}))
    scope.read_predecessor_checkpoint(current, checkpoint_attempt_id="refit-1", now=NOW)
    with pytest.raises(EvaluationAuthorityError, match="allowlisted"):
        scope.read_predecessor_checkpoint(current, checkpoint_attempt_id="other", now=NOW)


def test_evaluator_retry_is_preopen_only_and_result_is_one_shot() -> None:
    scope = EvaluationScope("project-a", promotional=True)
    scope.fail_before_open("evaluation-1")
    active = credential("evaluation-2", final=True)
    scope.open_test(active, now=NOW)
    assert scope.commit_final_result(attempt_id="evaluation-2", digest="digest-1")
    assert not scope.commit_final_result(attempt_id="evaluation-2", digest="digest-1")
    with pytest.raises(EvaluationAuthorityError, match="another digest"):
        scope.commit_final_result(attempt_id="evaluation-2", digest="digest-2")
    with pytest.raises(EvaluationAuthorityError, match="already opened"):
        scope.open_test(credential("evaluation-3", final=True), now=NOW)
    with pytest.raises(EvaluationAuthorityError, match="refit cannot reopen"):
        scope.register_refit_attempt("refit-after-open")
    with pytest.raises(EvaluationAuthorityError, match="post-open"):
        scope.fail_before_open("evaluation-after-open")


def test_attempt_budgets_and_terminal_identity_are_enforced() -> None:
    scope = EvaluationScope(
        "project-a", promotional=True, max_preopen_evaluator_attempts=1
    )
    scope.fail_before_open("evaluation-1")
    with pytest.raises(EvaluationAuthorityError, match="budget exhausted"):
        scope.fail_before_open("evaluation-2")
    with pytest.raises(EvaluationAuthorityError, match="budget exhausted"):
        scope.open_test(credential("evaluation-2", final=True), now=NOW)

    unopened = EvaluationScope("project-a", promotional=True)
    with pytest.raises(EvaluationAuthorityError, match="must be opened"):
        unopened.commit_final_result(attempt_id="evaluation-1", digest="digest")
    unopened.open_test(credential("evaluation-1", final=True), now=NOW)
    with pytest.raises(EvaluationAuthorityError, match="active evaluator"):
        unopened.commit_final_result(attempt_id="evaluation-2", digest="digest")


def test_identity_expiry_cross_attempt_and_provider_conformance_deny_final() -> None:
    scope = EvaluationScope("project-a", promotional=True)
    with pytest.raises(EvaluationAuthorityError, match="expired"):
        scope.open_test(
            credential("evaluation-1", final=True, expires=NOW - timedelta(seconds=1)),
            now=NOW,
        )
    wrong_attempt = credential("evaluation-1", final=True)
    with pytest.raises(EvaluationAuthorityError, match="another project or attempt"):
        wrong_attempt.validate(now=NOW, project_id="project-a", attempt_id="evaluation-2")

    non_promotional = EvaluationScope("project-a", promotional=False)
    conformance = DelegatedCredential(
        project_id="project-a",
        attempt_id="conformance-1",
        stage="provider_conformance",
        expires_at=NOW + timedelta(minutes=15),
        release_final_authority=False,
    )
    with pytest.raises(EvaluationAuthorityError, match="no release-final"):
        non_promotional.open_test(conformance, now=NOW)
    with pytest.raises(EvaluationAuthorityError, match="realistic timezone-aware"):
        conformance.validate(
            now=datetime(1999, 1, 1), project_id="project-a", attempt_id="conformance-1"
        )
