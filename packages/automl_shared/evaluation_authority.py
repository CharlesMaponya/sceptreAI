from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal


class EvaluationAuthorityError(RuntimeError):
    """Raised when a workflow identity exceeds its scoped evaluation authority."""


@dataclass(frozen=True)
class DelegatedCredential:
    project_id: str
    attempt_id: str
    stage: Literal["refit", "evaluation", "provider_conformance"]
    expires_at: datetime
    allowed_checkpoint_attempt_ids: frozenset[str] = frozenset()
    release_final_authority: bool = False

    def validate(self, *, now: datetime, project_id: str, attempt_id: str) -> None:
        if now.tzinfo is None or now <= datetime.now(UTC).replace(year=2000):
            raise EvaluationAuthorityError("now must be a realistic timezone-aware timestamp")
        if now >= self.expires_at:
            raise EvaluationAuthorityError("delegated credential expired")
        if project_id != self.project_id or attempt_id != self.attempt_id:
            raise EvaluationAuthorityError("credential is bound to another project or attempt")


@dataclass
class EvaluationScope:
    project_id: str
    promotional: bool
    max_refit_attempts: int = 2
    max_preopen_evaluator_attempts: int = 2
    refit_attempts: list[str] = field(default_factory=list)
    evaluator_attempts: list[str] = field(default_factory=list)
    test_opened_at: datetime | None = None
    final_result_digest: str | None = None

    def register_refit_attempt(self, attempt_id: str) -> None:
        if self.test_opened_at is not None:
            raise EvaluationAuthorityError("refit cannot reopen after final test access")
        if len(self.refit_attempts) >= self.max_refit_attempts:
            raise EvaluationAuthorityError("refit attempt budget exhausted")
        self.refit_attempts.append(attempt_id)

    def read_predecessor_checkpoint(
        self,
        credential: DelegatedCredential,
        *,
        checkpoint_attempt_id: str,
        now: datetime,
    ) -> None:
        credential.validate(
            now=now,
            project_id=self.project_id,
            attempt_id=credential.attempt_id,
        )
        if checkpoint_attempt_id not in credential.allowed_checkpoint_attempt_ids:
            raise EvaluationAuthorityError("checkpoint predecessor is not allowlisted")

    def open_test(
        self,
        credential: DelegatedCredential,
        *,
        now: datetime,
    ) -> None:
        credential.validate(
            now=now,
            project_id=self.project_id,
            attempt_id=credential.attempt_id,
        )
        if not self.promotional or not credential.release_final_authority:
            raise EvaluationAuthorityError("identity has no release-final authority")
        if self.test_opened_at is not None:
            raise EvaluationAuthorityError("the final test was already opened")
        if len(self.evaluator_attempts) >= self.max_preopen_evaluator_attempts:
            raise EvaluationAuthorityError("pre-open evaluator attempt budget exhausted")
        self.evaluator_attempts.append(credential.attempt_id)
        self.test_opened_at = now

    def fail_before_open(self, attempt_id: str) -> None:
        if self.test_opened_at is not None:
            raise EvaluationAuthorityError("post-open failure cannot be retried")
        if len(self.evaluator_attempts) >= self.max_preopen_evaluator_attempts:
            raise EvaluationAuthorityError("pre-open evaluator attempt budget exhausted")
        self.evaluator_attempts.append(attempt_id)

    def commit_final_result(self, *, attempt_id: str, digest: str) -> bool:
        if self.test_opened_at is None or not self.evaluator_attempts:
            raise EvaluationAuthorityError("test must be opened before committing a result")
        if self.evaluator_attempts[-1] != attempt_id:
            raise EvaluationAuthorityError("only the active evaluator attempt may commit")
        if self.final_result_digest is not None:
            if self.final_result_digest != digest:
                raise EvaluationAuthorityError("final result already committed with another digest")
            return False
        self.final_result_digest = digest
        return True
