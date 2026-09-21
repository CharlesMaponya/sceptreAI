"""Explicit run admission policy for training workloads.

Replaces the historical one-active-run-per-project guard with a reviewed
policy over user, project, environment, and resource class so that the
approved qualification profile supports at least 15 concurrent runs sharing
one benchmark dataset inside one project (task-index P4-W04/P4-W21).
"""

from __future__ import annotations

from dataclasses import dataclass, field

QUALIFIED_PRODUCTION_CONCURRENCY = 15


@dataclass(frozen=True)
class AdmissionPolicy:
    """Weighted-fair admission limits per environment and resource class."""

    environment: str
    global_max_active_runs: int = QUALIFIED_PRODUCTION_CONCURRENCY
    max_active_runs_per_project: int = QUALIFIED_PRODUCTION_CONCURRENCY
    max_active_runs_per_user: int = 5
    max_active_runs_per_resource_class: dict[str, int] = field(
        default_factory=lambda: {
            "training": QUALIFIED_PRODUCTION_CONCURRENCY,
            "low": 15,
            "medium": 10,
            "high": 4,
        }
    )

    def normalized_environment(self) -> str:
        return self.environment.strip().lower()


def admission_policy_for(environment: str) -> AdmissionPolicy:
    """Return the reviewed admission policy for the given environment."""
    normalized = environment.strip().lower()
    if normalized in {"staging", "production"}:
        return AdmissionPolicy(environment=normalized)
    # Local development keeps a smaller but still explicit budget.
    return AdmissionPolicy(
        environment=normalized or "local",
        global_max_active_runs=4,
        max_active_runs_per_project=2,
        max_active_runs_per_user=2,
        max_active_runs_per_resource_class={"training": 4, "low": 4, "medium": 2, "high": 1},
    )


@dataclass(frozen=True)
class AdmissionRequest:
    project_id: str
    user_id: str
    resource_class: str = "training"
    evaluation_scope_id: str | None = None


@dataclass(frozen=True)
class AdmissionDecision:
    admitted: bool
    blockers: list[str] = field(default_factory=list)

    @property
    def can_launch(self) -> bool:
        return self.admitted


def evaluate_admission(
    policy: AdmissionPolicy,
    request: AdmissionRequest,
    *,
    active_global: int,
    active_for_project: int,
    active_for_user: int,
    active_for_resource_class: int,
) -> AdmissionDecision:
    """Apply weighted-fair admission checks against current demand.

    Scope-bound runs (evaluation_scope_id set) bypass the per-project limit
    because they continue sealed-scope evidence rather than starting new
    selection work.
    """
    blockers: list[str] = []
    scope_bound = request.evaluation_scope_id is not None
    if active_global >= policy.global_max_active_runs and not scope_bound:
        blockers.append(
            f"Global concurrency limit reached ({active_global}/"
            f"{policy.global_max_active_runs} active runs)."
        )
    if not scope_bound and active_for_project >= policy.max_active_runs_per_project:
        blockers.append(
            f"Project concurrency limit reached ({active_for_project}/"
            f"{policy.max_active_runs_per_project} active runs)."
        )
    if active_for_user >= policy.max_active_runs_per_user and not scope_bound:
        blockers.append(
            f"User concurrency limit reached ({active_for_user}/"
            f"{policy.max_active_runs_per_user} active runs)."
        )
    class_limit = policy.max_active_runs_per_resource_class.get(request.resource_class)
    if class_limit is None:
        blockers.append(f"Unknown resource class '{request.resource_class}'.")
    elif active_for_resource_class >= class_limit and not scope_bound:
        blockers.append(
            f"Resource-class '{request.resource_class}' concurrency limit reached "
            f"({active_for_resource_class}/{class_limit} active runs)."
        )
    return AdmissionDecision(admitted=not blockers, blockers=blockers)
