from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class StaleGenerationError(RuntimeError):
    """Raised when a superseded physical attempt tries to mutate durable state."""


class ConflictingTerminalWriteError(RuntimeError):
    """Raised when a terminal CAS is repeated with a different result."""


@dataclass
class FencedAttemptLedger:
    """Minimal Phase 0A model of the Phase 1 database CAS contract."""

    run_generation: int = 0
    trial_generations: dict[str, int] = field(default_factory=dict)
    terminal_results: dict[str, dict[str, Any]] = field(default_factory=dict)

    def replace_run_attempt(self) -> int:
        self.run_generation += 1
        return self.run_generation

    def replace_trial_attempt(self, trial_id: str) -> int:
        generation = self.trial_generations.get(trial_id, 0) + 1
        self.trial_generations[trial_id] = generation
        return generation

    def commit_trial_terminal(
        self,
        trial_id: str,
        *,
        run_generation: int,
        trial_generation: int,
        result: dict[str, Any],
    ) -> bool:
        expected = (self.run_generation, self.trial_generations.get(trial_id))
        supplied = (run_generation, trial_generation)
        if supplied != expected:
            raise StaleGenerationError(f"stale fence {supplied}; current fence is {expected}")
        existing = self.terminal_results.get(trial_id)
        if existing is not None:
            if existing != result:
                raise ConflictingTerminalWriteError(
                    f"trial {trial_id} already has a different terminal result"
                )
            return False
        self.terminal_results[trial_id] = dict(result)
        return True
