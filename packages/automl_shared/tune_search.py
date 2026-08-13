from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

from ray.tune.search import Searcher
from skopt import Optimizer
from skopt.space import Categorical, Integer, Real

CHECKPOINT_REVISION = "sceptre-skopt-tune-v1"
RESERVED_PARAMETER_PREFIX = "sceptre_"


@dataclass(frozen=True)
class SearchDimension:
    kind: Literal["categorical", "integer", "real"]
    low: int | float | None = None
    high: int | float | None = None
    prior: str = "uniform"
    categories: tuple[str | int | float | bool | None, ...] = ()

    @classmethod
    def categorical(
        cls,
        categories: Sequence[str | int | float | bool | None],
    ) -> SearchDimension:
        return cls(kind="categorical", categories=tuple(categories))

    @classmethod
    def integer(
        cls,
        low: int,
        high: int,
        *,
        prior: str = "uniform",
    ) -> SearchDimension:
        return cls(kind="integer", low=low, high=high, prior=prior)

    @classmethod
    def real(
        cls,
        low: float,
        high: float,
        *,
        prior: str = "uniform",
    ) -> SearchDimension:
        return cls(kind="real", low=low, high=high, prior=prior)

    def to_skopt(self) -> Categorical | Integer | Real:
        if self.kind == "categorical":
            if not self.categories:
                raise ValueError("A categorical dimension requires at least one value.")
            return Categorical(self.categories)
        if self.kind not in {"integer", "real"}:
            raise ValueError(f"Unsupported search dimension kind: {self.kind}")
        if self.low is None or self.high is None or self.low > self.high:
            raise ValueError(f"{self.kind} dimensions require ordered low/high bounds.")
        if self.kind == "integer":
            if not isinstance(self.low, int) or not isinstance(self.high, int):
                raise TypeError("Integer dimension bounds must be integers.")
            return Integer(self.low, self.high, prior=self.prior)
        return Real(float(self.low), float(self.high), prior=self.prior)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["categories"] = list(self.categories)
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> SearchDimension:
        allowed = {"kind", "low", "high", "prior", "categories"}
        if set(payload) - allowed:
            raise ValueError("Search dimension contains unsupported fields.")
        return cls(
            kind=str(payload.get("kind")),  # type: ignore[arg-type]
            low=payload.get("low"),
            high=payload.get("high"),
            prior=str(payload.get("prior", "uniform")),
            categories=tuple(payload.get("categories") or ()),
        )


def canonical_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class DeterministicSkoptSearch(Searcher):
    """A replayable skopt searcher with deterministic concurrency batches.

    PostgreSQL will ultimately own this append-only event stream. The JSON
    checkpoint used by the Phase 0A spike proves that replay—not a pickled
    in-memory optimizer—is sufficient to recover identical search state.
    """

    def __init__(
        self,
        dimensions: Mapping[str, SearchDimension],
        *,
        metric: str,
        mode: Literal["min", "max"],
        seed: int,
        max_concurrent: int = 1,
        max_suggestions: int | None = None,
        objective_decimal_places: int = 8,
        failure_score: float | None = None,
    ) -> None:
        if not dimensions:
            raise ValueError("At least one search dimension is required.")
        if any(name.startswith(RESERVED_PARAMETER_PREFIX) for name in dimensions):
            raise ValueError(f"Parameter names cannot start with {RESERVED_PARAMETER_PREFIX!r}.")
        if mode not in {"min", "max"}:
            raise ValueError("mode must be 'min' or 'max'.")
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be positive.")
        if max_suggestions is not None and max_suggestions < 1:
            raise ValueError("max_suggestions must be positive when supplied.")
        if objective_decimal_places < 0 or objective_decimal_places > 15:
            raise ValueError("objective_decimal_places must be between 0 and 15.")
        if failure_score is None:
            failure_score = 1e12 if mode == "min" else -1e12
        if not math.isfinite(failure_score):
            raise ValueError("failure_score must be finite.")

        super().__init__(metric=metric, mode=mode)
        self.dimensions = dict(sorted(dimensions.items()))
        self.seed = int(seed)
        self.max_concurrent = max_concurrent
        self.max_suggestions = max_suggestions
        self.objective_decimal_places = objective_decimal_places
        self.failure_score = float(failure_score)
        self.events: list[dict[str, Any]] = []
        self._suggestions: dict[str, dict[str, Any]] = {}
        self._terminal: dict[str, dict[str, Any]] = {}
        self._observed_suggestion_ids: set[int] = set()
        self._optimizer = self._new_optimizer()
        self._point_queue: list[list[Any]] = []

    def _new_optimizer(self) -> Optimizer:
        return Optimizer(
            [dimension.to_skopt() for dimension in self.dimensions.values()],
            random_state=self.seed,
            n_initial_points=min(10, self.max_suggestions or 10),
        )

    def set_search_properties(
        self,
        metric: str | None,
        mode: str | None,
        config: dict[str, Any],
        **spec: Any,
    ) -> bool:
        del metric, mode, config, spec
        return False

    def suggest(self, trial_id: str) -> dict[str, Any] | str | None:
        existing = self._suggestions.get(trial_id)
        if existing is not None:
            return dict(existing["config"])
        if self.max_suggestions is not None and len(self._suggestions) >= self.max_suggestions:
            return Searcher.FINISHED
        unobserved = len(self._suggestions) - len(self._observed_suggestion_ids)
        if unobserved >= self.max_concurrent:
            return None

        point = self._next_point()
        suggestion_id = len(self._suggestions)
        config = self._config_for(point, suggestion_id)
        event = {
            "type": "suggested",
            "trial_id": trial_id,
            "suggestion_id": suggestion_id,
            "point": point,
            "config": config,
        }
        self.events.append(event)
        self._suggestions[trial_id] = event
        return dict(config)

    def _next_point(self) -> list[Any]:
        if not self._point_queue:
            batch_size = self.max_concurrent
            if self.max_suggestions is not None:
                batch_size = min(batch_size, self.max_suggestions - len(self._suggestions))
            points = self._optimizer.ask(n_points=batch_size, strategy="cl_min")
            self._point_queue.extend([_python_scalar(value) for value in point] for point in points)
        return self._point_queue.pop(0)

    def _config_for(self, point: list[Any], suggestion_id: int) -> dict[str, Any]:
        parameters = dict(zip(self.dimensions, point, strict=True))
        trial_seed = _derived_seed(self.seed, suggestion_id)
        recipe_hash = canonical_digest(
            {"parameters": parameters, "seed": trial_seed, "suggestion_id": suggestion_id}
        )
        return {
            **parameters,
            "sceptre_suggestion_id": suggestion_id,
            "sceptre_trial_seed": trial_seed,
            "sceptre_recipe_hash": recipe_hash,
        }

    def on_trial_complete(
        self,
        trial_id: str,
        result: dict[str, Any] | None = None,
        error: bool = False,
    ) -> None:
        if error:
            self._finish_trial(trial_id, status="failed", score=self.failure_score)
        elif result is None or self.metric not in result:
            self._finish_trial(trial_id, status="cancelled", score=self.failure_score)
        else:
            self._finish_trial(trial_id, status="completed", score=result[self.metric])

    def cancel_trial(self, trial_id: str) -> None:
        self._finish_trial(trial_id, status="cancelled", score=self.failure_score)

    def _finish_trial(self, trial_id: str, *, status: str, score: Any) -> None:
        suggestion = self._suggestions.get(trial_id)
        if suggestion is None:
            raise KeyError(f"Unknown trial_id: {trial_id}")
        quantized_score = self._quantize_score(score)
        terminal = {
            "type": "result_received",
            "trial_id": trial_id,
            "suggestion_id": suggestion["suggestion_id"],
            "status": status,
            "score": quantized_score,
        }
        terminal["result_digest"] = canonical_digest(terminal)
        existing = self._terminal.get(trial_id)
        if existing is not None:
            if existing["result_digest"] != terminal["result_digest"]:
                raise ValueError(f"Conflicting terminal result for trial {trial_id}.")
            return
        self.events.append(terminal)
        self._terminal[trial_id] = terminal
        self._flush_completed_batch()

    def _quantize_score(self, score: Any) -> float:
        try:
            value = Decimal(str(score))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("The search metric must be numeric.") from exc
        if not value.is_finite():
            raise ValueError("The search metric must be finite.")
        quantum = Decimal(1).scaleb(-self.objective_decimal_places)
        return float(value.quantize(quantum, rounding=ROUND_HALF_EVEN))

    def _flush_completed_batch(self) -> None:
        if self._point_queue:
            return
        unobserved = sorted(
            (
                suggestion
                for suggestion in self._suggestions.values()
                if suggestion["suggestion_id"] not in self._observed_suggestion_ids
            ),
            key=lambda item: item["suggestion_id"],
        )
        if not unobserved or any(item["trial_id"] not in self._terminal for item in unobserved):
            return
        for suggestion in unobserved:
            terminal = self._terminal[suggestion["trial_id"]]
            objective = terminal["score"] if self.mode == "min" else -terminal["score"]
            self._optimizer.tell(suggestion["point"], objective)
            suggestion_id = suggestion["suggestion_id"]
            self._observed_suggestion_ids.add(suggestion_id)
            self.events.append(
                {
                    "type": "observed",
                    "trial_id": suggestion["trial_id"],
                    "suggestion_id": suggestion_id,
                    "objective": objective,
                    "result_digest": terminal["result_digest"],
                }
            )

    def state_dict(self) -> dict[str, Any]:
        return {
            "revision": CHECKPOINT_REVISION,
            "dimensions": {
                name: dimension.to_dict() for name, dimension in self.dimensions.items()
            },
            "metric": self.metric,
            "mode": self.mode,
            "seed": self.seed,
            "max_concurrent": self.max_concurrent,
            "max_suggestions": self.max_suggestions,
            "objective_decimal_places": self.objective_decimal_places,
            "failure_score": self.failure_score,
            "events": list(self.events),
        }

    def canonical_suggestion_log(self) -> dict[str, Any]:
        """Return provider-neutral suggestions without physical Tune trial IDs."""
        return {
            "revision": CHECKPOINT_REVISION,
            "metric": self.metric,
            "mode": self.mode,
            "seed": self.seed,
            "suggestions": [
                {
                    "suggestion_id": event["suggestion_id"],
                    "point": list(event["point"]),
                    "config": dict(event["config"]),
                }
                for event in self.events
                if event["type"] == "suggested"
            ],
        }

    def save(self, checkpoint_path: str) -> None:
        destination = Path(checkpoint_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(self.state_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def restore(self, checkpoint_path: str) -> None:
        payload = json.loads(Path(checkpoint_path).read_text(encoding="utf-8"))
        restored = self.from_state_dict(payload)
        self.__dict__.update(restored.__dict__)

    @classmethod
    def from_state_dict(cls, payload: Mapping[str, Any]) -> DeterministicSkoptSearch:
        if payload.get("revision") != CHECKPOINT_REVISION:
            raise ValueError("Unsupported search checkpoint revision.")
        dimensions_payload = payload.get("dimensions")
        if not isinstance(dimensions_payload, Mapping):
            raise ValueError("Search checkpoint dimensions are missing.")
        searcher = cls(
            {
                str(name): SearchDimension.from_dict(dimension)
                for name, dimension in dimensions_payload.items()
            },
            metric=str(payload["metric"]),
            mode=str(payload["mode"]),  # type: ignore[arg-type]
            seed=int(payload["seed"]),
            max_concurrent=int(payload["max_concurrent"]),
            max_suggestions=(
                int(payload["max_suggestions"])
                if payload.get("max_suggestions") is not None
                else None
            ),
            objective_decimal_places=int(payload["objective_decimal_places"]),
            failure_score=float(payload["failure_score"]),
        )
        events = payload.get("events")
        if not isinstance(events, list):
            raise ValueError("Search checkpoint events are missing.")
        for event in events:
            searcher._replay_event(event)
        return searcher

    def _replay_event(self, event: Mapping[str, Any]) -> None:
        event_type = event.get("type")
        trial_id = str(event.get("trial_id"))
        if event_type == "suggested":
            point = self._next_point()
            if point != event.get("point") or int(event["suggestion_id"]) != len(self._suggestions):
                raise ValueError("Search suggestion replay diverged from its checkpoint.")
            if self._config_for(point, int(event["suggestion_id"])) != event.get("config"):
                raise ValueError("Search suggestion config failed checkpoint verification.")
            copied = dict(event)
            copied["config"] = dict(event["config"])
            copied["point"] = list(event["point"])
            self._suggestions[trial_id] = copied
        elif event_type == "result_received":
            copied = dict(event)
            expected_digest = copied.pop("result_digest", None)
            if canonical_digest(copied) != expected_digest:
                raise ValueError("Search result digest does not match its checkpoint event.")
            suggestion = self._suggestions.get(trial_id)
            if suggestion is None or suggestion["suggestion_id"] != copied.get("suggestion_id"):
                raise ValueError("Search result has no matching suggestion lineage.")
            copied["result_digest"] = expected_digest
            self._terminal[trial_id] = copied
        elif event_type == "observed":
            suggestion = self._suggestions.get(trial_id)
            terminal = self._terminal.get(trial_id)
            if suggestion is None or terminal is None:
                raise ValueError("Observed search event has no suggestion/result lineage.")
            if event.get("suggestion_id") != suggestion["suggestion_id"]:
                raise ValueError("Observed search event references the wrong suggestion.")
            if event.get("result_digest") != terminal["result_digest"]:
                raise ValueError("Observed search event references the wrong result.")
            self._optimizer.tell(suggestion["point"], float(event["objective"]))
            self._observed_suggestion_ids.add(int(event["suggestion_id"]))
        else:
            raise ValueError(f"Unsupported search event type: {event_type}")
        self.events.append(dict(event))


def _derived_seed(search_seed: int, suggestion_id: int) -> int:
    payload = f"{CHECKPOINT_REVISION}\0{search_seed}\0{suggestion_id}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:4], "big")


def _python_scalar(value: Any) -> Any:
    item = getattr(value, "item", None)
    return item() if callable(item) else value
