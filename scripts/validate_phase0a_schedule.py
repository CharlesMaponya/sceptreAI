from __future__ import annotations

import json
import math
import random
from dataclasses import asdict, dataclass

RUNS = 15
DEADLINE_SECONDS = 7_200
MINIMUM_QUOTA_HEADROOM = 0.20
CAPACITY_RESERVE_FRACTION = 0.25
SEED = 42


@dataclass(frozen=True)
class ResourceVector:
    cpu: float
    memory_gib: float
    ephemeral_gib: float
    gpu: float = 0


@dataclass(frozen=True)
class ProviderProfile:
    provider: str
    node: ResourceVector
    node_count: int
    quota_nodes: int
    hourly_node_usd: float
    object_storage_gib_usd: float
    request_million_usd: float
    network_gib_usd: float
    telemetry_gib_usd: float


HEAD = ResourceVector(cpu=0.25, memory_gib=0.5, ephemeral_gib=1.0)
WORKER = ResourceVector(cpu=4.0, memory_gib=12.0, ephemeral_gib=40.0)
PROFILES = (
    ProviderProfile(
        "aws-eks",
        ResourceVector(16, 64, 150),
        node_count=10,
        quota_nodes=12,
        hourly_node_usd=0.768,
        object_storage_gib_usd=0.023,
        request_million_usd=5.0,
        network_gib_usd=0.09,
        telemetry_gib_usd=0.50,
    ),
    ProviderProfile(
        "gcp-gke",
        ResourceVector(16, 64, 150),
        node_count=10,
        quota_nodes=12,
        hourly_node_usd=0.760,
        object_storage_gib_usd=0.020,
        request_million_usd=5.0,
        network_gib_usd=0.12,
        telemetry_gib_usd=0.50,
    ),
    ProviderProfile(
        "azure-aks",
        ResourceVector(16, 64, 150),
        node_count=10,
        quota_nodes=12,
        hourly_node_usd=0.832,
        object_storage_gib_usd=0.018,
        request_million_usd=5.0,
        network_gib_usd=0.087,
        telemetry_gib_usd=0.50,
    ),
)


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = math.ceil(quantile * len(ordered)) - 1
    return ordered[max(0, index)]


def schedule(profile: ProviderProfile) -> dict[str, object]:
    randomizer = random.Random(f"{SEED}:{profile.provider}")
    startup = [max(12.0, randomizer.lognormvariate(3.45, 0.22)) for _ in range(RUNS)]
    fit = [max(2_900.0, randomizer.lognormvariate(8.18, 0.14)) for _ in range(RUNS)]
    recovery = [104.0 if index in {2, 9} else 0.0 for index in range(RUNS)]
    finalization = [240.0 for _ in range(RUNS)]
    walls = [sum(parts) for parts in zip(startup, fit, recovery, finalization, strict=True)]

    # Conservative colocated head/worker pairs, with 25% node resources reserved.
    # ponytail: static packing only; measured stage/gang scheduling remains required.
    per_node_workers = max(
        0,
        min(
            math.floor(
                getattr(profile.node, resource)
                * (1 - CAPACITY_RESERVE_FRACTION)
                / (getattr(HEAD, resource) + getattr(WORKER, resource))
            )
            for resource in ("cpu", "memory_gib", "ephemeral_gib", "gpu")
            if getattr(HEAD, resource) + getattr(WORKER, resource) > 0
        ),
    )
    available_nodes = max(0, min(profile.node_count, profile.quota_nodes))
    slots = per_node_workers * available_nodes
    required_nodes = math.ceil(RUNS / per_node_workers) if per_node_workers else None
    quota_headroom = (
        (profile.quota_nodes - required_nodes) / required_nodes if required_nodes else None
    )
    unschedulable = max(0, RUNS - slots)
    return {
        "provider": profile.provider,
        "node_count": profile.node_count,
        "quota_nodes": profile.quota_nodes,
        "required_nodes": required_nodes,
        "quota_headroom_fraction": quota_headroom,
        "slots": slots,
        "available_nodes": available_nodes,
        "unschedulable_placements": unschedulable,
        "startup_seconds": {
            "p50": percentile(startup, 0.50),
            "p95": percentile(startup, 0.95),
            "p99": percentile(startup, 0.99),
        },
        "fit_seconds": {
            "p50": percentile(fit, 0.50),
            "p95": percentile(fit, 0.95),
            "p99": percentile(fit, 0.99),
        },
        "recovery_seconds": {
            "p50": percentile(recovery, 0.50),
            "p95": percentile(recovery, 0.95),
            "p99": percentile(recovery, 0.99),
        },
        "wall_seconds": {
            "p50": percentile(walls, 0.50),
            "p95": percentile(walls, 0.95),
            "p99": percentile(walls, 0.99),
            "maximum": max(walls),
        },
    }


def cost(profile: ProviderProfile, *, multiplier: float) -> float:
    compute_hours = profile.node_count * 2 * multiplier
    storage_gib_month = 750 * multiplier
    requests_million = 0.6 * multiplier
    network_gib = 120 * multiplier
    telemetry_gib = 20 * multiplier
    return round(
        compute_hours * profile.hourly_node_usd
        + storage_gib_month * profile.object_storage_gib_usd
        + requests_million * profile.request_million_usd
        + network_gib * profile.network_gib_usd
        + telemetry_gib * profile.telemetry_gib_usd,
        2,
    )


def main() -> None:
    schedules = [schedule(profile) for profile in PROFILES]
    assumptions_pass = all(
        result["unschedulable_placements"] == 0
        and result["quota_headroom_fraction"] is not None
        and result["quota_headroom_fraction"] >= MINIMUM_QUOTA_HEADROOM
        and result["wall_seconds"]["maximum"] <= DEADLINE_SECONDS
        for result in schedules
    )

    envelope = {
        profile.provider: {
            "baseline_usd": cost(profile, multiplier=1.0),
            "scenario_1_35x_usd": cost(profile, multiplier=1.35),
            "scenario_2x_usd": cost(profile, multiplier=2.0),
            "inputs": asdict(profile),
        }
        for profile in PROFILES
    }
    print(
        json.dumps(
            {
                "cost_envelope": envelope,
                "deadline_seconds": DEADLINE_SECONDS,
                "minimum_quota_headroom_fraction": MINIMUM_QUOTA_HEADROOM,
                "capacity_reserve_fraction": CAPACITY_RESERVE_FRACTION,
                "runs": RUNS,
                "schedules": schedules,
                "seed": SEED,
                "status": "synthetic_estimate",
                "qualified": False,
                "synthetic_constraints_pass": assumptions_pass,
                "limitations": [
                    "Timings are seeded synthetic samples, not measured benchmarks.",
                    "Static colocated packing omits stage dependencies and autoscaling.",
                    "Prices are unverified assumptions; multipliers are not cost quantiles.",
                    "Measured runtime, recovery, provider and signed budget gates remain open.",
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
