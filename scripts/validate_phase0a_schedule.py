from __future__ import annotations

import json
import math
import random
from dataclasses import asdict, dataclass

RUNS = 15
DEADLINE_SECONDS = 7_200
HEADROOM_FACTOR = 1.20
WARM_CAPACITY_FRACTION = 0.25
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

    per_node_workers = min(
        math.floor((profile.node.cpu * 0.75 - HEAD.cpu) / WORKER.cpu),
        math.floor((profile.node.memory_gib * 0.75 - HEAD.memory_gib) / WORKER.memory_gib),
        math.floor(
            (profile.node.ephemeral_gib * 0.75 - HEAD.ephemeral_gib) / WORKER.ephemeral_gib
        ),
    )
    slots = per_node_workers * profile.node_count
    warm_slots = math.floor(slots * (1 - WARM_CAPACITY_FRACTION))
    required_nodes = math.ceil(RUNS / max(1, per_node_workers))
    quota_headroom = (profile.quota_nodes - required_nodes) / required_nodes
    unschedulable = 0 if warm_slots >= RUNS else RUNS - warm_slots
    return {
        "provider": profile.provider,
        "node_count": profile.node_count,
        "quota_nodes": profile.quota_nodes,
        "required_nodes": required_nodes,
        "quota_headroom_fraction": quota_headroom,
        "slots": slots,
        "warm_slots": warm_slots,
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
    for result in schedules:
        if result["unschedulable_placements"] != 0:
            raise RuntimeError(f"{result['provider']} has unschedulable placements")
        if float(result["quota_headroom_fraction"]) < 0.20:
            raise RuntimeError(f"{result['provider']} has less than 20% quota headroom")
        wall = result["wall_seconds"]
        if not isinstance(wall, dict) or float(wall["maximum"]) > DEADLINE_SECONDS:
            raise RuntimeError(f"{result['provider']} misses the 7200-second deadline")

    envelope = {
        profile.provider: {
            "expected_usd": cost(profile, multiplier=1.0),
            "p90_usd": cost(profile, multiplier=1.35),
            "worst_case_usd": cost(profile, multiplier=2.0),
            "inputs": asdict(profile),
        }
        for profile in PROFILES
    }
    print(
        json.dumps(
            {
                "cost_envelope": envelope,
                "deadline_seconds": DEADLINE_SECONDS,
                "headroom_factor": HEADROOM_FACTOR,
                "runs": RUNS,
                "schedules": schedules,
                "seed": SEED,
                "status": "passed",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
