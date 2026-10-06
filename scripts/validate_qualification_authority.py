"""Bounded HTTPS lifecycle probe for an isolated qualification test deployment."""

from __future__ import annotations

import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
from automl_api.services.final_test_authority import verify_receipt


def validate(config: dict, ca: Path) -> dict:
    # Only preregistered synthetic allocations may be used by this probe.
    if config.get("test_environment") is not True:
        raise ValueError("Probe configuration must explicitly declare test_environment=true")
    if not config["base_url"].startswith("https://") or not config.get("public_key"):
        raise ValueError("Probe requires HTTPS and a pinned authority public key")
    if not __debug__:
        raise RuntimeError("Run this assertion-based probe without Python optimization")
    with httpx.Client(base_url=config["base_url"], verify=str(ca), timeout=15) as client:

        def post(path, body, caller):
            return client.post(
                path,
                json=body,
                headers={
                    "Authorization": f"Bearer {config['tokens'][caller]}",
                },
            )

        assert client.get("/readyz").status_code == 200
        payload = {
            "scope_id": config["scope"],
            "canonical_provider": "aws",
            "split_digest": hashlib.sha256(config["scope"].encode()).hexdigest(),
            "provider_manifest_digest": "a" * 64,
        }
        assert client.post("/allocations", json=payload).status_code == 401
        assert post("/allocations", payload, "aws").status_code == 403
        allocation = post("/allocations", payload, "allocator")
        assert allocation.status_code == 200, allocation.status_code
        allocation_id = allocation.json()["allocation_id"]
        assert post("/allocations", payload, "allocator").json() == allocation.json()
        changed = {**payload, "provider_manifest_digest": "f" * 64}
        assert post("/allocations", changed, "allocator").status_code == 409
        path = f"/allocations/{allocation_id}"
        body = {"provider_manifest_digest": "a" * 64}
        with ThreadPoolExecutor(max_workers=3) as pool:
            opened = list(
                pool.map(
                    lambda provider: post(path + "/open", body, provider), ("aws", "gcp", "azure")
                )
            )
        statuses = [r.status_code for r in opened]
        assert statuses == [200, 403, 403], f"Provider open statuses: {statuses}"
        receipt = opened[0].json()
        assert receipt["payload"]["evaluator_attempt_id"] == config["evaluator_attempt_id"]
        assert receipt["payload"]["frozen_pipeline_digest"] == config["frozen_pipeline_digest"]
        for caller in ("alternate_attempt", "alternate_pipeline"):
            for operation, extra in (
                ("open", {}),
                ("commit", {"result_digest": "b" * 64}),
                ("fail", {"reason": "different evaluator"}),
            ):
                assert post(path + "/" + operation, {**body, **extra}, caller).status_code == 403
        assert post(path + "/open", body, "aws").json() == receipt
        result_body = {**body, "result_digest": "b" * 64}
        with ThreadPoolExecutor(max_workers=3) as pool:
            committed = list(
                pool.map(lambda _: post(path + "/commit", result_body, "aws"), range(3))
            )
        assert all(r.status_code == 200 for r in committed)
        assert all(r.json() == committed[0].json() for r in committed)
        assert post(path + "/commit", {**body, "result_digest": "c" * 64}, "aws").status_code == 409
        assert (
            post(
                path + "/fail", {**body, "reason": "late failure", "expected_cas_version": 2}, "aws"
            ).status_code
            == 409
        )
        receipts = [receipt, committed[0].json()]
        assert all(r["signature_algorithm"] == "ed25519" for r in receipts)
        assert all(verify_receipt(SimpleNamespace(**r), config["public_key"]) for r in receipts)
        return {
            "status": "passed",
            "production_qualified": False,
            "allocation_id": allocation_id,
            "receipt_fingerprint": hashlib.sha256(
                json.dumps(receipts, sort_keys=True).encode()
            ).hexdigest(),
            "checks": [
                "verified_https",
                "public_key_receipts",
                "evaluator_and_pipeline_binding",
                "readiness",
                "identity_rejection",
                "allocation_replay_and_conflict",
                "three_provider_open_race",
                "concurrent_commit_replay",
                "terminal_conflict",
            ],
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--ca", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(validate(json.loads(args.config.read_text()), args.ca), sort_keys=True))


if __name__ == "__main__":
    main()
