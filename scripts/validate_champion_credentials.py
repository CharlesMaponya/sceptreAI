"""Bounded synthetic-only live probe; never point this at locked release data."""

from __future__ import annotations

import argparse
import hashlib
import json
import ssl
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit

import httpx
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.final_test_credentials import FinalDataManifest


def main():
    if not __debug__:
        raise RuntimeError("Probe assertions must not be disabled")
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--ca", required=True)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--register-evaluator", action="store_true")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    assert config.get("test_environment") is True
    assert urlsplit(config["base_url"]).scheme == "https"
    manifest = FinalDataManifest.model_validate(config["manifest"])
    context = ssl.create_default_context(cafile=args.ca)
    headers = lambda name: {"Authorization": f"Bearer {config['tokens'][name]}"}  # noqa: E731
    with httpx.Client(base_url=config["base_url"], verify=context, timeout=30) as client:
        allocated = client.post(
            "/allocations",
            headers=headers("allocator"),
            json={
                "split_digest": manifest.split_digest,
                "scope_id": str(manifest.scope_id),
                "canonical_provider": manifest.provider,
                "provider_manifest_digest": manifest.digest,
            },
        )
        assert allocated.status_code == 200, allocated.status_code
        allocation_id = allocated.json()["allocation_id"]
        path = f"/allocations/{allocation_id}"
        payload = {"provider_manifest_digest": manifest.digest}
        registration_digest = None
        if args.register_evaluator:
            # Synthetic control-plane attestation: this probe does not execute refit.
            published = client.post(
                path + "/refit",
                headers=headers("allocator"),
                json={
                    "refit_attempt_id": str(uuid.uuid5(manifest.scope_id, "synthetic-refit")),
                    "frozen_pipeline_digest": config["frozen_pipeline_digest"],
                    "frozen_pipeline_uri": "s3://synthetic-models/frozen/pipeline.joblib",
                    "refit_policy_digest": hashlib.sha256(b"synthetic-broker-policy").hexdigest(),
                },
            )
            assert published.status_code == 200, published.status_code
            assert verify_receipt(SimpleNamespace(**published.json()), config["public_key"])
            attempt = str(uuid.uuid5(manifest.scope_id, "registered-evaluator"))
            registered = client.post(
                path + "/evaluators",
                headers=headers("allocator"),
                json={
                    "evaluator_attempt_id": attempt,
                    "expected_generation": 0,
                },
            )
            assert registered.status_code == 200, registered.status_code
            assert registered.headers["cache-control"] == "no-store"
            assert (
                client.post(path + "/credentials", headers=headers("aws"), json=payload).status_code
                == 403
            )
            token = registered.json()["access_token"]
            registration_digest = hashlib.sha256(token.encode()).hexdigest()
            config["tokens"]["aws"] = token
            config["evaluator_attempt_id"] = attempt
        for name in ("allocator", "gcp", "azure"):
            assert (
                client.post(path + "/credentials", headers=headers(name), json=payload).status_code
                == 403
            )
        assert client.post(path + "/credentials", json=payload).status_code == 401
        checks = ["noncanonical_and_unauthenticated_denied"]
        if args.register_evaluator:
            checks.append("durable_refit_attestation_and_dynamic_evaluator_identity")
        if args.replay:
            denied = client.post(path + "/credentials", headers=headers("aws"), json=payload)
            assert denied.status_code == 409 and "urls" not in denied.json()
            checks.append("restart_does_not_reissue_credentials")
        else:
            with ThreadPoolExecutor(max_workers=5) as pool:
                responses = list(
                    pool.map(
                        lambda _: client.post(
                            path + "/credentials",
                            headers=headers("aws"),
                            json=payload,
                        ),
                        range(5),
                    )
                )
            assert sorted(r.status_code for r in responses) == [200, 409, 409, 409, 409]
            granted = next(r for r in responses if r.status_code == 200)
            assert granted.headers["cache-control"] == "no-store"
            grant = granted.json()
            assert verify_receipt(SimpleNamespace(**grant["receipt"]), config["public_key"])
            assert (
                grant["receipt"]["payload"]["evaluator_attempt_id"]
                == config["evaluator_attempt_id"]
            )
            assert (
                grant["receipt"]["payload"]["frozen_pipeline_digest"]
                == config["frozen_pipeline_digest"]
            )
            assert grant["manifest"] == manifest.model_dump(mode="json")
            capabilities = hashlib.sha256(
                json.dumps(grant["urls"], separators=(",", ":")).encode()
            ).hexdigest()
            assert capabilities == grant["receipt"]["payload"]["capabilities_digest"]
            with httpx.Client(verify=context, timeout=30, follow_redirects=False) as objects:
                for url, obj in zip(grant["urls"], manifest.objects, strict=True):
                    response = objects.get(url)
                    assert response.status_code == 200, response.status_code
                    assert len(response.content) == obj.byte_size
                    assert hashlib.sha256(response.content).hexdigest() == obj.sha256
                    parts = urlsplit(url)
                    assert objects.get(urlunsplit(parts._replace(query=""))).status_code == 403
                    assert (
                        objects.get(
                            urlunsplit(parts._replace(path=parts.path + ".other"))
                        ).status_code
                        == 403
                    )
                    assert objects.put(url, content=b"unauthorized-write").status_code == 403
            checks.extend(
                [
                    "one_of_five_receives_grant",
                    "receipt_signature_and_binding",
                    "version_pinned_reads_and_sha256",
                    "unsigned_reads_wrong_keys_and_writes_denied",
                ]
            )
        # This synthetic result is a broker probe, not a model quality claim.
        result_digest = hashlib.sha256(("broker-probe:" + manifest.digest).encode()).hexdigest()
        committed = client.post(
            path + "/commit",
            headers=headers("aws"),
            json={
                **payload,
                "result_digest": result_digest,
            },
        )
        assert committed.status_code == 200, committed.status_code
        receipt = committed.json()
        assert verify_receipt(SimpleNamespace(**receipt), config["public_key"])
        assert receipt["payload"]["result_digest"] == result_digest
        checks.append("immutable_result_receipt")
        print(
            json.dumps(
                {
                    "status": "passed",
                    "production_qualified": False,
                    "checks": checks,
                    "allocation_id": allocation_id,
                    "receipt_digest": receipt["receipt_digest"],
                    "manifest_digest": manifest.digest,
                    "evaluator_token_digest": registration_digest,
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
