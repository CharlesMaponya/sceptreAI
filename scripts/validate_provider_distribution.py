"""Verify a signed distribution against trusted local references; never print object locations."""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from automl_api.services.provider_distribution import verify_distribution


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--public-key", type=Path, required=True)
    parser.add_argument("--allocation-id", type=uuid.UUID, required=True)
    parser.add_argument("--scope-id", type=uuid.UUID, required=True)
    parser.add_argument("--project-reference", required=True)
    parser.add_argument("--provider", choices=("aws", "gcp", "azure"), required=True)
    parser.add_argument("--canonical-provider", choices=("aws", "gcp", "azure"), required=True)
    parser.add_argument("--fixture-revision", required=True)
    parser.add_argument("--split-revision", required=True)
    parser.add_argument("--split-digest", required=True)
    parser.add_argument("--verify-objects", action="store_true")
    parser.add_argument("--verification-budget", type=Path)
    args = vars(parser.parse_args())
    receipt = args.pop("receipt")
    public_key = args.pop("public_key")
    objects = args.pop("verify_objects")
    budget_path = args.pop("verification_budget")
    try:
        with receipt.open("rb") as source:
            payload = source.read(32 * 1024 * 1024 + 1)
        if len(payload) > 32 * 1024 * 1024:
            raise ValueError("Provider distribution exceeds its byte budget")
        manifest = verify_distribution(json.loads(payload), public_key.read_text(), **args)
        extra = {}
        if objects:
            from automl_api.services.provider_copy_verification import (
                CopyVerificationBudget,
                verify_provider_objects,
            )
            from automl_api.storage.object_store import get_object_store

            if budget_path is None:
                raise ValueError("Object verification requires explicit resource budgets")
            with budget_path.open("rb") as source:
                budget_bytes = source.read(65537)
            if len(budget_bytes) > 65536:
                raise ValueError("Verification budget declaration is too large")
            budget = CopyVerificationBudget.model_validate_json(budget_bytes)
            proof = verify_provider_objects(manifest, get_object_store(), budget)
            extra = {
                "objects_verified": True,
                "verified_bytes": proof.byte_size,
                "verified_rows": proof.rows,
                "manifest_digest": proof.manifest_digest,
            }
        elif budget_path is not None:
            raise ValueError("A verification budget requires object verification")
    except Exception:
        # Validation exceptions may contain complete object declarations.
        parser.exit(1, "Provider distribution verification failed\n")
    print(
        json.dumps(
            {
                "verified": True,
                "revision": manifest.revision,
                "provider": manifest.provider,
                "mode": manifest.mode,
                "safe_objects": len(manifest.safe_data.objects),
                **extra,
            }
        )
    )


if __name__ == "__main__":
    main()
