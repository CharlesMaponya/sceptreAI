"""Register a safe provider copy using independently approved operator configuration."""

import argparse
import json
import uuid
from pathlib import Path

from automl_api.services.provider_import import OPERATION


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--project-id", type=uuid.UUID, required=True)
    parser.add_argument("--allocation-id", type=uuid.UUID, required=True)
    parser.add_argument("--provider", choices=("aws", "gcp", "azure"), required=True)
    parser.add_argument("--actor-id", type=uuid.UUID, required=True)
    parser.add_argument("--budget", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--cancel", action="store_true")
    args = parser.parse_args()
    try:
        from automl_api.db.session import get_session_factory
        from automl_api.models.datasets import DatasetVersion
        from automl_api.services.provider_copy_verification import CopyVerificationBudget
        from automl_api.services.provider_import import begin_provider_import
        from automl_api.services.provider_source import (
            _bounded_file,
            configured_policy,
            require_prepared_source,
        )

        version_id = uuid.uuid5(
            args.project_id, f"{OPERATION}:{args.allocation_id}:{args.provider}"
        )
        policy, key = configured_policy(args.project_id, version_id)
        receipt = json.loads(_bounded_file(args.receipt, 32 * 1024**2))
        budget = CopyVerificationBudget.model_validate_json(_bounded_file(args.budget, 65536))
        factory = get_session_factory()
        if args.cancel:
            from automl_api.services.provider_import import cancel_provider_import

            with factory() as db, db.begin():
                cancel_provider_import(db, args.actor_id, version_id)
            print(json.dumps({"cancelled": True, "dataset_version_id": str(version_id)}))
            return
        with factory() as db, db.begin():
            row = begin_provider_import(
                db,
                args.actor_id,
                policy=policy,
                receipt=receipt,
                public_key=key,
                budget=budget,
                dataset_name=args.name,
            )
            if row.dataset_version_id != version_id:
                raise ValueError("Provider import identity changed")
        with factory() as db:
            from automl_api.models.datasets import DatasetProviderImport

            version = db.get(DatasetVersion, version_id)
            row = db.get(DatasetProviderImport, row.id)
            if row.status == "pending":
                print(
                    json.dumps(
                        {
                            "verified": False,
                            "status": "queued",
                            "dataset_version_id": str(version_id),
                        }
                    )
                )
                return
            plan = require_prepared_source(db, version)
            if plan is None:
                raise ValueError("Provider import is not verified")
        print(
            json.dumps(
                {
                    "verified": True,
                    "dataset_version_id": str(version_id),
                    "split_revision_id": str(plan.split_revision_id),
                    "mode": plan.manifest.mode,
                }
            )
        )
    except Exception:
        parser.exit(1, "Provider dataset import failed\n")


if __name__ == "__main__":
    main()
