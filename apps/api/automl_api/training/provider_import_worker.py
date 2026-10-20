"""Verify approved copies using pod identity and brokered reads only."""

import os
import sys
import time
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict

from automl_api.services.provider_copy_verification import (
    CopyVerificationBudget,
    verify_provider_objects,
)
from automl_api.services.provider_distribution import ProviderDistribution
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC
from automl_api.training.object_capabilities import ProviderGrantReader


class VerificationWork(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    protocol: Literal["provider-import-verifier-v1"]
    attempt_id: uuid.UUID
    project_id: uuid.UUID
    dataset_version_id: uuid.UUID
    fencing_token: str
    manifest: ProviderDistribution
    budget: CopyVerificationBudget


def run_verification():
    attempt_id = uuid.UUID(os.environ["AUTOML_ATTEMPT_ID"])
    rpc = AttemptRPC(
        attempt_id,
        base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
        token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
        ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
    )
    work = VerificationWork.model_validate(
        rpc.request("POST", "provider-import/start", max_bytes=32 * 1024**2)
    )
    if (
        work.attempt_id != attempt_id
        or work.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
        or work.fencing_token != os.environ["AUTOML_FENCING_TOKEN"]
    ):
        raise ValueError("Provider verification control returned another execution")
    reader = ProviderGrantReader(
        work.manifest,
        attempt_id,
        base_url=rpc.base_url,
        token_file=rpc.token_file,
        ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
    )
    renewed = time.monotonic()

    def progress():
        nonlocal renewed
        if time.monotonic() - renewed >= 15:
            rpc.request("POST", "provider-import/heartbeat", max_bytes=65536)
            renewed = time.monotonic()

    try:
        proof = verify_provider_objects(work.manifest, reader, work.budget, progress=progress)
        payload = proof.model_dump(mode="json")
        try:
            result = rpc.request("POST", "provider-import/result", payload=payload, max_bytes=65536)
        except AttemptControlUnavailable:
            result = rpc.request(
                "POST", "provider-import/receipt", payload=payload, max_bytes=65536
            )
        if uuid.UUID(result["dataset_version_id"]) != work.dataset_version_id:
            raise ValueError("Provider verification receipt belongs to another dataset")
        return result
    except Exception as error:
        try:
            rpc.request(
                "POST",
                "provider-import/failure",
                payload={
                    "code": "invalid_copy"
                    if isinstance(error, ValueError)
                    else "verifier_unavailable"
                },
                max_bytes=65536,
            )
        except Exception:
            pass
        raise


def main():
    try:
        run_verification()
    except Exception:
        print("Provider verification failed", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
