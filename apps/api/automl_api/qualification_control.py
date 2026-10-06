"""Dedicated qualification authority; never mounted in the application API."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import Depends, FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from automl_api.db.qualification_session import get_qualification_session_factory
from automl_api.models.qualification import FinalTestAllocation, FinalTestAuthorityReceipt
from automl_api.services import final_test_authority as authority
from automl_api.services.final_evaluator_identity import (
    authenticate_evaluator,
    publish_refit,
    register_evaluator,
)
from automl_api.services.final_test_credentials import (
    FinalDataManifest,
    expire_unacknowledged_grants,
    issue_final_grant,
)
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    InvalidTransition,
    StaleFence,
    canonical_request_hash,
)

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Principal(Contract):
    token_sha256: Digest
    role: Literal["allocator", "provider"]
    project_reference: Annotated[str, Field(min_length=1, max_length=255)]
    provider: Literal["aws", "gcp", "azure"] | None = None
    scope_id: uuid.UUID | None = None
    evaluator_attempt_id: uuid.UUID | None = None
    frozen_pipeline_digest: Digest | None = None
    expires_at: datetime

    @model_validator(mode="after")
    def validate_identity(self):
        if self.expires_at.tzinfo is None:
            raise ValueError("Identity expiry must be timezone aware")
        if self.role == "provider" and (
            self.provider is None
            or self.scope_id is None
            or self.evaluator_attempt_id is None
            or self.frozen_pipeline_digest is None
        ):
            raise ValueError("Provider identity requires provider, scope, evaluator and pipeline")
        return self


class AllocationRequest(Contract):
    split_digest: Digest
    scope_id: uuid.UUID
    canonical_provider: Literal["aws", "gcp", "azure"]
    provider_manifest_digest: Digest


class OpenRequest(Contract):
    provider_manifest_digest: Digest
    expected_cas_version: Annotated[int, Field(ge=0)] = 0


class CommitRequest(OpenRequest):
    expected_cas_version: Annotated[int, Field(ge=0)] = 1
    result_digest: Digest


class FailureRequest(OpenRequest):
    reason: Annotated[str, Field(min_length=1, max_length=2000)]


class RecoveryBinding(Contract):
    evaluator_attempt_id: uuid.UUID
    frozen_pipeline_digest: Digest
    generation: Annotated[int, Field(ge=1, le=2)]


class AbortRequest(Contract):
    scope_id: uuid.UUID
    provider_manifest_digest: Digest
    reason: Annotated[str, Field(min_length=1, max_length=2000)]


class RecoveryCommit(CommitRequest, RecoveryBinding):
    pass


class RecoveryFailure(FailureRequest, RecoveryBinding):
    pass


class RefitPublication(Contract):
    refit_attempt_id: uuid.UUID
    frozen_pipeline_digest: Digest
    frozen_pipeline_uri: Annotated[str, Field(min_length=1, max_length=1024)]
    refit_policy_digest: Digest

    @model_validator(mode="after")
    def object_uri(self):
        parts = urlsplit(self.frozen_pipeline_uri)
        if parts.scheme not in {"s3", "s3c", "gs", "az", "azure"} or not parts.netloc:
            raise ValueError("Frozen pipelines require an object-store URI")
        if parts.query or parts.fragment or parts.username or parts.password:
            raise ValueError("Frozen pipeline URIs must not contain credentials or fragments")
        return self


class EvaluatorRegistration(Contract):
    evaluator_attempt_id: uuid.UUID
    expected_generation: Annotated[int, Field(ge=0, le=1)] = 0
    scope_deadline_at: datetime | None = None

    @model_validator(mode="after")
    def aware_deadline(self):
        if self.scope_deadline_at is not None and self.scope_deadline_at.tzinfo is None:
            raise ValueError("Evaluator scope deadline must be timezone aware")
        return self


def receipt_response(receipt):
    return jsonable_encoder(
        {
            name: getattr(receipt, name)
            for name in (
                "allocation_id",
                "operation",
                "provider",
                "request_digest",
                "payload",
                "receipt_digest",
                "signature_algorithm",
                "signature",
            )
        }
    )


def create_app(
    *, session_factory=None, principals=None, signing_secret=None, manifests=None, mint=None
) -> FastAPI:
    """Uvicorn factory; missing credentials/database configuration prevents startup."""
    if principals is None:
        principals = [
            Principal.model_validate(value)
            for value in json.loads(Path(os.environ["QUALIFICATION_IDENTITIES_FILE"]).read_text())
        ]
    if not principals or len({p.token_sha256 for p in principals}) != len(principals):
        raise ValueError("Authority identities must be nonempty and have unique tokens")
    if signing_secret is None:
        signing_secret = Path(os.environ["QUALIFICATION_SIGNING_SECRET_FILE"]).read_text().strip()
    if isinstance(signing_secret, str):
        signing_secret = serialization.load_pem_private_key(signing_secret.encode(), password=None)
    if not isinstance(signing_secret, Ed25519PrivateKey):
        raise ValueError("Authority service requires an Ed25519 private signing key")
    if session_factory is None:
        session_factory = get_qualification_session_factory()
    if manifests is None:
        path = os.getenv("QUALIFICATION_FINAL_MANIFESTS_FILE")
        manifests = [
            FinalDataManifest.model_validate(value)
            for value in (json.loads(Path(path).read_text()) if path else [])
        ]
    manifest_by_digest = {manifest.digest: manifest for manifest in manifests}
    if len(manifest_by_digest) != len(manifests):
        raise ValueError("Duplicate final-data manifests")

    def recover_grants():
        try:
            with session_factory() as db, db.begin():
                expire_unacknowledged_grants(db, signing_key=signing_secret)
        except Exception as exc:
            logging.getLogger(__name__).error(
                "Final grant recovery unavailable", extra={"error_type": type(exc).__name__}
            )

    @asynccontextmanager
    async def lifespan(app):
        async def reconcile():
            while True:
                await asyncio.to_thread(recover_grants)
                await asyncio.sleep(30)

        task = asyncio.create_task(reconcile())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app = FastAPI(
        title="Qualification control",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    bearer = HTTPBearer(auto_error=False)
    bearer_dependency = Depends(bearer)

    def identity(credentials: HTTPAuthorizationCredentials | None = bearer_dependency):
        digest = hashlib.sha256(
            (credentials.credentials if credentials else "").encode()
        ).hexdigest()
        for principal in principals:
            if hmac.compare_digest(digest, principal.token_sha256):
                if datetime.now(UTC) < principal.expires_at:
                    return principal
        if credentials and len(credentials.credentials) <= 8192:
            try:
                with session_factory() as db:
                    claims = authenticate_evaluator(
                        db, credentials.credentials, signing_secret.public_key()
                    )
                return Principal(
                    token_sha256=digest,
                    role="provider",
                    project_reference=claims["project_reference"],
                    provider=claims["provider"],
                    scope_id=claims["scope_id"],
                    evaluator_attempt_id=claims["evaluator_attempt_id"],
                    frozen_pipeline_digest=claims["frozen_pipeline_digest"],
                    expires_at=datetime.fromtimestamp(claims["exp"], UTC),
                )
            except (jwt.InvalidTokenError, ValueError, KeyError, TypeError):
                pass
            except SQLAlchemyError as exc:
                raise HTTPException(503, "Authority identity store unavailable") from exc
        raise HTTPException(401, "Invalid or expired authority identity")

    principal_dependency = Depends(identity)

    def register(operation, allocation_id, payload, principal):
        if principal.role != "allocator":
            raise HTTPException(403, "Publication requires a control-plane identity")
        try:
            with session_factory() as db, db.begin():
                args = dict(
                    allocation_id=allocation_id,
                    project_reference=principal.project_reference,
                    signing_key=signing_secret,
                )
                if operation is publish_refit:
                    result = receipt_response(
                        operation(db, payload=payload.model_dump(mode="json"), **args)
                    )
                else:
                    token = operation(
                        db,
                        identity_expires_at=principal.expires_at,
                        **payload.model_dump(),
                        **args,
                    )
                    result = {"access_token": token, "token_type": "Bearer"}
            return JSONResponse(result, headers={"Cache-Control": "no-store"})
        except LookupError as exc:
            raise HTTPException(404, "Allocation not found") from exc
        except authority.ProviderRejected as exc:
            raise HTTPException(403, str(exc)) from exc
        except (IdempotencyConflict, InvalidTransition) as exc:
            raise HTTPException(409, str(exc)) from exc
        except SQLAlchemyError as exc:
            raise HTTPException(503, "Authority store unavailable") from exc

    @app.post("/allocations/{allocation_id}/refit")
    def refit(
        allocation_id: uuid.UUID,
        payload: RefitPublication,
        principal: Principal = principal_dependency,
    ):
        return register(publish_refit, allocation_id, payload, principal)

    @app.post("/allocations/{allocation_id}/evaluators")
    def evaluator(
        allocation_id: uuid.UUID,
        payload: EvaluatorRegistration,
        principal: Principal = principal_dependency,
    ):
        return register(register_evaluator, allocation_id, payload, principal)

    @app.get("/healthz")
    def health():
        return {"status": "ok"}

    @app.get("/readyz")
    def ready():
        try:
            with session_factory() as db:
                db.execute(select(FinalTestAllocation.id).limit(0))
                db.execute(select(FinalTestAuthorityReceipt.id).limit(0))
            return {"status": "ready"}
        except SQLAlchemyError as exc:
            raise HTTPException(503, "Authority store unavailable") from exc

    @app.post("/allocations")
    def allocate(payload: AllocationRequest, principal: Principal = principal_dependency):
        if principal.role != "allocator":
            raise HTTPException(403, "Allocation requires a control-plane identity")
        try:
            with session_factory() as db, db.begin():
                row = authority.allocate(
                    db, project_reference=principal.project_reference, **payload.model_dump()
                )
                result = {
                    "allocation_id": str(row.id),
                    "scope_id": str(row.scope_id),
                    "status": row.status,
                    "cas_version": row.cas_version,
                }
            return result
        except IdempotencyConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except SQLAlchemyError as exc:
            raise HTTPException(503, "Authority store unavailable") from exc

    @app.get("/allocations/{allocation_id}")
    def allocation_state(allocation_id: uuid.UUID, principal: Principal = principal_dependency):
        if principal.role != "allocator":
            raise HTTPException(403, "Recovery inspection requires a control-plane identity")
        try:
            with session_factory() as db, db.begin():
                # All transitions lock the allocation before appending receipts. A shared
                # lock gives recovery one consistent snapshot without minting anything.
                row = db.scalar(
                    select(FinalTestAllocation)
                    .where(
                        FinalTestAllocation.id == allocation_id,
                        FinalTestAllocation.project_reference == principal.project_reference,
                    )
                    .with_for_update(read=True)
                    .execution_options(populate_existing=True)
                )
                if row is None:
                    raise HTTPException(404, "Allocation not found")
                receipts = db.scalars(
                    select(FinalTestAuthorityReceipt)
                    .where(
                        FinalTestAuthorityReceipt.allocation_id == row.id,
                    )
                    .order_by(FinalTestAuthorityReceipt.created_at, FinalTestAuthorityReceipt.id)
                )
                result = {
                    "allocation_id": str(row.id),
                    "scope_id": str(row.scope_id),
                    "project_reference": row.project_reference,
                    "split_digest": row.split_digest,
                    "canonical_provider": row.canonical_provider,
                    "provider_manifest_digest": row.provider_manifest_digest,
                    "status": row.status,
                    "cas_version": row.cas_version,
                    "result_digest": row.result_digest,
                    "receipts": [receipt_response(receipt) for receipt in receipts],
                }
            return JSONResponse(jsonable_encoder(result), headers={"Cache-Control": "no-store"})
        except SQLAlchemyError as exc:
            raise HTTPException(503, "Authority store unavailable") from exc

    def transition(operation, allocation_id, payload, principal, *, recovery=False):
        if principal.role != ("allocator" if recovery else "provider"):
            raise HTTPException(403, "Transition requires an authorized identity")
        try:
            with session_factory() as db, db.begin():
                row = db.scalar(
                    select(FinalTestAllocation)
                    .where(
                        FinalTestAllocation.id == allocation_id,
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if row is None or row.project_reference != principal.project_reference:
                    raise HTTPException(404, "Allocation not found")
                if not recovery and row.scope_id != principal.scope_id:
                    raise HTTPException(403, "Identity belongs to another scope")
                values = payload.model_dump(mode="json")
                if recovery:
                    registered = authority._latest_evaluator_registration(db, allocation_id)
                    if registered is None or any(
                        registered.payload.get(key) != values[key]
                        for key in ("evaluator_attempt_id", "frozen_pipeline_digest", "generation")
                    ):
                        raise StaleFence("Recovery evaluator generation is stale")
                    # Generation is an authorization fence, not part of the worker's
                    # request digest: identical recovery must replay its signed receipt.
                    values.pop("generation")
                else:
                    values.update(
                        evaluator_attempt_id=str(principal.evaluator_attempt_id),
                        frozen_pipeline_digest=principal.frozen_pipeline_digest,
                    )
                if operation is authority.commit_result and (
                    row.provider_manifest_digest in manifest_by_digest
                    or authority._receipt(db, allocation_id, "grant_claim") is not None
                ):
                    if authority._receipt(db, allocation_id, "grant_issued") is None:
                        raise InvalidTransition("Final-data credentials were not issued")
                receipt = operation(
                    db,
                    allocation_id=allocation_id,
                    provider=row.canonical_provider if recovery else principal.provider,
                    signing_secret=signing_secret,
                    request_digest=canonical_request_hash(values),
                    **values,
                )
                result = receipt_response(receipt)
            # Commit the state and receipt before returning an acknowledgement.
            return result
        except authority.ProviderRejected as exc:
            raise HTTPException(403, str(exc)) from exc
        except (IdempotencyConflict, InvalidTransition, StaleFence) as exc:
            raise HTTPException(409, str(exc)) from exc
        except SQLAlchemyError as exc:
            raise HTTPException(503, "Authority store unavailable") from exc

    @app.post("/allocations/{allocation_id}/open")
    def open_allocation(
        allocation_id: uuid.UUID, payload: OpenRequest, principal: Principal = principal_dependency
    ):
        return transition(authority.open_allocation, allocation_id, payload, principal)

    @app.post("/allocations/{allocation_id}/commit")
    def commit(
        allocation_id: uuid.UUID,
        payload: CommitRequest,
        principal: Principal = principal_dependency,
    ):
        return transition(authority.commit_result, allocation_id, payload, principal)

    @app.post("/allocations/{allocation_id}/fail")
    def fail(
        allocation_id: uuid.UUID,
        payload: FailureRequest,
        principal: Principal = principal_dependency,
    ):
        return transition(authority.fail_allocation, allocation_id, payload, principal)

    @app.post("/allocations/{allocation_id}/recovery/abort")
    def abort_registration(
        allocation_id: uuid.UUID,
        payload: AbortRequest,
        principal: Principal = principal_dependency,
    ):
        if principal.role != "allocator":
            raise HTTPException(403, "Aborting registration requires an allocator identity")
        try:
            with session_factory() as db, db.begin():
                row = db.get(FinalTestAllocation, allocation_id)
                if row is None or row.project_reference != principal.project_reference:
                    raise HTTPException(404, "Allocation not found")
                receipt = authority.abort_unregistered(
                    db,
                    allocation_id=allocation_id,
                    project_reference=principal.project_reference,
                    request_digest=canonical_request_hash(payload.model_dump(mode="json")),
                    signing_secret=signing_secret,
                    **payload.model_dump(),
                )
                result = receipt_response(receipt)
            return result
        except authority.ProviderRejected as exc:
            raise HTTPException(403, str(exc)) from exc
        except (IdempotencyConflict, InvalidTransition, StaleFence) as exc:
            raise HTTPException(409, str(exc)) from exc
        except SQLAlchemyError as exc:
            raise HTTPException(503, "Authority store unavailable") from exc

    @app.post("/allocations/{allocation_id}/recovery/commit")
    def recover_commit(
        allocation_id: uuid.UUID,
        payload: RecoveryCommit,
        principal: Principal = principal_dependency,
    ):
        return transition(authority.commit_result, allocation_id, payload, principal, recovery=True)

    @app.post("/allocations/{allocation_id}/recovery/fail")
    def recover_failure(
        allocation_id: uuid.UUID,
        payload: RecoveryFailure,
        principal: Principal = principal_dependency,
    ):
        return transition(
            authority.fail_allocation, allocation_id, payload, principal, recovery=True
        )

    @app.post("/allocations/{allocation_id}/credentials")
    def credentials(
        allocation_id: uuid.UUID, payload: OpenRequest, principal: Principal = principal_dependency
    ):
        if principal.role != "provider":
            raise HTTPException(403, "Credentials require a scoped evaluator identity")
        manifest = manifest_by_digest.get(payload.provider_manifest_digest)
        if manifest is None:
            raise HTTPException(403, "Final-data manifest is not registered")
        try:
            result = issue_final_grant(
                session_factory,
                allocation_id=allocation_id,
                principal=principal,
                manifest=manifest,
                signing_key=signing_secret,
                **({"mint": mint} if mint is not None else {}),
            )
            return JSONResponse(jsonable_encoder(result), headers={"Cache-Control": "no-store"})
        except LookupError as exc:
            raise HTTPException(404, "Allocation not found") from exc
        except authority.ProviderRejected as exc:
            raise HTTPException(403, str(exc)) from exc
        except (InvalidTransition, IdempotencyConflict, StaleFence) as exc:
            raise HTTPException(409, str(exc)) from exc
        except Exception as exc:
            # Provider exceptions may contain credentials. Never echo them.
            raise HTTPException(503, "Final-data credential issuance unavailable") from exc

    return app
