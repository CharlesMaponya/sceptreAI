"""Pod-bound attempt admission; user session tokens cannot authorize this route."""

import hashlib
import json
import tempfile
import uuid
from contextlib import contextmanager
from typing import Annotated, Literal

import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from kubernetes.client import ApiException
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from starlette.concurrency import run_in_threadpool

from automl_api.db.session import get_session_factory
from automl_api.schemas.parent_control import ParentControlRequest
from automl_api.schemas.training_context import TrainingContextRequest
from automl_api.schemas.trial_control import TRIAL_CONTROL_MAX_BYTES, TrialControlRequest
from automl_api.services.attempt_identity import admit_attempt
from automl_api.services.attempt_object_grants import (
    InputGrantRequest,
    ObjectGrantUnavailable,
    issue_input_grant,
)
from automl_api.services.attempt_output_grants import (
    ModelGrantRequest,
    OutputGrantRequest,
    complete_output_grant,
    output_receipt,
    prepare_output_grant,
)
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.parent_control import control_parent
from automl_api.services.provider_copy_verification import CopyVerification
from automl_api.services.provider_import_control import control_provider_import
from automl_api.services.training_context import load_training_context
from automl_api.services.trial_control import control_trial
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.contracts import UploadCapabilityUnavailable
from automl_api.storage.object_store import get_object_store

router = APIRouter(prefix="/internal/attempts", include_in_schema=False)


@router.post("/{attempt_id}/parent/{action}")
async def parent_execution_control(
    attempt_id: uuid.UUID,
    action: Literal["context", "start", "heartbeat", "receipt", "complete", "failure"],
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > 8192:
            raise HTTPException(413, "Parent metadata exceeds its byte budget", headers=headers)
        body.extend(block)
    try:
        declaration = ParentControlRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid parent metadata", headers=headers) from exc
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable", headers=headers)
        result = await run_in_threadpool(
            control_parent, factory, attempt_id, token, k8s, action, declaration
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale parent authority", headers=headers) from exc
    except ApiException as exc:
        raise HTTPException(
            401 if exc.status in {401, 404} else 503,
            "Parent identity verification unavailable",
            headers=headers,
        ) from exc
    except (OSError, SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(503, "Parent control unavailable", headers=headers) from exc
    response.headers.update(headers)
    return result


@router.post("/{attempt_id}/training/context")
async def training_input_context(
    attempt_id: uuid.UUID,
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > 4096:
            raise HTTPException(413, "Training context exceeds its byte budget", headers=headers)
        body.extend(block)
    try:
        declaration = TrainingContextRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid training context", headers=headers) from exc
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable", headers=headers)
        result = await run_in_threadpool(
            load_training_context, factory, attempt_id, token, k8s, declaration
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(
            401, "Invalid or stale training context authority", headers=headers
        ) from exc
    except ApiException as exc:
        raise HTTPException(
            401 if exc.status in {401, 404} else 503,
            "Training identity verification unavailable",
            headers=headers,
        ) from exc
    except (OSError, SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(503, "Training context unavailable", headers=headers) from exc
    response.headers.update(headers)
    return result


@router.post("/{attempt_id}/trials/{action}")
async def trial_execution_control(
    attempt_id: uuid.UUID,
    action: Literal["enroll", "start", "heartbeat", "finish"],
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > TRIAL_CONTROL_MAX_BYTES:
            raise HTTPException(413, "Trial metadata exceeds its byte budget", headers=headers)
        body.extend(block)
    try:
        declaration = TrialControlRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid trial metadata", headers=headers) from exc
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable", headers=headers)
        result = await run_in_threadpool(
            control_trial, factory, attempt_id, token, k8s, action, declaration
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale trial authority", headers=headers) from exc
    except ApiException as exc:
        raise HTTPException(
            401 if exc.status in {401, 404} else 503,
            "Trial identity verification unavailable",
            headers=headers,
        ) from exc
    except (OSError, SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(503, "Trial control unavailable", headers=headers) from exc
    response.headers.update(headers)
    return result


class VerificationFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["invalid_copy", "verifier_unavailable"]


@router.post("/{attempt_id}/provider-import/{action}")
async def provider_verification_control(
    attempt_id: uuid.UUID,
    action: Literal["start", "heartbeat", "result", "receipt", "failure"],
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > 1024**2:
            raise HTTPException(413, "Verification metadata exceeds its byte budget")
        body.extend(block)
    try:
        payload = json.loads(body) if body else None
        proof = (
            CopyVerification.model_validate(payload) if action in {"result", "receipt"} else None
        )
        failure = VerificationFailure.model_validate(payload) if action == "failure" else None
        if action in {"start", "heartbeat"} and payload is not None:
            raise ValueError("Verification control does not accept execution arguments")
    except (ValueError, ValidationError) as exc:
        raise HTTPException(422, "Invalid verification metadata") from exc
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required.")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable")
        payload = await run_in_threadpool(
            control_provider_import,
            factory,
            attempt_id,
            token,
            k8s,
            action,
            proof=proof,
            failure_code=failure.code if failure else None,
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale verification authority") from exc
    except (OSError, SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(503, "Provider verification control unavailable") from exc
    except ApiException as exc:
        if exc.status in {401, 404}:
            raise HTTPException(401, "Invalid or stale verification authority") from exc
        raise HTTPException(503, "Workload identity verification unavailable") from exc
    response.headers["Cache-Control"] = "no-store"
    return payload


@contextmanager
def _output_identity(authorization):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise ObjectGrantUnavailable("Workload verification unavailable")
        yield token, k8s
    except (ObjectGrantUnavailable, UploadCapabilityUnavailable, OSError, SQLAlchemyError) as exc:
        raise HTTPException(
            503, "Output operation unavailable", headers={"Cache-Control": "no-store"}
        ) from exc
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(
            401, "Invalid or stale output authority", headers={"Cache-Control": "no-store"}
        ) from exc
    except ApiException as exc:
        raise HTTPException(
            401 if exc.status in {401, 404} else 503,
            "Output identity verification unavailable",
            headers={"Cache-Control": "no-store"},
        ) from exc


@router.put("/{attempt_id}/objects/{role}/{digest}")
async def write_object(
    attempt_id: uuid.UUID,
    role: Literal["pipeline", "result"],
    digest: str,
    request: Request,
    response: Response,
    request_id: Annotated[uuid.UUID, Header(alias="X-Request-ID")],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    lengths = request.headers.getlist("content-length")
    try:
        if (
            len(lengths) != 1
            or not lengths[0].isascii()
            or not lengths[0].isdecimal()
            or "transfer-encoding" in request.headers
        ):
            raise ValueError("An exact Content-Length is required")
        declaration = OutputGrantRequest(
            request_id=request_id, role=role, sha256=digest, byte_size=int(lengths[0])
        )
    except ValueError as exc:
        raise HTTPException(422, "Invalid output declaration") from exc
    return await _write_output(factory, attempt_id, authorization, declaration, request, response)


async def _write_output(factory, attempt_id, authorization, declaration, request, response):
    with _output_identity(authorization) as (token, k8s):
        receipt = await run_in_threadpool(
            prepare_output_grant,
            factory,
            attempt_id,
            token,
            k8s,
            declaration,
            store_factory=get_object_store,
        )
        if receipt["object_version"] is None:
            with tempfile.TemporaryFile() as target:
                received, actual = 0, hashlib.sha256()
                async for chunk in request.stream():
                    received += len(chunk)
                    if received > declaration.byte_size:
                        raise HTTPException(413, "Output exceeds its declared size")
                    actual.update(chunk)
                    target.write(chunk)
                if received != declaration.byte_size or actual.hexdigest() != declaration.sha256:
                    raise HTTPException(422, "Output bytes differ from their declaration")
                target.seek(0)
                receipt = await run_in_threadpool(
                    complete_output_grant,
                    factory,
                    attempt_id,
                    token,
                    k8s,
                    declaration,
                    target,
                    store_factory=get_object_store,
                )
    response.headers["Cache-Control"] = "no-store"
    return receipt


@router.get("/{attempt_id}/objects/receipts/{request_id}")
def read_output_receipt(
    attempt_id: uuid.UUID,
    request_id: uuid.UUID,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    with _output_identity(authorization) as (token, k8s):
        receipt = output_receipt(
            factory, attempt_id, token, k8s, request_id, store_factory=get_object_store
        )
    response.headers["Cache-Control"] = "no-store"
    return receipt


@router.put("/{attempt_id}/models/{run_id}/{candidate_name}/{digest}")
async def write_training_model(
    attempt_id: uuid.UUID,
    run_id: uuid.UUID,
    candidate_name: str,
    digest: str,
    request: Request,
    response: Response,
    request_id: Annotated[uuid.UUID, Header(alias="X-Request-ID")],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    lengths = request.headers.getlist("content-length")
    try:
        if (
            len(lengths) != 1
            or not lengths[0].isascii()
            or not lengths[0].isdecimal()
            or "transfer-encoding" in request.headers
        ):
            raise ValueError("An exact Content-Length is required")
        declaration = ModelGrantRequest(
            run_id=run_id,
            candidate_name=candidate_name,
            request_id=request_id,
            sha256=digest,
            byte_size=int(lengths[0]),
        )
    except ValueError as exc:
        raise HTTPException(422, "Invalid model output declaration") from exc
    return await _write_output(factory, attempt_id, authorization, declaration, request, response)


@router.get("/{attempt_id}/models/{run_id}/{candidate_name}/receipts/{request_id}")
def read_training_model_receipt(
    attempt_id: uuid.UUID,
    run_id: uuid.UUID,
    candidate_name: str,
    request_id: uuid.UUID,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    with _output_identity(authorization) as (token, k8s):
        receipt = output_receipt(
            factory,
            attempt_id,
            token,
            k8s,
            request_id,
            store_factory=get_object_store,
        )
        if receipt.get("run_id") != str(run_id) or receipt.get("candidate_name") != candidate_name:
            raise StaleFence("Model receipt belongs to another candidate")
    response.headers["Cache-Control"] = "no-store"
    return receipt


@router.post("/{attempt_id}/admission")
def admission(
    attempt_id: uuid.UUID,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required.")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable")
        payload = admit_attempt(factory, attempt_id, token, k8s)
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale workload identity") from exc
    except (OSError, SQLAlchemyError) as exc:
        raise HTTPException(503, "Workload authorization storage unavailable") from exc
    except ApiException as exc:
        if exc.status in {401, 404}:
            raise HTTPException(401, "Invalid or stale workload identity") from exc
        raise HTTPException(503, "Workload identity verification unavailable") from exc
    response.headers["Cache-Control"] = "no-store"
    return payload


@router.post("/{attempt_id}/objects/read")
def read_object_grant(
    attempt_id: uuid.UUID,
    request: InputGrantRequest,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required.")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable")
        payload = issue_input_grant(
            factory, attempt_id, token, k8s, request, store_factory=get_object_store
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale workload object authority") from exc
    except ObjectGrantUnavailable as exc:
        raise HTTPException(503, "Object capability unavailable") from exc
    except (OSError, SQLAlchemyError) as exc:
        raise HTTPException(503, "Workload authorization storage unavailable") from exc
    except ApiException as exc:
        if exc.status in {401, 404}:
            raise HTTPException(401, "Invalid or stale workload object authority") from exc
        raise HTTPException(503, "Workload identity verification unavailable") from exc
    response.headers["Cache-Control"] = "no-store"
    return payload


@router.post("/{attempt_id}/training/result")
async def publish_training_result(
    attempt_id: uuid.UUID,
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_result import TRAINING_RESULT_MAX_BYTES, TrainingResultRequest
    from automl_api.services.training_result import control_training_result

    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > TRAINING_RESULT_MAX_BYTES:
            raise HTTPException(413, "Training result exceeds its byte budget", headers=headers)
        body.extend(block)
    try:
        declaration = TrainingResultRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid training result", headers=headers) from exc
    with _output_identity(authorization) as (token, k8s):
        result = await run_in_threadpool(
            control_training_result,
            factory,
            attempt_id,
            token,
            k8s,
            declaration,
        )
    response.headers.update(headers)
    return result


@router.get("/{attempt_id}/training/result/{run_id}/receipts/{request_digest}")
def read_training_result_receipt(
    attempt_id: uuid.UUID,
    run_id: uuid.UUID,
    request_digest: str,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_result import TrainingResultReceiptRequest
    from automl_api.services.training_result import control_training_result

    headers = {"Cache-Control": "no-store"}
    try:
        declaration = TrainingResultReceiptRequest(run_id=run_id, request_digest=request_digest)
    except ValueError as exc:
        raise HTTPException(422, "Invalid training result receipt", headers=headers) from exc
    with _output_identity(authorization) as (token, k8s):
        result = control_training_result(
            factory,
            attempt_id,
            token,
            k8s,
            declaration,
            receipt_only=True,
        )
    response.headers.update(headers)
    return result


@router.post("/{attempt_id}/training/progress")
async def publish_training_progress(
    attempt_id: uuid.UUID,
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_progress import (
        TRAINING_PROGRESS_MAX_BYTES,
        TrainingProgressRequest,
    )
    from automl_api.services.training_progress import control_training_progress

    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > TRAINING_PROGRESS_MAX_BYTES:
            raise HTTPException(413, "Training progress exceeds its byte budget", headers=headers)
        body.extend(block)
    try:
        declaration = TrainingProgressRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid training progress", headers=headers) from exc
    with _output_identity(authorization) as (token, k8s):
        result = await run_in_threadpool(
            control_training_progress, factory, attempt_id, token, k8s, declaration
        )
    response.headers.update(headers)
    return result


@router.get("/{attempt_id}/training/progress/{run_id}/receipts/{request_id}")
def read_training_progress_receipt(
    attempt_id: uuid.UUID,
    run_id: uuid.UUID,
    request_id: uuid.UUID,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_progress import TrainingProgressReceiptRequest
    from automl_api.services.training_progress import control_training_progress

    with _output_identity(authorization) as (token, k8s):
        result = control_training_progress(
            factory,
            attempt_id,
            token,
            k8s,
            TrainingProgressReceiptRequest(run_id=run_id, request_id=request_id),
            receipt_only=True,
        )
    response.headers["Cache-Control"] = "no-store"
    return result


@router.post("/{attempt_id}/training/tracking")
async def ensure_training_tracking(
    attempt_id: uuid.UUID,
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_tracking import TrainingTrackingRequest
    from automl_api.services.training_tracking import control_training_tracking
    from automl_api.training.pipeline import TrackingCreationUnresolved

    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > 4096:
            raise HTTPException(413, "Tracking metadata exceeds its byte budget", headers=headers)
        body.extend(block)
    try:
        declaration = TrainingTrackingRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid tracking metadata", headers=headers) from exc
    try:
        with _output_identity(authorization) as (token, k8s):
            result = await run_in_threadpool(
                control_training_tracking, factory, attempt_id, token, k8s, declaration
            )
    except TrackingCreationUnresolved as exc:
        raise HTTPException(
            503, "Tracking creation outcome is unresolved", headers={"Cache-Control": "no-store"}
        ) from exc
    response.headers.update(headers)
    return result


@router.get("/{attempt_id}/training/tracking/{run_id}/receipts/{role}")
def read_training_tracking_receipt(
    attempt_id: uuid.UUID,
    run_id: uuid.UUID,
    role: str,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_tracking import TrainingTrackingRequest
    from automl_api.services.training_tracking import control_training_tracking
    from automl_api.training.pipeline import TrackingCreationUnresolved

    try:
        declaration = TrainingTrackingRequest(run_id=run_id, role=role)
    except ValueError as exc:
        raise HTTPException(
            422, "Invalid tracking receipt", headers={"Cache-Control": "no-store"}
        ) from exc
    try:
        with _output_identity(authorization) as (token, k8s):
            result = control_training_tracking(
                factory, attempt_id, token, k8s, declaration, receipt_only=True
            )
    except TrackingCreationUnresolved as exc:
        raise HTTPException(
            503, "Tracking creation outcome is unresolved", headers={"Cache-Control": "no-store"}
        ) from exc
    response.headers["Cache-Control"] = "no-store"
    return result


@router.post("/{attempt_id}/training/metadata")
async def write_training_metadata(
    attempt_id: uuid.UUID,
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_metadata import (
        TRAINING_METADATA_MAX_BYTES,
        TrainingMetadataRequest,
    )
    from automl_api.services.training_metadata import (
        TrackingMetadataUnresolved,
        control_training_metadata,
    )

    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > TRAINING_METADATA_MAX_BYTES:
            raise HTTPException(413, "Tracking metadata exceeds its byte budget", headers=headers)
        body.extend(block)
    try:
        declaration = TrainingMetadataRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid tracking metadata", headers=headers) from exc
    try:
        with _output_identity(authorization) as (token, k8s):
            result = await run_in_threadpool(
                control_training_metadata, factory, attempt_id, token, k8s, declaration
            )
    except TrackingMetadataUnresolved as exc:
        raise HTTPException(
            503, "Tracking metadata outcome is unresolved", headers=headers
        ) from exc
    response.headers.update(headers)
    return result


@router.get("/{attempt_id}/training/metadata/{run_id}/receipts/{role}/{request_id}")
def read_training_metadata_receipt(
    attempt_id: uuid.UUID,
    run_id: uuid.UUID,
    role: str,
    request_id: uuid.UUID,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_metadata import TrainingMetadataReceiptRequest
    from automl_api.services.training_metadata import (
        TrackingMetadataUnresolved,
        control_training_metadata,
    )

    headers = {"Cache-Control": "no-store"}
    try:
        declaration = TrainingMetadataReceiptRequest(
            run_id=run_id, role=role, request_id=request_id
        )
    except ValueError as exc:
        raise HTTPException(422, "Invalid tracking metadata receipt", headers=headers) from exc
    try:
        with _output_identity(authorization) as (token, k8s):
            result = control_training_metadata(
                factory, attempt_id, token, k8s, declaration, receipt_only=True
            )
    except TrackingMetadataUnresolved as exc:
        raise HTTPException(
            503, "Tracking metadata outcome is unresolved", headers=headers
        ) from exc
    response.headers.update(headers)
    return result


@router.post("/{attempt_id}/training/models/read")
async def read_training_model(
    attempt_id: uuid.UUID,
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    from automl_api.schemas.training_model_read import TrainingModelReadRequest
    from automl_api.services.training_model_read import issue_model_read

    headers = {"Cache-Control": "no-store"}
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > 4096:
            raise HTTPException(
                413, "Model read declaration exceeds its byte budget", headers=headers
            )
        body.extend(block)
    try:
        declaration = TrainingModelReadRequest.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(422, "Invalid model read declaration", headers=headers) from exc
    with _output_identity(authorization) as (token, k8s):
        result = await run_in_threadpool(
            issue_model_read,
            factory,
            attempt_id,
            token,
            k8s,
            declaration,
            store_factory=get_object_store,
        )
    response.headers.update(headers)
    return result
