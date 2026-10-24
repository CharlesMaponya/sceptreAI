"""Metadata-only parent control using freshly read projected Pod identity."""

import os
import uuid

from automl_api.core.redaction import redact_text
from automl_api.schemas.parent_control import (
    PARENT_CONTROL_PROTOCOL,
    ParentControlReply,
    ParentControlRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptRPC


def parent_control(action, run_id, context, worker_id=None, *, failure=None, expected_kind=None):
    protocol = os.getenv("AUTOML_PARENT_CONTROL_PROTOCOL")
    if protocol is None:
        # shortcut: unmarked workers use SQL; retire after broker rollout and legacy drain.
        return None
    if protocol != PARENT_CONTROL_PROTOCOL or context is None:
        raise StaleFence("Parent control requires its declared protocol and current attempt.")
    try:
        if action == "start" and expected_kind is None:
            expected_kind = parent_control("context", run_id, context).run_kind
        request = ParentControlRequest(
            run_id=run_id,
            worker_id=worker_id or uuid.UUID(int=0),
            failure=redact_text(str(failure))[:2000] if failure is not None else None,
            run_kind=expected_kind,
        )
        control = AttemptRPC(
            context[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        reply = ParentControlReply.model_validate(
            control.request(
                "POST", f"parent/{action}", payload=request.model_dump(mode="json"), max_bytes=65536
            )
        )
        if (
            reply.attempt_id != context[0]
            or reply.run_id != run_id
            or reply.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
            or reply.audience != attempt_audience(*context)
            or (expected_kind is not None and reply.run_kind != expected_kind)
        ):
            raise ValueError("Parent control response belongs to another execution")
        return reply
    except Exception:
        raise StaleFence("Parent control could not verify its current execution.") from None
