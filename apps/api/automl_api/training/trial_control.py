"""Durable trial RPCs using freshly read projected Pod identity."""

import os
import uuid

from automl_api.core.redaction import redact_text
from automl_api.schemas.trial_control import (
    TRIAL_CONTROL_MAX_BYTES,
    TRIAL_CONTROL_PROTOCOL,
    TrialControlReply,
    TrialControlRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptRPC


def trial_control(action, run, parent, **fields):
    protocol = os.getenv("AUTOML_TRIAL_CONTROL_PROTOCOL")
    if protocol is None:
        # shortcut: unmarked workers retain SQL until broker rollout and legacy drain.
        return None
    if protocol != TRIAL_CONTROL_PROTOCOL or parent is None:
        raise StaleFence("Trial control requires its declared protocol and current parent")
    try:
        if fields.get("failure") is not None:
            fields["failure"] = redact_text(str(fields["failure"]))[:2000]
        request = TrialControlRequest(
            run_id=run.id, worker_id=fields.pop("worker_id", uuid.UUID(int=0)), **fields
        )
        if len(request.model_dump_json().encode()) > TRIAL_CONTROL_MAX_BYTES:
            raise ValueError("Trial metadata exceeds its byte budget")
        control = AttemptRPC(
            parent[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        reply = TrialControlReply.model_validate(
            control.request(
                "POST",
                f"trials/{action}",
                payload=request.model_dump(mode="json"),
                max_bytes=TRIAL_CONTROL_MAX_BYTES,
            )
        )
        if (
            reply.parent_attempt_id != parent[0]
            or reply.run_id != run.id
            or reply.project_id != run.project_id
            or reply.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
            or reply.audience != attempt_audience(*parent)
            or (
                request.child_attempt_id is not None
                and reply.child_attempt_id != request.child_attempt_id
            )
            or (action == "enroll" and reply.terminal is not None)
            or (action == "finish" and reply.terminal is None)
        ):
            raise ValueError("Trial control belongs to another execution")
        if action == "finish":
            terminal = (
                {"status": "failed", "failure": request.failure}
                if request.failure is not None
                else {"status": "succeeded", "result": request.result}
            )
            if canonical_request_hash(reply.terminal) != canonical_request_hash(terminal):
                raise ValueError("Trial control acknowledged another result")
        return reply
    except Exception:
        raise StaleFence("Trial control could not verify its current execution.") from None
