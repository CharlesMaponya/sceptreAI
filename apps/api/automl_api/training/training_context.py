"""Load validated input metadata using fresh Pod identity, never worker SQL."""

import os
import uuid

from automl_api.models.runs import ModelRun
from automl_api.models.workflows import DatasetSplitRevision
from automl_api.schemas.training_context import (
    TRAINING_CONTEXT_MAX_BYTES,
    TRAINING_CONTEXT_PROTOCOL,
    TrainingContextReply,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptRPC


def training_context(run_id):
    protocol = os.getenv("AUTOML_TRAINING_CONTEXT_PROTOCOL")
    if protocol is None:
        # shortcut: unmarked workers retain SQL until the context/broker rollout drains them.
        return None
    if protocol != TRAINING_CONTEXT_PROTOCOL:
        raise StaleFence("Unsupported training context protocol")
    try:
        from automl_api.training.worker import _fenced_attempt_context

        context = _fenced_attempt_context()
        if context is None:
            raise ValueError("Training context requires a current attempt")
        control = AttemptRPC(
            context[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        reply = TrainingContextReply.model_validate(
            control.request(
                "POST",
                "training/context",
                payload={"run_id": str(run_id)},
                max_bytes=TRAINING_CONTEXT_MAX_BYTES,
            )
        )
        if (
            reply.attempt_id != context[0]
            or reply.run.id != run_id
            or reply.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
            or reply.audience != attempt_audience(*context)
        ):
            raise ValueError("Training context belongs to another execution")
        split = reply.split
        specification = {
            name: getattr(split, name)
            for name in (
                "split_counts",
                "split_strategy",
                "time_column",
                "row_set_digest",
                "provider_import_id",
            )
        }
        if specification["provider_import_id"] is not None:
            specification["provider_import_id"] = str(specification["provider_import_id"])
        specification["uris"] = {"train": split.train_uri, "validation": split.validation_uri}
        return (
            ModelRun(**reply.run.model_dump()),
            DatasetSplitRevision(
                id=split.id,
                project_id=split.project_id,
                dataset_version_id=split.dataset_version_id,
                specification=specification,
            ),
            reply.approved_columns,
            reply.source_plan,
        )
    except Exception:
        raise StaleFence("Training context could not verify its current execution.") from None
