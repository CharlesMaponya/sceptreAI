"""Pause the owned admission process on either side of its SQL commit."""

import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.iam import User
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import DatasetUploadRequest
from automl_api.services import buffered_uploads as service
from fault_worker_environment import database_url
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import Session, sessionmaker

actor_id, project_id, key, directory, mode = sys.argv[1:]
actor_id, project_id = uuid.UUID(actor_id), uuid.UUID(project_id)
root = Path(directory).resolve()
assert root.is_dir() and mode in {"before-commit", "after-commit"}
engine = create_engine(
    database_url(),
    pool_size=1,
    max_overflow=0,
    connect_args={"application_name": f"phase1_buffered_input_fault_{os.getpid()}"},
)
factory = sessionmaker(engine, expire_on_commit=False)
with factory() as db:
    token_version = db.get(User, actor_id).token_version
    backend_pid = db.scalar(text("SELECT pg_backend_pid()"))
body = (root / "body.csv").read_bytes()
payload = DatasetUploadRequest.model_validate_json((root / "payload.json").read_text())


def pause(phase, identifier):
    temporary = root / "checkpoint.tmp"
    temporary.write_text(
        json.dumps(
            {
                "phase": phase,
                "command_id": str(identifier),
                "pid": os.getpid(),
                "backend_pid": backend_pid,
                "sha256": hashlib.sha256(body).hexdigest(),
                "sql_checkouts": engine.pool.checkedout(),
            }
        )
        + "\n"
    )
    temporary.replace(root / "checkpoint.json")
    assert os.read(sys.stdin.fileno(), 1) == b"c"


def before_commit(db):
    if mode != "before-commit":
        return
    identifier = db.scalar(
        select(BufferedUploadInput.command_id)
        .join(WorkflowCommand, BufferedUploadInput.command_id == WorkflowCommand.id)
        .where(WorkflowCommand.project_id == project_id, WorkflowCommand.idempotency_key == key)
    )
    if identifier is not None:
        pause("before-commit", identifier)


event.listen(Session, "before_commit", before_commit)
identifier = service.admit_input(factory, actor_id, token_version, project_id, payload, body, key)
pause("after-commit", identifier)
