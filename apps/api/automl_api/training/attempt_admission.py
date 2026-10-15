"""Re-read projected pod identity on every admission and rotation check."""

import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from automl_api.core.config import get_settings
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence

ANALYSIS_PROTOCOL = "analysis-v1"
TRAINING_CATALOG_PROTOCOL = "training-catalog-v1"
TOKEN_PATH = "/var/run/sceptre/identity/token"


def control_base_url(value):
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or not parts.netloc
        or parts.username is not None
        or parts.query
        or parts.fragment
    ):
        raise ValueError("Attempt admission requires an HTTPS control URL without credentials.")
    return value.rstrip("/")


def admit_parent(context, *, project_id=None):
    settings = get_settings()
    base = os.getenv("ATTEMPT_CONTROL_BASE_URL", settings.attempt_control_base_url)
    required = settings.environment in {"production", "staging"}
    if not base:
        if required:
            raise StaleFence("Production training requires attempt admission configuration.")
        return None
    if context is None:
        raise StaleFence("Attempt admission requires a durable parent fence.")
    try:
        base = control_base_url(base)
        token = Path(os.getenv("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH)).read_text().strip()
        if not token or len(token) > 65536:
            raise ValueError("Invalid projected identity token.")
        response = httpx.post(
            f"{base}/{context[0]}/admission",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
            follow_redirects=False,
        )
        response.raise_for_status()
        proof = response.json()
        now = int(datetime.now(UTC).timestamp())
        if (
            proof.get("attempt_id") != str(context[0])
            or (project_id is not None and proof.get("project_id") != str(project_id))
            or proof.get("audience") != attempt_audience(*context)
            or type(proof.get("expires_at")) is not int
            or not now < proof["expires_at"] <= now + 900
        ):
            raise ValueError("Admission response has invalid identity or expiry.")
        return proof
    except Exception as exc:
        raise StaleFence("Attempt admission could not verify current workload identity.") from exc


def proof_deadline(proof):
    if proof is None:
        return None
    deadline = datetime.fromtimestamp(proof["expires_at"], UTC)
    if deadline <= datetime.now(UTC):
        raise StaleFence("Attempt admission expired before lease acquisition.")
    return deadline
