"""Credentials restricted to a single deployment and its model download."""

from __future__ import annotations

import hashlib
import hmac
import uuid

from automl_api.core.config import get_settings


def deployment_token(deployment_id: uuid.UUID) -> str:
    return hmac.new(
        get_settings().jwt_secret_key.encode(),
        f"sceptre-deployment-v1:{deployment_id}".encode(),
        hashlib.sha256,
    ).hexdigest()
