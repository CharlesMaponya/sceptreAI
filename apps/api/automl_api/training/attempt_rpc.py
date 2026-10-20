"""Bounded authenticated control RPCs using fresh projected identity."""

import json
import ssl
import uuid
from pathlib import Path

import httpx

from automl_api.training.attempt_admission import TOKEN_PATH, control_base_url


class AttemptControlUnavailable(RuntimeError):
    pass


class AttemptRPC:
    def __init__(self, attempt_id, *, base_url, token_file=TOKEN_PATH, ca_file=None):
        self.attempt_id = uuid.UUID(str(attempt_id))
        self.base_url = control_base_url(base_url)
        self.token_file = Path(token_file)
        self.tls = ssl.create_default_context()
        if ca_file:
            self.tls.load_verify_locations(cafile=ca_file)

    def request(self, method, path, *, payload=None, max_bytes):
        try:
            if type(max_bytes) is not int or max_bytes <= 0:
                raise ValueError("A positive control response budget is required")
            with self.token_file.open("rb") as source:
                encoded = source.read(65537)
            if len(encoded) > 65536:
                raise ValueError("Projected workload identity is too large")
            token = encoded.decode().strip()
            if not token:
                raise ValueError("Projected workload identity is invalid")
            with httpx.Client(
                verify=self.tls, trust_env=False, timeout=5, follow_redirects=False
            ) as client:
                with client.stream(
                    method,
                    f"{self.base_url}/{self.attempt_id}/{path}",
                    headers={"Authorization": f"Bearer {token}", "Accept-Encoding": "identity"},
                    json=payload,
                ) as response:
                    response.raise_for_status()
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise ValueError("Encoded control responses are not accepted")
                    body = bytearray()
                    for block in response.iter_bytes(chunk_size=8192):
                        if len(body) + len(block) > max_bytes:
                            raise ValueError("Control response exceeds its byte budget")
                        body.extend(block)
            return json.loads(body)
        except Exception:
            raise AttemptControlUnavailable("Workload control request unavailable") from None
