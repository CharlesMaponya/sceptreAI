"""Renew a pod's own serving key without accepting platform or object-store credentials."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import ssl
import stat
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from cryptography.hazmat.primitives import hashes, serialization

from automl_api.services.inference_tls import (
    IDENTITY_DIRECTORY,
    MAX_PEM_BYTES,
    PROTOCOL,
    TOKEN_PATH,
    PeerIdentity,
    _load_chain,
    ca_certificate,
    make_csr,
    new_private_key,
    private_key_pem,
    server_context,
    validate_server_certificate,
)


def serving_key(directory):
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / "serving-key.pem"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError("Serving key permissions changed") from None
            encoded = source.read(MAX_PEM_BYTES + 1)
            if len(encoded) > MAX_PEM_BYTES:
                raise ValueError("Serving key exceeds its byte budget") from None
            key = serialization.load_pem_private_key(encoded, password=None)
            make_csr(key)
            return key
    key = new_private_key()
    with os.fdopen(descriptor, "wb") as destination:
        destination.write(private_key_pem(key))
        destination.flush()
        os.fsync(destination.fileno())
    return key


class ServingCredentials:
    def __init__(self):
        if os.environ.get("INFERENCE_IDENTITY_PROTOCOL") != PROTOCOL:
            raise ValueError("Serving requires the pod TLS protocol")
        self.run_id = str(uuid.UUID(os.environ["INFERENCE_DEPLOYMENT_ID"]))
        self.project_id = str(uuid.UUID(os.environ["INFERENCE_PROJECT_ID"]))
        self.ca_digest = os.environ["INFERENCE_CA_SHA256"]
        self.model_digest = os.environ["MODEL_SHA256"]
        for digest in (self.ca_digest, self.model_digest):
            if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
                raise ValueError("Serving requires exact certificate and model digests")
        self.identity_url = os.environ["INFERENCE_IDENTITY_URL"]
        parts = urlsplit(self.identity_url)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or not parts.path.endswith(f"/internal/deployments/{self.run_id}/identity")
        ):
            raise ValueError("Serving identity requires its authenticated HTTPS control URL")
        self.model_url = self.identity_url.removesuffix("/identity") + "/model"
        if os.environ.get("MODEL_DOWNLOAD_URL") != self.model_url:
            raise ValueError("Serving model download must use the original control endpoint")
        self.key = serving_key(Path(IDENTITY_DIRECTORY) / "private")
        self.control_tls = ssl.create_default_context()
        self.context = None
        self.peer = None
        self.expires = datetime.min.replace(tzinfo=UTC)
        self.model_byte_size = None

    @property
    def active(self):
        return self.context is not None and datetime.now(UTC) < self.expires

    def _headers(self):
        with Path(TOKEN_PATH).open("rb") as source:
            encoded = source.read(65537)
        if not 0 < len(encoded) <= 65536:
            raise ValueError("A bounded projected pod token is required")
        token = encoded.decode("ascii").strip()
        if not token or any(char.isspace() for char in token):
            raise ValueError("Invalid projected pod token")
        return {"Authorization": f"Bearer {token}", "Accept-Encoding": "identity"}

    def _client(self):
        return httpx.Client(
            verify=self.control_tls,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(40, connect=5),
        )

    def renew(self):
        try:
            with self._client() as client:
                with client.stream(
                    "POST",
                    self.identity_url,
                    headers=self._headers(),
                    content=make_csr(self.key),
                ) as response:
                    response.raise_for_status()
                    if (
                        response.status_code != 200
                        or response.headers.get("Content-Encoding", "identity") != "identity"
                    ):
                        raise ValueError("Invalid serving admission response")
                    body = bytearray()
                    for block in response.iter_bytes(chunk_size=8192):
                        if len(body) + len(block) > 3 * MAX_PEM_BYTES:
                            raise ValueError("Serving admission exceeds its byte budget")
                        body.extend(block)
            proof = json.loads(body)
            peer = PeerIdentity(**proof["identity"])
            ca = proof["ca_certificate"].encode("ascii")
            cert = proof["certificate"].encode("ascii")
            size = proof["model_byte_size"]
            if (
                proof["protocol"] != PROTOCOL
                or peer.project_id != self.project_id
                or peer.run_id != self.run_id
                or (self.peer is not None and peer != self.peer)
                or ca_certificate(ca).fingerprint(hashes.SHA256()).hex() != self.ca_digest
                or proof["model_digest"] != self.model_digest
                or type(size) is not int
                or size <= 0
                or (self.model_byte_size is not None and size != self.model_byte_size)
            ):
                raise ValueError("Serving admission differs from its original scope")
            expires = validate_server_certificate(ca, cert, peer, self.key)
            if self.context is None:
                self.context = server_context(ca, cert, private_key_pem(self.key))
            else:
                _load_chain(self.context, cert, private_key_pem(self.key))
            self.peer, self.expires, self.model_byte_size = peer, expires, size
        except Exception:
            self.expires = datetime.min.replace(tzinfo=UTC)
            raise RuntimeError("Serving identity renewal unavailable") from None

    def download_model(self):
        if not self.active:
            raise RuntimeError("Serving requires an active identity before model download")
        model = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
        try:
            digest, size = hashlib.sha256(), 0
            with self._client() as client:
                with client.stream("GET", self.model_url, headers=self._headers()) as response:
                    response.raise_for_status()
                    if (
                        response.status_code != 200
                        or response.headers.get("Content-Encoding", "identity") != "identity"
                    ):
                        raise ValueError("Invalid model download response")
                    for block in response.iter_bytes(chunk_size=65536):
                        size += len(block)
                        if size > self.model_byte_size or not self.active:
                            raise ValueError("Model download exceeded its original scope")
                        digest.update(block)
                        model.write(block)
            if size != self.model_byte_size or not hmac.compare_digest(
                digest.hexdigest(), self.model_digest
            ):
                raise ValueError("Model integrity verification failed")
            model.seek(0)
            return model
        except Exception:
            model.close()
            raise RuntimeError("Original serving model download unavailable") from None
