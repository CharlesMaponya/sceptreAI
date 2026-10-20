"""Read signed safe objects through the broker, without a provider SDK or storage key."""

import os
import uuid
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, SecretStr

from automl_api.services.provider_distribution import Digest, ProviderDistribution
from automl_api.storage.read_capabilities import validate_native_read_url
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptRPC

MAX_GRANT_RESPONSE_BYTES = 64 * 1024


class ProviderObjectUnavailable(RuntimeError):
    pass


class _ReadGrant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    grant_id: uuid.UUID
    grant_digest: Digest
    expires_at: AwareDatetime
    byte_limit: Annotated[int, Field(ge=0, strict=True)]
    sha256: Digest
    url: SecretStr


class _BoundedChunks:
    def __init__(self, response, limit):
        self.chunks = response.iter_raw(chunk_size=65536)
        self.limit, self.received, self.pending = limit, 0, b""

    def read(self, size):
        if type(size) is not int or not 0 <= size <= 65536:
            raise ValueError("Provider reads require a bounded chunk size")
        result = bytearray()
        while len(result) < size:
            if not self.pending:
                try:
                    block = next(self.chunks, b"")
                except Exception:
                    raise ProviderObjectUnavailable(
                        "Approved provider object stream failed"
                    ) from None
                if not block:
                    if self.received != self.limit:
                        raise ValueError("Provider object size differs from its declaration")
                    break
                self.received += len(block)
                if self.received > self.limit:
                    raise ValueError("Provider object exceeds its declared bytes")
                self.pending = block
            take = min(size - len(result), len(self.pending))
            result.extend(self.pending[:take])
            self.pending = self.pending[take:]
        return bytes(result)


class ProviderGrantReader:
    def __init__(self, manifest, attempt_id, *, base_url, token_file=TOKEN_PATH, ca_file=None):
        manifest = ProviderDistribution.model_validate(manifest)
        if manifest.mode != "validation_only" or manifest.final_data is not None:
            raise ValueError("Provider verification permits only signed safe source roles")
        self.control = AttemptRPC(
            attempt_id, base_url=base_url, token_file=token_file, ca_file=ca_file
        )
        self.driver_name = {"aws": "aws_s3", "gcp": "gcs", "azure": "azure_blob"}[manifest.provider]
        self.tls = self.control.tls
        self.objects = {}
        counts = {"train": 0, "validation": 0}
        for item in manifest.safe_data.objects:
            key = (item.uri, item.version)
            if key in self.objects:
                raise ValueError("Provider safe object declaration is ambiguous")
            self.objects[key] = (item, counts[item.role])
            counts[item.role] += 1

    @contextmanager
    def open_stream(self, uri, *, version):
        if (uri, version) not in self.objects:
            raise ValueError("Provider object is outside the approved safe declaration")
        item, index = self.objects[(uri, version)]
        with ExitStack() as cleanup:
            try:
                grant = _ReadGrant.model_validate(
                    self.control.request(
                        "POST",
                        "objects/read",
                        payload={
                            "request_id": str(uuid.uuid4()),
                            "role": item.role,
                            "object_index": index,
                        },
                        max_bytes=MAX_GRANT_RESPONSE_BYTES,
                    )
                )
                client = cleanup.enter_context(
                    httpx.Client(
                        verify=self.tls, trust_env=False, timeout=5, follow_redirects=False
                    )
                )
                now = datetime.now(UTC)
                if (
                    grant.byte_limit != item.byte_size
                    or grant.sha256 != item.sha256
                    or not now < grant.expires_at <= now + timedelta(minutes=15)
                ):
                    raise ValueError("Broker returned another object or invalid lifetime")
                url = grant.url.get_secret_value()
                validate_native_read_url(url, self.driver_name, grant.expires_at, version, now=now)
                response = cleanup.enter_context(
                    client.stream("GET", url, headers={"Accept-Encoding": "identity"})
                )
                if response.status_code != 200 or "content-range" in response.headers:
                    raise ValueError("Provider did not return the complete approved object")
                version_header = {
                    "aws_s3": "x-amz-version-id",
                    "gcs": "x-goog-generation",
                    "azure_blob": "x-ms-version-id",
                }[self.driver_name]
                if response.headers.get_list(version_header) != [version]:
                    raise ValueError("Provider returned a different or missing object version")
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ValueError("Provider changed the encoded object bytes")
                length = response.headers.get("content-length")
                if length is not None and (
                    not length.isascii() or not length.isdecimal() or int(length) != item.byte_size
                ):
                    raise ValueError("Provider object size differs from its declaration")
                reader = _BoundedChunks(response, item.byte_size)
            except Exception:
                raise ProviderObjectUnavailable(
                    "Approved provider object could not be read"
                ) from None
            yield reader


def provider_grant_reader(manifest):
    try:
        return ProviderGrantReader(
            manifest,
            os.environ["AUTOML_ATTEMPT_ID"],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
    except Exception:
        raise ProviderObjectUnavailable(
            "Provider reads require configured broker identity"
        ) from None
