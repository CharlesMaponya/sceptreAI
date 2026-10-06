"""Credential-scoped final evaluator; never retries evaluation or a final-data grant."""

import hashlib
import os
import signal
import tempfile
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, Request, build_opener

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from automl_api.storage.contracts import ObjectMetadata
from automl_api.training.champion_evaluation import (
    EvaluationPlan,
    GrantedFinalStore,
    execute_evaluation,
)
from automl_api.training.champion_refit import _copy_verified
from automl_api.training.refit_worker import _NoRedirect, _tls_context


def _https(url):
    parts = urlsplit(str(url))
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username
        or parts.password
        or parts.fragment
    ):
        raise ValueError("Evaluator endpoints require credential-free HTTPS URLs")


class CachedPipeline:
    def __init__(self, plan, control, path, *, ca_file, progress):
        self.item, self.path = plan.frozen_pipeline, path
        response = control.get("pipeline")
        response.raise_for_status()
        url = response.json()["url"]
        _https(url)
        opener = build_opener(_NoRedirect(), HTTPSHandler(context=_tls_context(ca_file)))
        source = SimpleNamespace(
            stat=self.stat,
            open_stream=lambda _uri: opener.open(Request(url, method="GET"), timeout=60),
        )
        with path.open("w+b") as stream:
            _copy_verified(source, self.item, stream, progress)

    def stat(self, uri):
        if uri != self.item.uri:
            raise ValueError("Evaluator may read only its frozen pipeline")
        return ObjectMetadata(uri=uri, byte_size=self.item.byte_size)

    def open_stream(self, uri):
        self.stat(uri)
        return self.path.open("rb")


def _acknowledge(operation):
    try:
        response = operation()
    except httpx.TransportError:
        response = operation()  # Only identical result writes/commits/publications use this helper.
    response.raise_for_status()
    return response


def run(control, authority, *, signing_key, authority_public_key, ca_file=None):
    _https(control.base_url)
    _https(authority.base_url)
    if not isinstance(signing_key, Ed25519PrivateKey):
        raise ValueError("Evaluator requires an Ed25519 result key")
    response = control.post("start")
    response.raise_for_status()
    plan = EvaluationPlan.model_validate(response.json())
    stop, lost = Event(), Event()

    def heartbeat():
        while not stop.wait(15):
            try:
                reply = control.post("heartbeat")
                reply.raise_for_status()
                if reply.json() != {"renewed": True}:
                    raise ValueError("Evaluator heartbeat changed")
            except Exception:
                lost.set()
                return

    def progress():
        if lost.is_set():
            raise RuntimeError("Evaluator control lease was lost")

    thread = Thread(target=heartbeat, name="evaluator-heartbeat", daemon=True)
    thread.start()
    try:
        if signing_key.public_key().public_bytes_raw().hex() != plan.result_public_key:
            raise ValueError("Evaluator result key differs from its plan")
        with tempfile.TemporaryDirectory() as directory:
            # Pipeline transport/hash failures happen before consuming final-data access.
            pipeline = CachedPipeline(
                plan,
                control,
                Path(directory) / "pipeline.joblib",
                ca_file=ca_file,
                progress=progress,
            )
            progress()
            path = f"allocations/{plan.allocation_id}"
            opened = {"provider_manifest_digest": plan.final_manifest.digest}
            # Deliberately no retry here, even when the response is lost.
            response = authority.post(path + "/credentials", json=opened)
            response.raise_for_status()
            final_store = GrantedFinalStore(
                plan, response.json(), authority_public_key, ca_file=ca_file
            )
            payload = execute_evaluation(
                plan, pipeline, final_store, signing_key=signing_key, progress=progress
            )
        progress()
        digest = hashlib.sha256(payload).hexdigest()
        root, separator, _ = plan.frozen_pipeline.uri.partition(
            f"/projects/{plan.project_id}/scopes/{plan.scope_id}/refit/"
        )
        if not separator:
            raise ValueError("Frozen pipeline storage lineage is invalid")
        expected = dict(
            uri=f"{root}/projects/{plan.project_id}/scopes/{plan.scope_id}/"
            f"evaluation/{plan.attempt_id}/{digest}/result.json",
            sha256=digest,
            byte_size=len(payload),
        )
        uploaded = _acknowledge(lambda: control.put("result", content=payload))
        if uploaded.json() != expected:
            raise ValueError("Evaluator result acknowledgement changed")
        progress()
        # The control API has independently verified persisted bytes before this acknowledgement.
        committed = _acknowledge(
            lambda: authority.post(path + "/commit", json={**opened, "result_digest": digest})
        )
        published = _acknowledge(lambda: control.post("publish", json=committed.json()))
        if published.json() != expected:
            raise ValueError("Evaluator publication acknowledgement changed")
        return expected
    except Exception:
        try:
            control.post("fail")
        except Exception:
            pass  # Reconciliation inspects authority and stored output; it never reopens data.
        raise
    finally:
        stop.set()
        thread.join(timeout=5)


def main():
    def terminate(_signum, _frame):
        raise InterruptedError("Evaluator workload is terminating")

    signal.signal(signal.SIGTERM, terminate)
    control_url, authority_url = (
        os.environ[name] for name in ("EVALUATION_CONTROL_URL", "EVALUATION_AUTHORITY_URL")
    )
    _https(control_url)
    _https(authority_url)
    ca = os.getenv("EVALUATION_CA_FILE") or None
    key = serialization.load_pem_private_key(
        Path(os.environ["EVALUATION_RESULT_KEY_FILE"]).read_bytes(), password=None
    )
    public_key = Path(os.environ["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"]).read_text()
    options = dict(
        verify=_tls_context(ca), follow_redirects=False, timeout=httpx.Timeout(60, connect=10)
    )
    with httpx.Client(
        base_url=control_url.rstrip("/") + "/",
        headers={"Authorization": f"Bearer {os.environ['EVALUATION_CONTROL_TOKEN']}"},
        **options,
    ) as control:
        with httpx.Client(
            base_url=authority_url.rstrip("/") + "/",
            headers={"Authorization": f"Bearer {os.environ['EVALUATION_AUTHORITY_TOKEN']}"},
            **options,
        ) as authority:
            run(control, authority, signing_key=key, authority_public_key=public_key, ca_file=ca)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"Evaluator worker failed: {type(error).__name__}") from None
