"""Refit workload using attempt capabilities, with no database or bucket credentials."""

from __future__ import annotations

import hashlib
import os
import signal
import ssl
from threading import Event, Thread
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

import httpx

from automl_api.storage.contracts import ObjectMetadata
from automl_api.training.champion_refit import RefitPlan, execute_refit


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Refit input redirects are forbidden")


def _tls_context(ca_file):
    context = ssl.create_default_context()
    if ca_file:
        context.load_verify_locations(cafile=ca_file)
    return context


class RefitCapabilityStore:
    def __init__(self, plan, control, *, ca_file=None):
        self.plan, self.control = plan, control
        self.inputs = {item.uri: (i, item) for i, item in enumerate(
            (plan.candidate, *plan.partitions)
        )}
        self.root, separator, _ = plan.candidate.uri.partition(
            f"/projects/{plan.project_id}/runs/{plan.run_id}/"
        )
        if not separator:
            raise ValueError("Candidate storage lineage is invalid")
        self.opener = build_opener(
            _NoRedirect(), HTTPSHandler(context=_tls_context(ca_file)),
        )

    def uri_for_key(self, key):
        return self.root + "/" + key

    def stat(self, uri):
        _, item = self.inputs[uri]
        return ObjectMetadata(uri=item.uri, byte_size=item.byte_size)

    def open_stream(self, uri):
        index, _ = self.inputs[uri]
        response = self.control.get(f"inputs/{index}")
        response.raise_for_status()
        url = response.json()["url"]
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.username or parts.fragment:
            raise ValueError("Refit inputs require HTTPS capabilities")
        # Separate transport: the control bearer token never reaches object storage.
        return self.opener.open(Request(url, method="GET"), timeout=60)

    def put_bytes(self, key, value):
        digest = hashlib.sha256(value).hexdigest()
        expected = (
            f"projects/{self.plan.project_id}/scopes/{self.plan.scope_id}/"
            f"refit/{self.plan.attempt_id}/{digest}/pipeline.joblib"
        )
        if key != expected or len(value) > self.plan.max_model_bytes:
            raise ValueError("Refit output is outside the attempt capability")
        response = self.control.put(
            f"model/{digest}", content=value,
            headers={"Content-Type": "application/octet-stream"},
        )
        response.raise_for_status()
        stored = response.json()
        if stored != {"uri": self.uri_for_key(key), "sha256": digest, "byte_size": len(value)}:
            raise ValueError("Refit output acknowledgement changed")
        return ObjectMetadata(uri=stored["uri"], byte_size=stored["byte_size"])


def run(control, *, ca_file=None):
    response = control.post("start")
    response.raise_for_status()
    plan = RefitPlan.model_validate(response.json())
    stop, lost = Event(), Event()

    def heartbeat():
        while not stop.wait(15):
            try:
                renewed = control.post("heartbeat")
                renewed.raise_for_status()
                if renewed.json() != {"renewed": True}:
                    raise ValueError("Refit lease renewal rejected")
            except Exception:
                lost.set()
                return

    def progress():
        if lost.is_set():
            raise RuntimeError("Refit control lease was lost")

    thread = Thread(target=heartbeat, name="refit-control-heartbeat", daemon=True)
    thread.start()
    try:
        result = execute_refit(
            plan, RefitCapabilityStore(plan, control, ca_file=ca_file), progress=progress,
        )
        progress()
        # A lost acknowledgement replays the same CAS; never repeat fitting here.
        try:
            published = control.post("publish", json=result)
        except httpx.TransportError:
            published = control.post("publish", json=result)
        published.raise_for_status()
        if published.json()["digest"] != result["frozen_pipeline_digest"]:
            raise ValueError("Refit publication acknowledgement changed")
        return result
    except Exception:
        try:
            control.post("fail")
        except Exception:
            pass  # The reconciler owns recovery after lease loss or an unavailable API.
        raise
    finally:
        stop.set()
        thread.join(timeout=5)


def main():
    def terminate(_signum, _frame):
        raise InterruptedError("Refit workload is terminating")

    # Python as container PID 1 otherwise inherits special SIGTERM handling.
    signal.signal(signal.SIGTERM, terminate)
    base_url = os.environ["REFIT_CONTROL_URL"]
    if urlsplit(base_url).scheme != "https":
        raise ValueError("Refit control requires HTTPS")
    ca_file = os.getenv("REFIT_CA_FILE") or None
    with httpx.Client(
        base_url=base_url.rstrip("/") + "/",
        headers={"Authorization": f"Bearer {os.environ['REFIT_CONTROL_TOKEN']}"},
        verify=_tls_context(ca_file), follow_redirects=False,
        timeout=httpx.Timeout(60, connect=10),
    ) as control:
        run(control, ca_file=ca_file)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # HTTP/provider exception strings can contain capability URLs or credentials.
        raise SystemExit(f"Refit worker failed: {type(exc).__name__}") from None
