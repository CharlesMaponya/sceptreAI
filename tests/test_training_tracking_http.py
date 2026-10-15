import http.client
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import mlflow
import pytest
from automl_api.training import pipeline
from mlflow import MlflowClient
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_tracking import tracking  # noqa: F401


@pytest.fixture(params=["503", "disconnect"])
def http_tracking(tracking, monkeypatch, tmp_path, request):  # noqa: F811
    engine, run, attempt_id, store_client = tracking
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    log = (tmp_path / "mlflow-server.log").open("w+")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "mlflow",
            "server",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "1",
            "--backend-store-uri",
            store_client.tracking_uri,
            "--default-artifact-root",
            (tmp_path / "artifacts").as_uri(),
            "--no-serve-artifacts",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    proxy = None
    thread = None
    calls = []

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.forward()

        def do_POST(self):
            self.forward()

        def forward(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            try:
                connection.request(
                    self.command, self.path, body=body, headers={"Content-Type": "application/json"}
                )
                response = connection.getresponse()
                payload = response.read()
                status = response.status
                if self.path.endswith("/runs/create"):
                    calls.append(status)
                    if len(calls) == 1 and status == 200:
                        if request.param == "disconnect":
                            self.close_connection = True
                            self.connection.shutdown(socket.SHUT_RDWR)
                            return
                        status = 503
                        payload = (
                            b'{"error_code":"TEMPORARILY_UNAVAILABLE",'
                            b'"message":"lost committed response"}'
                        )
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            finally:
                connection.close()

    try:
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if process.poll() is not None:
                log.seek(0)
                pytest.fail(f"MLflow server exited: {log.read()[-2000:]}")
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
            try:
                connection.request("GET", "/health")
                response = connection.getresponse()
                response.read()
                if response.status == 200:
                    break
            except OSError:
                time.sleep(0.1)
            finally:
                connection.close()
        else:
            pytest.fail("MLflow HTTP server did not become healthy")
        proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        uri = f"http://127.0.0.1:{proxy.server_port}"
        mlflow.set_tracking_uri(uri)
        client = MlflowClient(tracking_uri=uri)
        monkeypatch.setattr(pipeline, "MlflowClient", lambda: client)
        monkeypatch.setenv("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "1")
        monkeypatch.setenv("MLFLOW_HTTP_REQUEST_BACKOFF_FACTOR", "0")
        monkeypatch.setenv("MLFLOW_HTTP_REQUEST_BACKOFF_JITTER", "0")
        yield run, store_client, calls
    finally:
        try:
            mlflow.end_run()
        finally:
            if proxy is not None:
                proxy.shutdown()
                proxy.server_close()
                thread.join(timeout=5)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
            log.close()


def test_committed_http_creation_failure_does_not_trigger_sdk_retry(http_tracking):
    run, store_client, calls = http_tracking
    with pytest.raises(pipeline.TrackingCreationUnresolved):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="http-fixture")
    experiment = store_client.get_experiment_by_name(f"automl-project-{run.project_id}")
    stored = store_client.search_runs([experiment.experiment_id])
    assert len(stored) == 1 and calls == [200]
    for _ in range(2):
        with pipeline._tracked_mlflow_run(run, role="parent", run_name="http-fixture") as resumed:
            assert resumed.info.run_id == stored[0].info.run_id
    assert calls == [200]
