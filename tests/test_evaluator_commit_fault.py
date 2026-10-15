import http.client
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest

spec = importlib.util.spec_from_file_location(
    "commit_fault", Path("scripts/evaluator_commit_fault.py")
)
fault = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fault)


@pytest.mark.parametrize(
    "namespace,origin",
    [
        ("production", "https://authority.test"),
        ("qa-test", "http://authority.test"),
        ("qa-test", "https://user:password@authority.test"),
        ("qa-test", "https://authority.test/unexpected-path"),
    ],
)
def test_fault_endpoint_rejects_unsafe_configuration(monkeypatch, namespace, origin):
    monkeypatch.setenv("TRAINING_NAMESPACE", namespace)
    monkeypatch.setattr(
        "sys.argv",
        [
            "fault",
            "--mode",
            "before",
            "--upstream",
            origin,
            "--cert",
            "unused",
            "--key",
            "unused",
            "--ca",
            "unused",
        ],
    )
    with pytest.raises(SystemExit, match="restricted|HTTPS origin"):
        fault.main()


@pytest.mark.parametrize("mode,status", [("before", 200), ("after", 200), ("after", 409)])
def test_commit_fault_boundary_and_credential_forwarding(mode, status):
    calls = []

    class Upstream(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            calls.append(
                (
                    self.path,
                    self.headers.get("Authorization"),
                    self.rfile.read(int(self.headers["Content-Length"])),
                )
            )
            self.send_response(status if self.path.endswith("commit") else 200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

    with (
        ThreadingHTTPServer(("127.0.0.1", 0), Upstream) as upstream,
        ThreadingHTTPServer(("127.0.0.1", 0), fault.CommitFaultHandler) as proxy,
    ):
        proxy.mode = mode
        proxy.connect = lambda: http.client.HTTPConnection(*upstream.server_address, timeout=3)
        threads = [Thread(target=s.serve_forever, daemon=True) for s in (upstream, proxy)]
        for thread in threads:
            thread.start()
        try:

            def post(path, body=b"{}"):
                connection = http.client.HTTPConnection(*proxy.server_address, timeout=3)
                try:
                    connection.request("POST", path, body, {"Authorization": "Bearer test-only"})
                    response = connection.getresponse()
                    return response.status, response.read()
                finally:
                    connection.close()

            root = "/allocations/00000000-0000-0000-0000-000000000001/"
            assert post(root + "credentials") == (200, b"{}")
            assert calls == [(root + "credentials", "Bearer test-only", b"{}")]
            for _ in range(2):
                if mode == "before" or status == 200:
                    with pytest.raises(http.client.RemoteDisconnected):
                        post(root + "commit")
                else:
                    assert post(root + "commit") == (409, b"{}")
            assert len(calls) == (1 if mode == "before" else 3)
            count = len(calls)
            assert post("/arbitrary/path")[0] == 404
            assert post(root + "commit", b"x" * 65537)[0] == 413
            assert len(calls) == count
        finally:
            for server in (proxy, upstream):
                server.shutdown()
            for thread in threads:
                thread.join(timeout=5)
