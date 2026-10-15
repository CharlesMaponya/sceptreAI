"""QA-only TLS fault endpoint for native evaluator commit recovery probes."""

import argparse
import http.client
import json
import os
import re
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class CommitFaultHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass  # Never log request headers or evaluator credentials.

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def do_POST(self):
        route = re.fullmatch(r"/allocations/[0-9a-f-]{36}/(credentials|commit)", self.path)
        if route is None:
            self.send_error(404)
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_error(400)
            return
        if not 0 < size <= 65536 or self.headers.get("Transfer-Encoding"):
            self.send_error(413)
            return
        body = self.rfile.read(size)
        if len(body) != size:
            self.send_error(400)
            return
        commit = route[1] == "commit"
        if commit and self.server.mode == "before":
            self.close_connection = True
            print(json.dumps({"fault": "before_commit"}), flush=True)
            return
        upstream = self.server.connect()
        try:
            upstream.request(
                "POST",
                self.path,
                body=body,
                headers={
                    "Authorization": self.headers.get("Authorization", ""),
                    "Content-Type": "application/json",
                },
            )
            response = upstream.getresponse()
            payload = response.read(65537)
            status = response.status
        finally:
            upstream.close()
        if len(payload) > 65536:
            self.send_error(502)
            return
        if commit and self.server.mode == "after" and status == 200:
            self.close_connection = True
            print(json.dumps({"fault": "after_commit", "upstream_status": status}), flush=True)
            return
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["before", "after"], required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--ca", required=True)
    args = parser.parse_args()
    if not os.environ.get("TRAINING_NAMESPACE", "").startswith("qa-"):
        raise SystemExit("Fault endpoint is restricted to isolated qa-* test namespaces")
    target = urlsplit(args.upstream)
    if (
        target.scheme != "https"
        or not target.hostname
        or target.username
        or target.password
        or target.path not in ("", "/")
        or target.query
        or target.fragment
    ):
        raise SystemExit("Upstream must be an HTTPS origin")
    client_tls = ssl.create_default_context(cafile=args.ca)
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(args.cert, args.key)
    with ThreadingHTTPServer(("0.0.0.0", 8443), CommitFaultHandler) as server:
        server.mode = args.mode
        server.connect = lambda: http.client.HTTPSConnection(
            target.hostname, target.port or 443, context=client_tls, timeout=10
        )
        server.socket = server_tls.wrap_socket(server.socket, server_side=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
