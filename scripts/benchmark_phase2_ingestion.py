#!/usr/bin/env python3
"""Drive the Phase 2 browser-direct ingestion qualification workload.

The harness deliberately refuses to start unless the reference object store has
space for every payload plus headroom and a signed control-plane RSS limit is
provided. Payload bytes are generated as a deterministic, valid CSV stream, so
the runner never needs a 10-GiB source file on local disk.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

GIB = 1024**3
DEFAULT_OBJECTS = 15
DEFAULT_OBJECT_BYTES = 10 * GIB
DEFAULT_HEADROOM_FRACTION = 0.20
DEFAULT_MINIMUM_GOODPUT_BITS_PER_SECOND = 500_000_000


@dataclass(frozen=True)
class CapacityPreflight:
    object_count: int
    object_bytes: int
    payload_bytes: int
    required_capacity_bytes: int
    available_capacity_bytes: int
    headroom_fraction: float
    passed: bool


@dataclass(frozen=True)
class UploadResult:
    session_id: str
    dataset_name: str
    confirmed_bytes: int
    payload_host: str
    status: str
    elapsed_seconds: float


def capacity_preflight(
    *,
    object_count: int,
    object_bytes: int,
    available_capacity_bytes: int,
    headroom_fraction: float = DEFAULT_HEADROOM_FRACTION,
) -> CapacityPreflight:
    if object_count <= 0 or object_bytes <= 0 or available_capacity_bytes < 0:
        raise ValueError("Capacity inputs must be positive.")
    if headroom_fraction < 0:
        raise ValueError("Headroom cannot be negative.")
    payload_bytes = object_count * object_bytes
    required = int(payload_bytes * (1 + headroom_fraction))
    return CapacityPreflight(
        object_count=object_count,
        object_bytes=object_bytes,
        payload_bytes=payload_bytes,
        required_capacity_bytes=required,
        available_capacity_bytes=available_capacity_bytes,
        headroom_fraction=headroom_fraction,
        passed=available_capacity_bytes >= required,
    )


class CsvRange:
    """Seek-free file object exposing an arbitrary range of one valid CSV."""

    header = b"feature,target\n"
    row = b"0,0\n"

    def __init__(self, total_size: int, offset: int, length: int) -> None:
        if total_size < len(self.header) + 3:
            raise ValueError("CSV fixture is too small.")
        if offset < 0 or length <= 0 or offset + length > total_size:
            raise ValueError("CSV range is outside the fixture.")
        body_size = total_size - len(self.header)
        self.full_rows = (body_size - 3) // len(self.row)
        tail_size = body_size - self.full_rows * len(self.row)
        self.tail = b"x" * (tail_size - 3) + b",0\n"
        self.total_size = total_size
        self.position = offset
        self.end = offset + length

    def read(self, size: int = -1) -> bytes:
        remaining = self.end - self.position
        if remaining <= 0:
            return b""
        requested = remaining if size < 0 else min(size, remaining)
        result = self._slice(self.position, requested)
        self.position += len(result)
        return result

    def _slice(self, offset: int, length: int) -> bytes:
        chunks: list[bytes] = []
        remaining = length
        position = offset
        header_end = len(self.header)
        repeated_end = header_end + self.full_rows * len(self.row)
        if position < header_end:
            take = min(remaining, header_end - position)
            chunks.append(self.header[position : position + take])
            position += take
            remaining -= take
        if remaining and position < repeated_end:
            take = min(remaining, repeated_end - position)
            row_offset = (position - header_end) % len(self.row)
            prefix = self.row[row_offset:]
            repeated = (prefix + self.row * ((take + len(self.row)) // len(self.row)))[:take]
            chunks.append(repeated)
            position += take
            remaining -= take
        if remaining:
            tail_offset = position - repeated_end
            chunks.append(self.tail[tail_offset : tail_offset + remaining])
        return b"".join(chunks)


def fixture_sha256(total_size: int, *, block_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    stream = CsvRange(total_size, 0, total_size)
    while block := stream.read(block_size):
        digest.update(block)
    return digest.hexdigest()


class ApiClient:
    def __init__(self, base_url: str, token: str, origin: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.origin = origin

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        include_origin: bool = False,
    ) -> dict[str, Any]:
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {"Accept": "application/json", "Authorization": f"Bearer {self.token}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        if include_origin:
            headers["Origin"] = self.origin
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=body, headers=headers, method=method
        )
        with urllib.request.urlopen(request, timeout=90) as response:
            return json.loads(response.read())


def put_range(instruction: dict[str, Any], stream: CsvRange, origin: str) -> dict[str, Any]:
    parsed = urllib.parse.urlsplit(instruction["url"])
    connection_type = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_type(parsed.hostname, parsed.port, timeout=180)
    connection.blocksize = 1024 * 1024
    headers = {str(key): str(value) for key, value in instruction["headers"].items()}
    headers["Origin"] = origin
    headers["Content-Length"] = str(instruction["cursor"]["length"])
    target = urllib.parse.urlunsplit(("", "", parsed.path, parsed.query, ""))
    try:
        connection.request(instruction["method"], target, body=stream, headers=headers)
        response = connection.getresponse()
        response.read()
        if response.status >= 300:
            raise RuntimeError(f"Provider transfer failed with HTTP {response.status}.")
        return {
            "etag": response.getheader("ETag"),
            "checksum_sha256": response.getheader("x-amz-checksum-sha256"),
            "provider_headers": {
                name: value
                for name in instruction["expose_response_headers"]
                if (value := response.getheader(name)) is not None
            },
            "payload_host": parsed.netloc,
        }
    finally:
        connection.close()


def upload_fixture(
    *,
    client: ApiClient,
    project_id: str,
    index: int,
    object_bytes: int,
    digest: str,
    data_region: str,
    retry_budget: int,
) -> UploadResult:
    started = time.monotonic()
    dataset_name = f"phase2-systems-fixture-{index:02d}"
    root = f"/projects/{project_id}/datasets/uploads"
    session = client.request(
        root,
        method="POST",
        payload={
            "upload_kind": "dataset",
            "dataset_name": dataset_name,
            "filename": f"{dataset_name}.csv",
            "byte_size": object_bytes,
            "content_type": "text/csv",
            "sha256": digest,
            "sensitivity": "internal",
            "data_region": data_region,
            "tags": {
                "fixture_kind": "systems",
                "statistical_dataset": "false",
                "qualification": "phase-2-ingestion",
            },
        },
        idempotency_key=f"phase2-begin-{index}-{uuid.uuid4()}",
        include_origin=True,
    )
    session_id = session["id"]
    confirmed = 0
    payload_host = ""
    receipts: list[dict[str, Any]] = []
    failures_for_unit = 0
    while confirmed < object_bytes:
        progress = client.request(f"{root}/{session_id}/progress")
        if progress["confirmed_bytes"] < confirmed:
            raise RuntimeError("Provider progress moved backwards.")
        confirmed = progress["confirmed_bytes"]
        receipts = progress["receipts"]
        cursor = progress["next_cursor"]
        if cursor is None:
            break
        try:
            instruction = client.request(
                f"{root}/{session_id}/instructions",
                method="POST",
                payload={"cursor": cursor},
                idempotency_key=(
                    f"phase2-unit-{session_id}-{cursor['unit_number']}-{uuid.uuid4()}"
                ),
                include_origin=True,
            )
            provider = put_range(
                instruction,
                CsvRange(object_bytes, cursor["offset"], cursor["length"]),
                client.origin,
            )
            payload_host = provider.pop("payload_host")
            receipts = [
                *[item for item in receipts if item["unit_number"] != cursor["unit_number"]],
                {**cursor, **provider},
            ]
            failures_for_unit = 0
        except (OSError, RuntimeError, urllib.error.URLError):
            failures_for_unit += 1
            if failures_for_unit > retry_budget:
                raise
            time.sleep(min(2**failures_for_unit, 30))
    final_progress = client.request(f"{root}/{session_id}/progress")
    if final_progress["confirmed_bytes"] != object_bytes or not final_progress["complete"]:
        raise RuntimeError("Provider did not confirm the exact fixture size.")
    completion = client.request(
        f"{root}/{session_id}/complete",
        method="POST",
        payload={"sha256": digest, "receipts": final_progress["receipts"] or receipts},
        idempotency_key=f"phase2-complete-{session_id}",
    )
    status = completion["session"]["status"]
    deadline = time.monotonic() + 3600
    while status not in {"ready", "quarantined", "failed"} and time.monotonic() < deadline:
        time.sleep(2)
        status = client.request(f"{root}/{session_id}")["status"]
    if status != "ready":
        raise RuntimeError(f"Fixture {session_id} ended in {status}.")
    return UploadResult(
        session_id=session_id,
        dataset_name=dataset_name,
        confirmed_bytes=object_bytes,
        payload_host=payload_host,
        status=status,
        elapsed_seconds=round(time.monotonic() - started, 3),
    )


class RssSampler:
    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self.maximum: dict[str, int] = {"api": 0, "ui": 0}
        self.maximum_by_pod: dict[str, int] = {}
        self.stop = threading.Event()

    def sample_until_stopped(self) -> None:
        while not self.stop.wait(1):
            for component in self.maximum:
                pods = subprocess.run(
                    [
                        "kubectl",
                        "-n",
                        self.namespace,
                        "get",
                        "pods",
                        "-l",
                        f"app.kubernetes.io/component={component}",
                        "-o",
                        "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                if pods.returncode != 0:
                    continue
                command = (
                    "for file in /sys/fs/cgroup/memory.current "
                    "/sys/fs/cgroup/memory/memory.usage_in_bytes; "
                    "do test -r $file && cat $file && break; done"
                )
                for pod in pods.stdout.splitlines():
                    result = subprocess.run(
                        [
                            "kubectl",
                            "-n",
                            self.namespace,
                            "exec",
                            pod,
                            "--",
                            "sh",
                            "-c",
                            command,
                        ],
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0 and result.stdout.strip().isdigit():
                        observed = int(result.stdout.strip())
                        self.maximum_by_pod[pod] = max(self.maximum_by_pod.get(pod, 0), observed)
                        self.maximum[component] = max(self.maximum[component], observed)


def rss_within_bound(maximum: dict[str, int], bound_bytes: int) -> bool:
    return bool(maximum) and all(0 < value <= bound_bytes for value in maximum.values())


def aggregate_goodput_bits_per_second(payload_bytes: int, elapsed_seconds: float) -> float:
    if payload_bytes < 0 or elapsed_seconds <= 0:
        raise ValueError("Goodput inputs require non-negative bytes and positive elapsed time.")
    return payload_bytes * 8 / elapsed_seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8080/api/v1")
    parser.add_argument("--origin", default="http://localhost:8080")
    parser.add_argument("--token")
    parser.add_argument("--project-id")
    parser.add_argument("--objects", type=int, default=DEFAULT_OBJECTS)
    parser.add_argument("--object-bytes", type=int, default=DEFAULT_OBJECT_BYTES)
    parser.add_argument("--available-capacity-bytes", type=int)
    parser.add_argument("--preflight-path", type=Path, default=Path.cwd())
    parser.add_argument("--headroom-fraction", type=float, default=DEFAULT_HEADROOM_FRACTION)
    parser.add_argument("--rss-bound-bytes", type=int)
    parser.add_argument("--namespace", default="sceptre")
    parser.add_argument("--retry-budget", type=int, default=5)
    parser.add_argument(
        "--minimum-goodput-bps",
        type=int,
        default=DEFAULT_MINIMUM_GOODPUT_BITS_PER_SECOND,
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    available = args.available_capacity_bytes
    if available is None:
        available = shutil.disk_usage(args.preflight_path).free
    preflight = capacity_preflight(
        object_count=args.objects,
        object_bytes=args.object_bytes,
        available_capacity_bytes=available,
        headroom_fraction=args.headroom_fraction,
    )
    report: dict[str, Any] = {
        "schema_revision": "sceptre-phase2-load-v1",
        "preflight": asdict(preflight),
    }
    if not args.run:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if preflight.passed else 2
    if not preflight.passed:
        raise SystemExit("Capacity preflight failed; refusing to start the qualification workload.")
    if not args.token or not args.project_id:
        raise SystemExit("--token and --project-id are required with --run.")
    if not args.rss_bound_bytes or args.rss_bound_bytes <= 0:
        raise SystemExit("A positive signed --rss-bound-bytes is required with --run.")
    client = ApiClient(args.base_url, args.token, args.origin)
    capabilities = client.request("/capabilities")
    data_region = str(capabilities.get("upload_data_region") or "").strip()
    if not data_region:
        raise SystemExit("The API capabilities response did not resolve an upload data region.")
    digest = fixture_sha256(args.object_bytes)
    sampler = RssSampler(args.namespace)
    sampler_thread = threading.Thread(target=sampler.sample_until_stopped, daemon=True)
    sampler_thread.start()
    started = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.objects) as executor:
            futures = [
                executor.submit(
                    upload_fixture,
                    client=client,
                    project_id=args.project_id,
                    index=index,
                    object_bytes=args.object_bytes,
                    digest=digest,
                    data_region=data_region,
                    retry_budget=args.retry_budget,
                )
                for index in range(1, args.objects + 1)
            ]
            uploads = [future.result() for future in futures]
    finally:
        sampler.stop.set()
        sampler_thread.join(timeout=10)
    payload_by_host: dict[str, int] = {}
    for upload in uploads:
        payload_by_host[upload.payload_host] = (
            payload_by_host.get(upload.payload_host, 0) + upload.confirmed_bytes
        )
    api_host = urllib.parse.urlsplit(args.base_url).netloc
    ui_host = urllib.parse.urlsplit(args.origin).netloc
    proxied_payload_bytes = sum(
        size for host, size in payload_by_host.items() if host in {api_host, ui_host}
    )
    elapsed_seconds = time.monotonic() - started
    aggregate_goodput = aggregate_goodput_bits_per_second(
        preflight.payload_bytes, elapsed_seconds
    )
    report.update(
        {
            "fixture_sha256": digest,
            "fixture_kind": "systems",
            "statistical_dataset": False,
            "data_region": data_region,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "aggregate_goodput_bits_per_second": round(aggregate_goodput, 3),
            "minimum_goodput_bits_per_second": args.minimum_goodput_bps,
            "uploads": [asdict(upload) for upload in uploads],
            "payload_bytes_by_host": payload_by_host,
            "payload_bytes_proxied_through_api_or_ui": proxied_payload_bytes,
            "peak_rss_bytes": sampler.maximum,
            "peak_rss_bytes_by_pod": sampler.maximum_by_pod,
            "rss_bound_bytes": args.rss_bound_bytes,
            "passed": (
                len(uploads) == args.objects
                and all(upload.confirmed_bytes == args.object_bytes for upload in uploads)
                and proxied_payload_bytes == 0
                and aggregate_goodput >= args.minimum_goodput_bps
                and rss_within_bound(sampler.maximum, args.rss_bound_bytes)
            ),
        }
    )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
