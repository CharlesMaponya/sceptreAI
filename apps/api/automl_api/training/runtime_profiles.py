"""Check the complete inventories of the reviewed API and CPU image pair."""

from __future__ import annotations

import hashlib
import json
import platform
import re
from functools import lru_cache
from pathlib import Path

PYTHON_VERSION = "3.12.13"
PIP_VERSION = "26.1.2"
_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _package_name(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def locked_packages(content):
    packages = {}
    current = None
    hashes = 0
    for line in content.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        pin = re.fullmatch(r"([\w.-]+)==([^\s\\]+)(?: \\)?", line)
        if pin:
            if current is not None and not hashes:
                raise ValueError("Runtime lock entry has no wheel hashes.")
            current = _package_name(pin[1])
            if current in packages:
                raise ValueError("Duplicate runtime lock entry.")
            packages[current] = pin[2]
            hashes = 0
        elif current is not None and re.fullmatch(r"\s+--hash=sha256:[a-f0-9]{64}(?: \\)?", line):
            hashes += 1
        else:
            raise ValueError("Runtime locks must contain hashed exact pins only.")
    if not packages or not hashes:
        raise ValueError("Runtime lock is empty or has an unhashed entry.")
    return packages


@lru_cache
def reviewed_profiles():
    root = Path(__file__).resolve().parents[4]
    directory = root / "infra" / "locks"
    sources = {
        name: (directory / f"requirements-{name}.lock").read_bytes()
        for name in ("api", "training-cpu")
    }
    api = locked_packages(sources["api"].decode())
    cpu = locked_packages(sources["training-cpu"].decode())
    if any(api[name] != cpu[name] for name in api.keys() & cpu.keys()):
        raise ValueError("API and CPU runtime locks disagree on a shared dependency.")
    application = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for directory in (root / "apps/api", root / "packages")
        for path in directory.rglob("*.py")
    }
    return {
        "scope": "reviewed-api-cpu-lock-pair-v1",
        "python": PYTHON_VERSION,
        "policy_source_sha256": _SOURCE_SHA256,
        "application_source_scope": "python-source-bundle-v1",
        "application_source_digest": hashlib.sha256(
            json.dumps(application, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "lock_sha256": {
            name: hashlib.sha256(content).hexdigest() for name, content in sources.items()
        },
        "profiles": {
            "api": {**api, "pip": PIP_VERSION},
            "cpu": {**api, **cpu, "pip": PIP_VERSION},
        },
    }


def runtime_identity(inventory, *, profile, environment):
    if not profile:
        if environment.strip().lower() in {"production", "staging"}:
            raise ValueError("A reviewed runtime profile is required in production/staging.")
        return inventory
    reviewed = reviewed_profiles()
    if profile not in reviewed["profiles"]:
        raise ValueError("Unknown reviewed runtime profile.")
    actual = [(_package_name(name), version) for name, version in inventory["packages"]]
    expected = reviewed["profiles"][profile]
    if (
        inventory["python"] != reviewed["python"]
        or inventory.get("package_scope") != "interpreter-site-packages-v1"
        or len(actual) != len(set(actual))
        or set(actual) != set(expected.items())
    ):
        raise ValueError("Installed runtime inventory differs from its reviewed image lock.")
    # Both images bind every reviewed inventory; the executing image must match its own.
    return {**reviewed, "system": platform.system(), "architecture": platform.machine()}
