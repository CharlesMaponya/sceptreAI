"""sceptrectl local-runtime interface contract tests (task-index P9-W01/W02)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCEPTRECTL = ROOT / "scripts" / "sceptrectl"
PINS_FILE = ROOT / "infra" / "local-versions.yaml"

SUBCOMMANDS = {
    "preflight",
    "create",
    "load-images",
    "install",
    "verify",
    "upgrade",
    "destroy",
}


def test_sceptrectl_exists_and_is_executable() -> None:
    assert SCEPTRECTL.is_file()
    import os

    assert os.access(SCEPTRECTL, os.X_OK)


def test_help_lists_every_required_subcommand() -> None:
    result = subprocess.run(
        [str(SCEPTRECTL), "--help"],
        capture_output=True,
        text=True,
        check=True,
    )

    for command in SUBCOMMANDS:
        assert command in result.stdout


@pytest.mark.parametrize("command", sorted(SUBCOMMANDS))
def test_script_defines_each_subcommand_handler(command: str) -> None:
    source = SCEPTRECTL.read_text(encoding="utf-8")

    assert f"cmd_{command}()" in source
    assert f"{command}) cmd_{command}" in source


def test_pins_file_declares_runtime_versions() -> None:
    import yaml

    pins = yaml.safe_load(PINS_FILE.read_text(encoding="utf-8"))

    for key in ("kubernetes", "k3d", "kubectl", "helm", "kind", "minikube"):
        assert pins.get(key), f"missing pinned version: {key}"
    # Three-node topology is required across local runtimes.
    assert pins["nodes"]["servers"] >= 1
    assert pins["nodes"]["agents"] >= 2


def test_unknown_command_fails_closed() -> None:
    result = subprocess.run(
        [str(SCEPTRECTL), "bogus-command"],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "unknown argument" in result.stderr


def test_preflight_runs_green_on_this_host() -> None:
    result = subprocess.run(
        [str(SCEPTRECTL), "--runtime", "k3d", "preflight"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        pytest.skip(f"host lacks k3d toolchain: {result.stderr.strip()}")
    assert "preflight OK" in result.stdout


@pytest.mark.parametrize("mismatch", [None, "helm", "kubectl", "k3d"])
def test_preflight_rejects_tool_version_mismatches(tmp_path, mismatch) -> None:
    import yaml

    pins = yaml.safe_load(PINS_FILE.read_text())
    for name in ("helm", "kubectl", "k3d", "docker"):
        version = "0.0.0" if name == mismatch else pins.get(name, "1.0.0")
        command = tmp_path / name
        command.write_text(f"#!/bin/sh\nprintf 'v{version}\\n'\n")
        command.chmod(0o755)
    result = subprocess.run(
        [str(SCEPTRECTL), "--runtime", "k3d", "preflight"],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
    )
    if mismatch:
        assert result.returncode != 0
        assert f"{mismatch} 0.0.0 does not match pinned" in result.stderr
        assert "preflight OK" not in result.stdout
    else:
        assert result.returncode == 0, result.stderr
        assert "preflight OK" in result.stdout


@pytest.mark.parametrize("command", ["install", "upgrade"])
def test_release_operations_wait_for_migrations(tmp_path, command) -> None:
    calls = tmp_path / "helm-args"
    helm = tmp_path / "helm"
    helm.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$SCEPTRE_TEST_HELM_ARGS"\n')
    kubectl = tmp_path / "kubectl"
    kubectl.write_text("#!/bin/sh\nexit 0\n")
    for executable in (helm, kubectl):
        executable.chmod(0o755)
    result = subprocess.run(
        [str(SCEPTRECTL), "--runtime", "k3d", command],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SCEPTRE_TEST_HELM_ARGS": str(calls),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "--wait-for-jobs" in calls.read_text().splitlines()
