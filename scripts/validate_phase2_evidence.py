#!/usr/bin/env python3
"""Validate candidate-bound Phase 2 gate records and their evidence digests."""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RECORDS = (
    ROOT
    / "docs"
    / "production-readiness"
    / "evidence"
    / "phase-2"
    / "gate-records-2026-08-22.yaml"
)
DEFAULT_MANIFEST = DEFAULT_RECORDS.with_name("evidence-manifest-2026-08-22.yaml")
EXPECTED_GATES = {f"P2-G{index:02d}" for index in range(1, 8)}
EVIDENCE_PREFIX = "docs/production-readiness/evidence/phase-2/"
ALLOWED_STATUSES = {"not_started", "running", "passed", "failed", "expired", "waived"}
SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
REQUIRED_ARTIFACTS = {
    "git_sha",
    "baseline_revision",
    "artifact_manifest",
    "qualification_attestation",
    "release_channel_pointer",
    "channel_publication_receipt",
    "chart",
    "images",
    "migration_range",
    "dependency_lock",
    "catalog_revision",
    "feature_revisions",
    "split_revision",
    "ray_runtime_revision",
}
REQUIRED_RECORD_FIELDS = {
    "gate_id",
    "title",
    "owner",
    "reviewer",
    "status",
    "prerequisites",
    "environment",
    "artifacts",
    "not_applicable_reasons",
    "procedure",
    "thresholds",
    "expected_failures",
    "observed_results",
    "evidence",
    "rollback_procedure",
    "requalification_triggers",
    "started_at",
    "completed_at",
    "expires_at",
    "approvals",
    "waiver",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _git_output(root: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise ValueError(result.stderr.decode(errors="replace").strip())
    return result.stdout


def _candidate_identities(root: Path, revision: str) -> dict[str, str]:
    source_archive = hashlib.sha256(_git_output(root, "archive", revision)).hexdigest()
    chart_archive = hashlib.sha256(
        _git_output(root, "archive", revision, "infra/helm/sceptre")
    ).hexdigest()

    def blob(path: str) -> bytes:
        return _git_output(root, "show", f"{revision}:{path}")

    dependency = hashlib.sha256()
    for index, path in enumerate(
        (
        "pyproject.toml",
        "requirements-training.txt",
        "apps/ui/react_app/package-lock.json",
        )
    ):
        if index:
            dependency.update(b"\0")
        dependency.update(path.encode())
        dependency.update(b"\0")
        dependency.update(blob(path))
    fixture = hashlib.sha256()
    for path in (
        "scripts/benchmark_phase2_ingestion.py",
        "tests/test_phase2_load_harness.py",
        "tests/test_phase2_upload_drivers.py",
        "tests/test_phase2_upload_policy.py",
        "tests/test_phase2_upload_services.py",
        "apps/ui/react_app/e2e/phase2-ingestion.spec.ts",
    ):
        fixture.update(path.encode())
        fixture.update(b"\0")
        fixture.update(blob(path))
        fixture.update(b"\0")
    ray_runtime = hashlib.sha256()
    ray_runtime.update(b"requirements-training.txt\0")
    ray_runtime.update(blob("requirements-training.txt"))
    return {
        "source_archive_sha256": source_archive,
        "chart_archive_sha256": chart_archive,
        "dependency_lock_sha256": dependency.hexdigest(),
        "fixture_manifest_sha256": fixture.hexdigest(),
        "ray_runtime_sha256": ray_runtime.hexdigest(),
    }


def _validate_candidate_position(*, root: Path, candidate: str, head: str) -> list[str]:
    """Require HEAD to be the candidate or a strictly evidence-only descendant.

    An evidence record cannot contain the identity of the commit that contains
    that record: changing the embedded identity changes the commit again.  The
    release therefore uses two commits.  The first is the immutable candidate;
    the second may add or refresh only Phase 2 evidence.  Any code, test,
    workflow, guide, or task-index change after the candidate invalidates the
    binding.
    """
    if head == candidate:
        return []
    ancestry = subprocess.run(
        ["git", "merge-base", "--is-ancestor", candidate, head],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if ancestry.returncode != 0:
        return ["The candidate Git commit is not an ancestor of the current commit."]
    changed = {
        path
        for path in _git_output(root, "diff", "--name-only", f"{candidate}..{head}")
        .decode(errors="replace")
        .splitlines()
        if path
    }
    unexpected = sorted(path for path in changed if not path.startswith(EVIDENCE_PREFIX))
    if unexpected:
        return [
            "Non-evidence files changed after the candidate Git commit: "
            + ", ".join(unexpected)
        ]
    return []


def _load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one YAML mapping.")
    return value


def _path_inside_root(root: Path, value: object) -> Path | None:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        return None
    candidate = (root / value).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate


def _validate_file_digest(
    *, root: Path, entry: Mapping[str, Any], prefix: str, errors: list[str]
) -> None:
    path = _path_inside_root(root, entry.get("path"))
    if path is None:
        errors.append(f"{prefix}.path must be a repository-relative path.")
        return
    if not path.is_file():
        errors.append(f"{prefix}.path does not exist: {entry.get('path')}")
        return
    claimed = entry.get("digest", entry.get("sha256"))
    if isinstance(claimed, str) and claimed.startswith("sha256:"):
        claimed = claimed.removeprefix("sha256:")
    if claimed != _sha256(path):
        errors.append(f"{prefix} digest does not match {entry.get('path')}.")


def _all_null(value: object) -> bool:
    return value is None or (
        isinstance(value, Mapping)
        and bool(value)
        and all(_all_null(item) for item in value.values())
    )


def _validate_artifacts(record: Mapping[str, Any], prefix: str, errors: list[str]) -> None:
    artifacts = record.get("artifacts")
    reasons = record.get("not_applicable_reasons")
    if not isinstance(artifacts, Mapping):
        errors.append(f"{prefix}.artifacts must be a mapping.")
        return
    missing = REQUIRED_ARTIFACTS - artifacts.keys()
    if missing:
        errors.append(f"{prefix}.artifacts is missing {sorted(missing)}.")
    if not isinstance(reasons, Mapping):
        errors.append(f"{prefix}.not_applicable_reasons must be a mapping.")
        reasons = {}
    for key, value in artifacts.items():
        if _all_null(value) and not reasons.get(key):
            errors.append(f"{prefix}.artifacts.{key} requires a not-applicable reason.")
    for key in ("git_sha", "baseline_revision", "chart", "dependency_lock", "ray_runtime_revision"):
        value = artifacts.get(key)
        if value is not None and (not isinstance(value, str) or not SHA256.fullmatch(value)):
            errors.append(f"{prefix}.artifacts.{key} must be a sha256 identity.")


def _validate_record(
    record: Mapping[str, Any],
    *,
    root: Path,
    manifest_digest: str,
    candidate: str,
    identities: Mapping[str, str],
) -> list[str]:
    gate_id = str(record.get("gate_id") or "unknown")
    prefix = gate_id
    errors: list[str] = []
    missing = REQUIRED_RECORD_FIELDS - record.keys()
    if missing:
        errors.append(f"{prefix} is missing {sorted(missing)}.")
    status = record.get("status")
    if status not in ALLOWED_STATUSES:
        errors.append(f"{prefix}.status is not allowed: {status!r}.")
    prerequisites = record.get("prerequisites")
    if not isinstance(prerequisites, list) or any(
        not isinstance(value, str) or not re.fullmatch(r"P\d+-G\d{2}", value)
        for value in prerequisites
    ):
        errors.append(f"{prefix}.prerequisites must contain stable gate IDs.")
    environment = record.get("environment")
    if not isinstance(environment, Mapping) or not all(
        environment.get(key) for key in ("profile", "provider", "region")
    ):
        errors.append(f"{prefix}.environment is incomplete.")
    elif environment.get("namespace") != "sceptre":
        errors.append(f"{prefix}.environment.namespace must be sceptre.")
    _validate_artifacts(record, prefix, errors)
    artifacts = record.get("artifacts")
    if isinstance(artifacts, Mapping):
        expected_artifacts = {
            "git_sha": identities.get("source_archive_sha256"),
            "chart": identities.get("chart_archive_sha256"),
            "dependency_lock": identities.get("dependency_lock_sha256"),
            "ray_runtime_revision": identities.get("ray_runtime_sha256"),
        }
        for key, digest in expected_artifacts.items():
            if artifacts.get(key) != f"sha256:{digest}":
                errors.append(f"{prefix}.artifacts.{key} does not bind the candidate.")
    procedure = record.get("procedure")
    if not isinstance(procedure, Mapping) or not all(
        key in procedure
        for key in (
            "command_or_workflow",
            "fixture_manifest",
            "workload_parameters",
            "capacity_profile",
        )
    ):
        errors.append(f"{prefix}.procedure is incomplete.")
    elif not SHA256.fullmatch(str(procedure.get("fixture_manifest") or "")):
        errors.append(f"{prefix}.procedure.fixture_manifest must be a sha256 identity.")
    elif procedure.get("fixture_manifest") != (
        f"sha256:{identities.get('fixture_manifest_sha256')}"
    ):
        errors.append(f"{prefix}.procedure.fixture_manifest does not bind the candidate.")
    if not isinstance(record.get("thresholds"), Mapping) or not record["thresholds"]:
        errors.append(f"{prefix}.thresholds must be a nonempty mapping.")
    if not isinstance(record.get("expected_failures"), list):
        errors.append(f"{prefix}.expected_failures must be a list.")
    observed = record.get("observed_results")
    if not isinstance(observed, Mapping) or not observed:
        errors.append(f"{prefix}.observed_results must be a nonempty mapping.")
    elif observed.get("candidate_git_commit") != candidate:
        errors.append(f"{prefix} does not bind the candidate Git commit.")
    evidence = record.get("evidence")
    if not isinstance(evidence, Mapping):
        errors.append(f"{prefix}.evidence must be a mapping.")
    else:
        if evidence.get("manifest") != f"sha256:{manifest_digest}":
            errors.append(f"{prefix}.evidence.manifest has the wrong digest.")
        files = evidence.get("files")
        if not isinstance(files, list) or not files:
            errors.append(f"{prefix}.evidence.files must be nonempty.")
        else:
            for index, entry in enumerate(files):
                if not isinstance(entry, Mapping):
                    errors.append(f"{prefix}.evidence.files[{index}] must be a mapping.")
                    continue
                _validate_file_digest(
                    root=root,
                    entry=entry,
                    prefix=f"{prefix}.evidence.files[{index}]",
                    errors=errors,
                )
    if not record.get("rollback_procedure") or not record.get("requalification_triggers"):
        errors.append(f"{prefix} must define rollback and requalification triggers.")
    waiver = record.get("waiver")
    if not isinstance(waiver, Mapping) or "allowed" not in waiver:
        errors.append(f"{prefix}.waiver is incomplete.")
    if status in {"failed", "expired"} and record.get("completed_at") is None:
        errors.append(f"{prefix}.{status} requires completed_at.")
    if status == "expired" and record.get("expires_at") is None:
        errors.append(f"{prefix}.expired requires expires_at.")
    if status == "not_started" and any(
        record.get(key) is not None for key in ("started_at", "completed_at")
    ):
        errors.append(f"{prefix}.not_started cannot have start or completion timestamps.")
    if status == "passed":
        if record.get("reviewer") in {None, "unassigned"} or not record.get("approvals"):
            errors.append(f"{prefix}.passed requires an independent reviewer and approval.")
        if not isinstance(evidence, Mapping) or not evidence.get("immutable_sink_receipt"):
            errors.append(f"{prefix}.passed requires an immutable evidence sink receipt.")
        if record.get("expected_failures"):
            errors.append(f"{prefix}.passed cannot retain expected failures.")
    return errors


def validate_phase2_evidence(
    records_path: Path = DEFAULT_RECORDS,
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
    root: Path = ROOT,
) -> list[str]:
    errors: list[str] = []
    records_document = _load_yaml(records_path)
    manifest = _load_yaml(manifest_path)
    if records_document.get("schema_revision") != "sceptre-gate-record-v1":
        errors.append("Gate-record schema revision is not sceptre-gate-record-v1.")
    if manifest.get("schema_revision") != "sceptre-phase-2-evidence-manifest-v1":
        errors.append("Phase 2 evidence-manifest schema revision is invalid.")
    for index, entry in enumerate(manifest.get("files") or []):
        if not isinstance(entry, Mapping):
            errors.append(f"manifest.files[{index}] must be a mapping.")
            continue
        _validate_file_digest(
            root=root,
            entry=entry,
            prefix=f"manifest.files[{index}]",
            errors=errors,
        )
    for key in ("guide", "baseline"):
        entry = manifest.get(key)
        if not isinstance(entry, Mapping):
            errors.append(f"manifest.{key} must be a mapping.")
            continue
        _validate_file_digest(root=root, entry=entry, prefix=f"manifest.{key}", errors=errors)
    candidate = records_document.get("candidate_git_commit")
    if candidate != (manifest.get("candidate") or {}).get("git_commit"):
        errors.append("Gate records and evidence manifest bind different candidates.")
    try:
        head = _git_output(root, "rev-parse", "HEAD").decode().strip()
        identities = _candidate_identities(root, str(candidate))
    except ValueError:
        errors.append("The candidate Git commit could not be resolved.")
        head = ""
        identities = {}
    if head:
        errors.extend(
            _validate_candidate_position(root=root, candidate=str(candidate), head=head)
        )
    manifest_candidate = manifest.get("candidate")
    if isinstance(manifest_candidate, Mapping):
        for key, digest in identities.items():
            if manifest_candidate.get(key) != digest:
                errors.append(f"Evidence manifest candidate identity is stale: {key}.")
    records = records_document.get("records")
    if not isinstance(records, list):
        return [*errors, "Gate records must be a list."]
    gate_ids = [record.get("gate_id") for record in records if isinstance(record, Mapping)]
    if len(gate_ids) != len(set(gate_ids)):
        errors.append("Gate IDs must be unique.")
    if set(gate_ids) != EXPECTED_GATES:
        errors.append(f"Gate records must contain exactly {sorted(EXPECTED_GATES)}.")
    manifest_digest = _sha256(manifest_path)
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            errors.append(f"records[{index}] must be a mapping.")
            continue
        errors.extend(
            _validate_record(
                record,
                root=root,
                manifest_digest=manifest_digest,
                candidate=str(candidate),
                identities=identities,
            )
        )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("records", nargs="?", type=Path, default=DEFAULT_RECORDS)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    errors = validate_phase2_evidence(args.records, manifest_path=args.manifest)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("validated 7 candidate-bound Phase 2 gate records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
