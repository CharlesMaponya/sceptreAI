from __future__ import annotations

import copy
from pathlib import Path

import yaml

from scripts.validate_phase2_evidence import (
    DEFAULT_MANIFEST,
    DEFAULT_RECORDS,
    ROOT,
    _validate_candidate_position,
    validate_phase2_evidence,
)


def _records() -> dict[str, object]:
    return yaml.safe_load(DEFAULT_RECORDS.read_text(encoding="utf-8"))


def _write_records(tmp_path: Path, value: dict[str, object]) -> Path:
    path = tmp_path / "gate-records.yaml"
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    return path


def test_checked_in_phase2_gate_records_are_candidate_bound_and_valid() -> None:
    assert validate_phase2_evidence() == []


def test_phase2_gate_records_reject_duplicate_and_missing_gate_ids(tmp_path: Path) -> None:
    value = _records()
    records = value["records"]
    assert isinstance(records, list)
    records[-1]["gate_id"] = records[0]["gate_id"]
    errors = validate_phase2_evidence(
        _write_records(tmp_path, value), manifest_path=DEFAULT_MANIFEST, root=ROOT
    )
    assert "Gate IDs must be unique." in errors
    assert any("must contain exactly" in error for error in errors)


def test_phase2_gate_records_reject_digest_drift(tmp_path: Path) -> None:
    value = _records()
    records = value["records"]
    assert isinstance(records, list)
    records[0]["evidence"] = copy.deepcopy(records[0]["evidence"])
    records[0]["evidence"]["files"][0]["digest"] = "sha256:" + "0" * 64
    errors = validate_phase2_evidence(
        _write_records(tmp_path, value), manifest_path=DEFAULT_MANIFEST, root=ROOT
    )
    assert any("P2-G01.evidence.files[0] digest" in error for error in errors)


def test_passed_gate_requires_reviewer_approval_and_immutable_receipt(tmp_path: Path) -> None:
    value = _records()
    records = value["records"]
    assert isinstance(records, list)
    record = records[0]
    record["status"] = "passed"
    record["expected_failures"] = []
    errors = validate_phase2_evidence(
        _write_records(tmp_path, value), manifest_path=DEFAULT_MANIFEST, root=ROOT
    )
    assert any("independent reviewer and approval" in error for error in errors)
    assert any("immutable evidence sink receipt" in error for error in errors)


def test_phase2_gate_records_require_the_sceptre_namespace(tmp_path: Path) -> None:
    value = _records()
    records = value["records"]
    assert isinstance(records, list)
    records[0]["environment"] = copy.deepcopy(records[0]["environment"])
    records[0]["environment"]["namespace"] = "default"
    errors = validate_phase2_evidence(
        _write_records(tmp_path, value), manifest_path=DEFAULT_MANIFEST, root=ROOT
    )
    assert "P2-G01.environment.namespace must be sceptre." in errors


def test_candidate_position_accepts_only_an_evidence_attestation_commit(
    monkeypatch,
) -> None:
    class Result:
        returncode = 0

    monkeypatch.setattr(
        "scripts.validate_phase2_evidence.subprocess.run", lambda *a, **k: Result()
    )
    monkeypatch.setattr(
        "scripts.validate_phase2_evidence._git_output",
        lambda *_args: b"docs/production-readiness/evidence/phase-2/gate-records.yaml\n",
    )
    assert _validate_candidate_position(root=ROOT, candidate="candidate", head="head") == []


def test_candidate_position_rejects_code_changes_after_candidate(monkeypatch) -> None:
    class Result:
        returncode = 0

    monkeypatch.setattr(
        "scripts.validate_phase2_evidence.subprocess.run", lambda *a, **k: Result()
    )
    monkeypatch.setattr(
        "scripts.validate_phase2_evidence._git_output",
        lambda *_args: b"apps/api/automl_api/main.py\n",
    )
    errors = _validate_candidate_position(root=ROOT, candidate="candidate", head="head")
    assert errors == [
        "Non-evidence files changed after the candidate Git commit: "
        "apps/api/automl_api/main.py"
    ]
