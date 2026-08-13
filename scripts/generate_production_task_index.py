from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GUIDE = ROOT / "docs" / "production-readiness" / "implementation-guide.md"
DEFAULT_OUTPUT = ROOT / "docs" / "production-readiness" / "task-index.yaml"
PHASE_PATTERN = re.compile(r"^## Phase (?P<phase>\d+A?): (?P<title>.+)$")
HEADING_PATTERN = re.compile(r"^(?P<marks>#{1,6})\s+(?P<title>.+)$")
LIST_ITEM_PATTERN = re.compile(r"^(?:- |\d+\. )(?P<text>.+)$")


@dataclass(frozen=True)
class IndexedBullet:
    phase: str
    kind: str
    phase_title: str
    section_title: str
    text: str
    fingerprint: str
    prerequisites: tuple[str, ...]
    owner: str | None


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _normalize(lines: list[str]) -> str:
    parts = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        stripped = re.sub(r"^(?:- |\d+\. )", "", stripped)
        parts.append(stripped)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _metadata_value(lines: list[str], start: int, label: str) -> str | None:
    prefix = f"**{label}:**"
    line = lines[start]
    if not line.startswith(prefix):
        return None
    values = [line.removeprefix(prefix).strip()]
    cursor = start + 1
    while cursor < len(lines):
        candidate = lines[cursor]
        if not candidate.strip() or candidate.startswith("**") or candidate.startswith("#"):
            break
        values.append(candidate.strip())
        cursor += 1
    return _normalize(values)


def _section_kind(title: str, current: str | None) -> str | None:
    normalized = title.casefold()
    if normalized == "purpose" or normalized.startswith("rollback"):
        return None
    if normalized in {"gate", "approval conditions"}:
        return "gate"
    if normalized in {"work", "local work", "provision", "qualification fixture matrix"}:
        return "work"
    return current


def parse_guide(path: Path) -> list[IndexedBullet]:
    lines = path.read_text().splitlines()
    phase: str | None = None
    phase_title = ""
    section_title = ""
    kind: str | None = None
    prerequisites: tuple[str, ...] = ()
    owner: str | None = None
    bullets: list[IndexedBullet] = []
    cursor = 0

    while cursor < len(lines):
        line = lines[cursor]
        phase_match = PHASE_PATTERN.match(line)
        if phase_match:
            phase = phase_match.group("phase")
            phase_title = phase_match.group("title")
            section_title = ""
            kind = None
            prerequisites = ()
            owner = None
            cursor += 1
            continue

        if phase is None:
            cursor += 1
            continue

        depends_on = _metadata_value(lines, cursor, "Depends on")
        if depends_on is not None:
            prerequisites = (depends_on,)
            cursor += 1
            continue
        primary_owners = _metadata_value(lines, cursor, "Primary owners")
        if primary_owners is not None:
            owner = primary_owners
            cursor += 1
            continue

        heading_match = HEADING_PATTERN.match(line)
        if heading_match:
            if len(heading_match.group("marks")) == 2:
                phase = None
                kind = None
            elif len(heading_match.group("marks")) == 3:
                section_title = heading_match.group("title")
                kind = _section_kind(section_title, kind)
            cursor += 1
            continue

        item_match = LIST_ITEM_PATTERN.match(line)
        if item_match and kind is not None:
            item_lines = [line]
            lookahead = cursor + 1
            fenced = False
            while lookahead < len(lines):
                candidate = lines[lookahead]
                if candidate.startswith("```"):
                    fenced = not fenced
                if not fenced and (
                    HEADING_PATTERN.match(candidate) or LIST_ITEM_PATTERN.match(candidate)
                ):
                    break
                item_lines.append(candidate)
                lookahead += 1
            text = _normalize(item_lines)
            fingerprint_input = "\0".join((phase, kind, section_title, text))
            fingerprint = _sha256_bytes(fingerprint_input.encode())
            bullets.append(
                IndexedBullet(
                    phase=phase,
                    kind=kind,
                    phase_title=phase_title,
                    section_title=section_title,
                    text=text,
                    fingerprint=fingerprint,
                    prerequisites=prerequisites,
                    owner=owner,
                )
            )
            cursor = lookahead
            continue

        cursor += 1

    return bullets


def _git_sha() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _assign_ids(
    bullets: list[IndexedBullet], existing: dict[str, Any] | None
) -> list[dict[str, Any]]:
    existing_by_fingerprint = {
        entry["fingerprint"]: entry
        for entry in (existing or {}).get("entries", [])
    }
    counters: dict[tuple[str, str], int] = {}
    for entry in (existing or {}).get("entries", []):
        match = re.fullmatch(r"P(\d+A?)-(W|G)(\d+)", entry["id"])
        if match:
            key = (match.group(1), "work" if match.group(2) == "W" else "gate")
            counters[key] = max(counters.get(key, 0), int(match.group(3)))

    entries = []
    for bullet in bullets:
        prior = existing_by_fingerprint.get(bullet.fingerprint)
        if prior is not None:
            entry_id = prior["id"]
            issue_url = prior.get("issue_url")
            supersedes = prior.get("supersedes", [])
            reviewer = prior.get("reviewer")
        else:
            key = (bullet.phase, bullet.kind)
            counters[key] = counters.get(key, 0) + 1
            marker = "W" if bullet.kind == "work" else "G"
            entry_id = f"P{bullet.phase}-{marker}{counters[key]:02d}"
            issue_url = None
            supersedes = []
            reviewer = None
        phase_evidence = (
            {
                "repository_paths": [
                    "docs/production-readiness/decision-register.yaml",
                    "infra/k3d/",
                    "packages/automl_shared/",
                    "scripts/",
                ],
                "tests": [
                    "scripts/test_backend.sh",
                    "npm run test:coverage --prefix apps/ui/react_app",
                    "scripts/validate_phase0a_schedule.py",
                    "scripts/validate_phase0a_tls.py",
                ],
                "evidence_outputs": [
                    "docs/production-readiness/evidence/phase-0a/phase-0a-summary-2026-08-13.yaml",
                    "docs/production-readiness/evidence/phase-0a/quality-gates-2026-08-13.json",
                    "docs/production-readiness/evidence/phase-0a/go-no-go-2026-08-13.yaml",
                ],
                "rollback": (
                    "Destroy disposable spike resources and retain only non-secret evidence."
                ),
                "requalification_triggers": [
                    "Python, Ray, PyArrow, Polars, searcher, estimator, feature-execution, "
                    "cluster-topology, node-family, or qualification-target change"
                ],
            }
            if bullet.phase == "0A"
            else {
                "repository_paths": [],
                "tests": [],
                "evidence_outputs": [],
                "rollback": None,
                "requalification_triggers": [],
            }
        )
        entries.append(
            {
                "id": entry_id,
                "phase": bullet.phase,
                "kind": bullet.kind,
                "heading_path": [
                    f"Phase {bullet.phase}: {bullet.phase_title}",
                    bullet.section_title,
                ],
                "text": bullet.text,
                "fingerprint": bullet.fingerprint,
                "guide_commit": _git_sha(),
                "prerequisites": list(bullet.prerequisites),
                "evidence_schema_revision": (
                    "sceptre-gate-record-v1"
                    if bullet.kind == "gate"
                    else "sceptre-work-record-v1"
                ),
                "repository_paths": phase_evidence["repository_paths"],
                "tests": phase_evidence["tests"],
                "evidence_outputs": phase_evidence["evidence_outputs"],
                "owner": bullet.owner,
                "reviewer": reviewer,
                "issue_url": issue_url,
                "rollback": phase_evidence["rollback"],
                "requalification_triggers": phase_evidence["requalification_triggers"],
                "supersedes": supersedes,
            }
        )
    return entries


def build_index(guide: Path, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    guide_bytes = guide.read_bytes()
    bullets = parse_guide(guide)
    entries = _assign_ids(bullets, existing)
    return {
        "schema_revision": "sceptre-production-task-index-v1",
        "status": "draft",
        "guide": {
            "path": str(guide.relative_to(ROOT)),
            "commit": _git_sha(),
            "sha256": _sha256_bytes(guide_bytes),
        },
        "entry_count": len(entries),
        "entries": entries,
    }


def _load(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"Task index is not a mapping: {path}")
    return value


def _validate(index: dict[str, Any]) -> None:
    entries = index["entries"]
    ids = [entry["id"] for entry in entries]
    fingerprints = [entry["fingerprint"] for entry in entries]
    if len(ids) != len(set(ids)):
        raise ValueError("Task index contains duplicate IDs")
    if len(fingerprints) != len(set(fingerprints)):
        raise ValueError("Task index contains duplicate bullet fingerprints")
    if index["entry_count"] != len(entries):
        raise ValueError("Task index entry_count does not match entries")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guide", type=Path, default=DEFAULT_GUIDE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    guide = arguments.guide.resolve()
    output = arguments.output.resolve()
    existing = _load(output)
    generated = build_index(guide, existing)
    _validate(generated)

    if arguments.check:
        if existing != generated:
            raise SystemExit("Task index is stale; regenerate it and review the mapping.")
        print(f"validated {generated['entry_count']} immutable task IDs")
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml.safe_dump(generated, sort_keys=False, width=100))
    print(f"wrote {generated['entry_count']} task IDs to {output.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
