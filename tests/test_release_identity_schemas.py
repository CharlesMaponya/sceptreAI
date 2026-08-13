from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "docs" / "production-readiness" / "schemas"
DIGEST = "sha256:" + "a" * 64
GIT_SHA = "b" * 40


def _schema(name: str) -> dict:
    return json.loads((SCHEMA_DIR / name).read_text())


def _validator(name: str) -> Draft202012Validator:
    schema = _schema(name)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def test_artifact_manifest_is_immutable_build_identity_not_qualification_state() -> None:
    manifest = {
        "schema_revision": "sceptre-artifact-manifest-v1",
        "artifact_version": "0.2.0",
        "python_distribution_version": "0.2.0",
        "git_sha": GIT_SHA,
        "chart": {"name": "sceptre", "version": "0.2.0", "digest": DIGEST},
        "images": {"api": {"digest": DIGEST, "platforms": ["linux/amd64", "linux/arm64"]}},
        "python_abi": "cp312",
        "dependency_lock_digest": DIGEST,
        "runtime": {
            "python": "3.12.13",
            "ray": "2.56.1",
            "pyarrow": "24.0.0",
            "polars": "1.43.2",
            "sklearn": "1.8.0",
        },
        "catalog_revision": DIGEST,
        "feature_revisions": {
            "contract_schema": DIGEST,
            "registry_schema": DIGEST,
            "recipe_schema": DIGEST,
        },
        "platform_compatibility_contract_revision": DIGEST,
        "qualification_suggestion_log_revision": DIGEST,
        "qualification_workload_revision": DIGEST,
        "migration_range": {"from": "0001_initial", "to": "0004_resumable_dataset_uploads"},
    }
    validator = _validator("artifact-manifest.schema.json")

    validator.validate(manifest)
    with pytest.raises(ValidationError):
        validator.validate({**manifest, "qualification_label": "0.2.0-rc.1"})


def test_attestation_binds_artifact_provider_evidence_and_release_decision() -> None:
    provider = {
        "platform_bom_digest": DIGEST,
        "capacity_profile_digest": DIGEST,
        "evidence_manifest_digest": DIGEST,
    }
    attestation = {
        "schema_revision": "sceptre-qualification-attestation-v1",
        "qualification_label": "0.2.0-rc.1",
        "decision": "approved",
        "decided_at": "2026-08-10T08:00:00Z",
        "artifact_manifest_digest": DIGEST,
        "decision_register_digest": DIGEST,
        "go_no_go_record_digest": DIGEST,
        "providers": {"aws-eks": provider, "gcp-gke": provider, "azure-aks": provider},
        "final_allocations": [
            {
                "task": "classification",
                "canonical_provider": "aws-eks",
                "allocation_receipt_digest": DIGEST,
                "result_digest": DIGEST,
            }
        ],
        "approvers": [
            {
                "subject": "release@example.com",
                "role": "release-board",
                "approved_at": "2026-08-10T08:00:00Z",
            }
        ],
        "signatures": [
            {
                "issuer": "https://token.actions.githubusercontent.com",
                "subject": "release",
                "bundle_digest": DIGEST,
            }
        ],
    }

    _validator("qualification-attestation.schema.json").validate(attestation)


def test_release_pointer_targets_attestation_and_rejects_direct_artifact_target() -> None:
    pointer = {
        "schema_revision": "sceptre-release-channel-pointer-v1",
        "channel": "stable",
        "sequence": 1,
        "qualification_attestation_digest": DIGEST,
        "previous_pointer_digest": None,
        "published_at": "2026-08-10T08:00:00Z",
        "publisher": "release-board",
        "publication_receipt": "registry://sceptre/stable/1",
        "signatures": [
            {
                "issuer": "https://token.actions.githubusercontent.com",
                "subject": "release",
                "bundle_digest": DIGEST,
            }
        ],
    }
    validator = _validator("release-channel-pointer.schema.json")

    validator.validate(pointer)
    direct_artifact_pointer = {**pointer, "artifact_manifest_digest": DIGEST}
    with pytest.raises(ValidationError):
        validator.validate(direct_artifact_pointer)
