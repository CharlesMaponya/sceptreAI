"""The existing operator CLI can verify physical copies without leaking locations."""

import hashlib
import importlib.util
import json
import sys
from unittest.mock import MagicMock

import pytest
from automl_api.services import final_test_authority as authority
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.storage import object_store
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from test_provider_copy_verification import VersionStore, budget, manifest


def signed(value, key):
    data = value.model_dump(mode="json")
    record = {
        "allocation_id": str(value.allocation_id),
        "operation": "distribution_" + value.provider,
        "provider": value.provider,
        "request_digest": canonical_request_hash(data),
        "payload": {
            "manifest": data,
            "signing_key_id": hashlib.sha256(key.public_key().public_bytes_raw()).hexdigest(),
        },
        "signature_algorithm": "ed25519",
    }
    body = authority._receipt_bytes(
        **{
            name: record[name]
            for name in ["allocation_id", "operation", "provider", "request_digest", "payload"]
        }
    )
    return {
        **record,
        "receipt_digest": hashlib.sha256(body).hexdigest(),
        "signature": key.sign(body).hex(),
    }


@pytest.fixture
def command(tmp_path, monkeypatch):
    store = VersionStore()
    value = manifest(store)
    key = Ed25519PrivateKey.generate()
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(signed(value, key)))
    public = tmp_path / "public.pem"
    public.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    limits = tmp_path / "budget.json"
    limits.write_text(budget().model_dump_json())
    args = ["validate_provider_distribution.py", str(receipt), "--public-key", str(public)]
    for name in [
        "allocation_id",
        "scope_id",
        "project_reference",
        "provider",
        "canonical_provider",
        "fixture_revision",
        "split_revision",
        "split_digest",
    ]:
        args += ["--" + name.replace("_", "-"), str(getattr(value, name))]
    spec = importlib.util.spec_from_file_location(
        "copy_cli", "scripts/validate_provider_distribution.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    factory = MagicMock(return_value=store)
    monkeypatch.setattr(object_store, "get_object_store", factory)
    monkeypatch.setattr(sys, "argv", args)
    return cli, args, limits, receipt, store, factory


def test_offline_signature_check_does_not_construct_storage(command, capsys):
    cli, _, _, _, _, factory = command
    cli.main()
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "verified": True,
        "revision": "provider-distribution-v1",
        "provider": "aws",
        "mode": "validation_only",
        "safe_objects": 2,
    }
    factory.assert_not_called()


def test_opted_in_physical_verification_uses_explicit_budgets_and_safe_output(command, capsys):
    cli, args, limits, _, store, factory = command
    args += ["--verify-objects", "--verification-budget", str(limits)]
    cli.main()
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["verified"] and result["objects_verified"]
    assert result["verified_rows"] == {"train": 2, "validation": 2}
    assert result["verified_bytes"] == sum(len(v) for v in store.objects.values())
    assert len(result["manifest_digest"]) == 64 and "s3c://" not in captured.out
    assert captured.err == "" and all(source.closed for source in store.streams)
    factory.assert_called_once_with()


@pytest.mark.parametrize(
    "fault", ["signature", "references", "missing_budget", "bad_budget", "budget_without_objects"]
)
def test_invalid_or_unbudgeted_request_never_constructs_storage(command, capsys, fault):
    cli, args, limits, receipt, _, factory = command
    args += ["--verify-objects"]
    if fault != "missing_budget":
        args += ["--verification-budget", str(limits)]
    if fault == "signature":
        record = json.loads(receipt.read_text())
        record["signature"] = "0" * 128
        receipt.write_text(json.dumps(record))
    elif fault == "references":
        args[args.index("--project-reference") + 1] = "another-project"
    elif fault == "bad_budget":
        limits.write_text('{"secret":"must-not-disclose"}')
    elif fault == "budget_without_objects":
        args.remove("--verify-objects")
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 1
    result = capsys.readouterr()
    assert result.out == "" and result.err == "Provider distribution verification failed\n"
    factory.assert_not_called()


def test_correct_signature_cannot_bless_replaced_object_bytes(command, capsys):
    cli, args, limits, _, store, _ = command
    args += ["--verify-objects", "--verification-budget", str(limits)]
    key = next(iter(store.objects))
    store.objects[key] = b"replaced-private-data"
    with pytest.raises(SystemExit) as error:
        cli.main()
    result = capsys.readouterr()
    assert error.value.code == 1 and result.out == ""
    assert result.err == "Provider distribution verification failed\n"
    assert all(source.closed for source in store.streams)
