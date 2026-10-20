"""Revision-pair uniqueness and expand-compatible central allocation records."""

import copy
import importlib.util
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from automl_api.models.qualification import FinalTestAllocation, FinalTestAuthorityReceipt
from automl_api.qualification_control import create_app
from automl_api.services import final_test_authority as authority
from automl_api.services.workflow_state import IdempotencyConflict
from fastapi.testclient import TestClient
from sqlalchemy import MetaData, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from test_phase1_migration_rehearsal import (  # noqa: F401
    migrated_case,
    migrated_template,
    rehearsal,
)
from test_provider_distribution import distribution_case  # noqa: F401
from test_qualification_control import authority_api  # noqa: F401


@pytest.fixture
def revision_case(authority_api):  # noqa: F811
    client, scope, kwargs = authority_api
    payload = dict(
        split_digest=uuid.uuid4().hex * 2,
        scope_id=str(scope),
        canonical_provider="aws",
        provider_manifest_digest="a" * 64,
        fixture_revision="fixture-" + scope.hex,
        split_revision="split-v1",
    )
    return client, kwargs, payload


def post(case, payload=None, client=None):
    original, _, value = case
    return (client or original).post(
        "/allocations",
        json=value if payload is None else payload,
        headers={"Authorization": "Bearer allocator"},
    )


def test_bound_revision_replay_and_recovery_inspection(revision_case):
    client, _, payload = revision_case
    response = post(revision_case)
    assert response.status_code == 200, response.text
    assert post(revision_case).json() == response.json()
    state = client.get(
        "/allocations/" + response.json()["allocation_id"],
        headers={"Authorization": "Bearer allocator"},
    )
    assert state.status_code == 200
    assert state.json()["fixture_revision"] == payload["fixture_revision"]
    assert state.json()["split_revision"] == payload["split_revision"]


@pytest.mark.parametrize(
    "change",
    ["split_digest", "scope_id", "provider", "project", "fixture", "split", "drop_revisions"],
)
def test_existing_binding_cannot_be_reassigned(revision_case, change):
    _, kwargs, payload = revision_case
    assert post(revision_case).status_code == 200
    changed = copy.deepcopy(payload)
    if change == "split_digest":
        changed["split_digest"] = "b" * 64
    elif change == "scope_id":
        changed["scope_id"] = str(uuid.uuid4())
    elif change == "provider":
        changed["canonical_provider"] = "gcp"
    elif change == "fixture":
        changed["fixture_revision"] = "different-fixture"
    elif change == "split":
        changed["split_revision"] = "different-split"
    elif change == "drop_revisions":
        changed.pop("fixture_revision")
        changed.pop("split_revision")
    if change == "project":
        allocator = kwargs["principals"][0].model_copy(update={"project_reference": "different"})
        with TestClient(create_app(**{**kwargs, "principals": [allocator]})) as client:
            assert post(revision_case, changed, client).status_code == 409
    else:
        assert post(revision_case, changed).status_code == 409


@pytest.mark.parametrize(
    "values",
    [
        {"fixture_revision": "only"},
        {"split_revision": "only"},
        {"fixture_revision": "", "split_revision": "v1"},
        {"fixture_revision": "  ", "split_revision": "v1"},
        {"fixture_revision": "x" * 129, "split_revision": "v1"},
        {"fixture_revision": "line\nbreak", "split_revision": "v1"},
    ],
)
def test_incomplete_or_invalid_pairs_fail_at_api_and_service(revision_case, values):
    _, kwargs, payload = revision_case
    payload = {k: v for k, v in payload.items() if k not in {"fixture_revision", "split_revision"}}
    payload.update(values)
    assert post(revision_case, payload).status_code == 422
    with kwargs["session_factory"]() as db, pytest.raises(ValueError):
        authority.allocate(
            db,
            project_reference=kwargs["principals"][0].project_reference,
            **{**payload, "scope_id": uuid.UUID(payload["scope_id"])},
        )


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_pair_allocation_has_one_winner(revision_case, conflicting):
    _, kwargs, payload = revision_case
    barrier = Barrier(6)

    def allocate(i):
        value = copy.deepcopy(payload)
        if conflicting and i % 2:
            value["scope_id"] = str(uuid.uuid4())
            value["split_digest"] = uuid.uuid4().hex * 2
        barrier.wait(timeout=10)
        return post(revision_case, value)

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(allocate, range(6)))
    successes = [r.json()["allocation_id"] for r in responses if r.status_code == 200]
    assert len(set(successes)) == 1
    assert all(r.status_code in {200, 409} for r in responses)
    assert len(successes) == (6 if not conflicting else 3 if responses[0].status_code == 200 else 1)
    with kwargs["session_factory"]() as db:
        rows = list(
            db.scalars(
                select(FinalTestAllocation).where(
                    FinalTestAllocation.fixture_revision == payload["fixture_revision"],
                    FinalTestAllocation.split_revision == payload["split_revision"],
                )
            )
        )
        assert len(rows) == 1 and str(rows[0].id) == successes[0]


def test_database_uniqueness_is_global_and_half_pairs_are_rejected(revision_case):
    _, kwargs, payload = revision_case
    assert post(revision_case).status_code == 200
    with kwargs["session_factory"]() as db, db.begin():
        for fixture, split in [
            (payload["fixture_revision"], payload["split_revision"]),
            ("half", None),
            (None, "half"),
            ("", "v1"),
            (" ", "v1"),
            ("bad\n", "v1"),
        ]:
            with pytest.raises(IntegrityError), db.begin_nested():
                db.add(
                    FinalTestAllocation(
                        split_digest=uuid.uuid4().hex * 2,
                        scope_id=uuid.uuid4(),
                        project_reference="other",
                        canonical_provider="azure",
                        provider_manifest_digest="e" * 64,
                        fixture_revision=fixture,
                        split_revision=split,
                    )
                )
                db.flush()


def test_unbound_legacy_allocation_cannot_be_upgraded_by_replay(revision_case):
    _, kwargs, payload = revision_case
    legacy = {k: v for k, v in payload.items() if k not in {"fixture_revision", "split_revision"}}
    first = post(revision_case, legacy)
    assert first.status_code == 200
    assert post(revision_case, legacy).json() == first.json()
    assert post(revision_case).status_code == 409
    with kwargs["session_factory"]() as db:
        row = db.get(FinalTestAllocation, uuid.UUID(first.json()["allocation_id"]))
        assert row.fixture_revision is None and row.split_revision is None


@pytest.mark.parametrize("unbound", [False, True])
def test_distribution_cannot_relabel_allocation(distribution_case, unbound):  # noqa: F811
    client, kwargs, _, request, path, refs = distribution_case
    with kwargs["session_factory"]() as db, db.begin():
        row = db.get(FinalTestAllocation, refs["allocation_id"])
        row.fixture_revision = None if unbound else "different-fixture"
        if unbound:
            row.split_revision = None
    assert client.post(path, json=request).status_code == 409
    with kwargs["session_factory"]() as db:
        assert (
            db.scalar(
                select(FinalTestAuthorityReceipt.id).where(
                    FinalTestAuthorityReceipt.allocation_id == refs["allocation_id"]
                )
            )
            is None
        )


def test_dedicated_sql_expansion_preserves_old_readers_and_matches_models(
    revision_case, monkeypatch
):
    _, kwargs, _ = revision_case
    engine = kwargs["session_factory"].kw["bind"]
    schema = "revision_expansion_" + uuid.uuid4().hex
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            root = Path("services/qualification-control/migrations")
            connection.exec_driver_sql((root / "0001_authority.sql").read_text())
            client = TestClient(
                create_app(
                    **{
                        **kwargs,
                        "session_factory": lambda: Session(
                            bind=connection, join_transaction_mode="create_savepoint"
                        ),
                    }
                )
            )
            assert client.get("/readyz").status_code == 503
            old_columns = [
                c["name"] for c in inspect(connection).get_columns("final_test_allocations")
            ]
            old_id = uuid.uuid4()
            values = {"id": old_id, "split": uuid.uuid4().hex * 2, "scope": uuid.uuid4()}
            statement = text(
                "INSERT INTO final_test_allocations "
                "(id,split_digest,project_reference,scope_id,canonical_provider,"
                "provider_manifest_digest) "
                "VALUES (:id,:split,'legacy',:scope,'aws','old-manifest')"
            )
            connection.execute(statement, values)
            connection.execute(
                text(
                    "INSERT INTO final_test_authority_receipts "
                    "(id,allocation_id,operation,provider,request_digest,receipt_digest,signature_algorithm,"
                    "signature,payload) VALUES (:id,:allocation,'legacy','aws','request','receipt',"
                    "'hmac-sha256','preserved-test-signature',CAST(:payload AS JSONB))"
                ),
                {"id": uuid.uuid4(), "allocation": old_id, "payload": json.dumps({"legacy": True})},
            )
            columns = ", ".join('"' + c + '"' for c in old_columns)
            before = connection.execute(
                text("SELECT " + columns + " FROM final_test_allocations")
            ).all()
            receipts = connection.execute(text("SELECT * FROM final_test_authority_receipts")).all()
            connection.exec_driver_sql((root / "0002_allocation_revisions.sql").read_text())
            assert client.get("/readyz").status_code == 200
            client.close()
            assert (
                connection.execute(text("SELECT " + columns + " FROM final_test_allocations")).all()
                == before
            )
            assert (
                connection.execute(text("SELECT * FROM final_test_authority_receipts")).all()
                == receipts
            )
            assert connection.execute(
                text("SELECT fixture_revision,split_revision FROM final_test_allocations")
            ).one() == (None, None)
            connection.execute(
                statement,
                {"id": uuid.uuid4(), "split": uuid.uuid4().hex * 2, "scope": uuid.uuid4()},
            )
            spec = importlib.util.spec_from_file_location(
                "revision_schema_verifier", "scripts/verify_database_schema.py"
            )
            verifier = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(verifier)
            metadata = MetaData(naming_convention=FinalTestAllocation.metadata.naming_convention)
            for model in (FinalTestAllocation, FinalTestAuthorityReceipt):
                model.__table__.to_metadata(metadata)
            monkeypatch.setattr(verifier, "Base", SimpleNamespace(metadata=metadata))
            assert verifier.schema_differences(connection) == []
        finally:
            transaction.rollback()
        assert not inspect(connection).has_schema(schema)


def test_alembic_downgrade_cannot_discard_bound_revisions(migrated_case):  # noqa: F811
    from sqlalchemy import create_engine

    target, _ = migrated_case
    engine = create_engine(target)
    try:
        with engine.begin() as connection:
            connection.execute(
                FinalTestAllocation.__table__.insert().values(
                    split_digest="b" * 64,
                    project_reference="migration-bound",
                    scope_id=uuid.uuid4(),
                    canonical_provider="aws",
                    provider_manifest_digest="c" * 64,
                    fixture_revision="fixture-v1",
                    split_revision="split-v1",
                )
            )
        failed = rehearsal.subprocess.run(
            [
                rehearsal.sys.executable,
                "-m",
                "alembic",
                "downgrade",
                "0011_preserve_deletion_evidence",
            ],
            env={**rehearsal.os.environ, "DATABASE_URL": target},
            capture_output=True,
            text=True,
        )
        assert (
            failed.returncode != 0
            and "Cannot discard bound final-test revision evidence" in failed.stderr
        )
        with engine.connect() as connection:
            assert (
                connection.scalar(text("SELECT version_num FROM alembic_version"))
                == ScriptDirectory.from_config(Config("alembic.ini")).get_current_head()
            )
            assert connection.execute(
                text("SELECT fixture_revision,split_revision FROM final_test_allocations")
            ).one() == ("fixture-v1", "split-v1")
    finally:
        engine.dispose()


def test_replay_refreshes_a_retained_old_revision_binding(revision_case):
    _, kwargs, payload = revision_case
    response = post(revision_case)
    allocation_id = uuid.UUID(response.json()["allocation_id"])
    with kwargs["session_factory"]() as stale:
        cached = stale.get(FinalTestAllocation, allocation_id)
        assert cached.fixture_revision == payload["fixture_revision"]
        stale.commit()
        with kwargs["session_factory"]() as other, other.begin():
            other.get(FinalTestAllocation, allocation_id).fixture_revision = "changed-by-operator"
        with pytest.raises(IdempotencyConflict):
            authority.allocate(
                stale,
                project_reference=kwargs["principals"][0].project_reference,
                **{**payload, "scope_id": uuid.UUID(payload["scope_id"])},
            )
        assert cached.fixture_revision == "changed-by-operator"
