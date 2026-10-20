"""A policy reference cannot choose another local project's authority credential."""

import json
import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.services import evaluation_reconciler as controller
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401


@pytest.fixture
def config(tmp_path, monkeypatch):
    project = uuid.uuid4()
    other = uuid.uuid4()
    tokens = {
        str(project): {"project_reference": "authority-a", "token": "private-allocator-a"},
        str(other): {"project_reference": "authority-b", "token": "private-allocator-b"},
    }
    path = tmp_path / "tokens.json"
    path.write_text(json.dumps(tokens))
    monkeypatch.setenv("EVALUATION_ALLOCATOR_TOKENS_FILE", str(path))
    monkeypatch.setenv("EVALUATION_AUTHORITY_URL", "https://authority.test")
    monkeypatch.setenv("EVALUATION_CA_FILE", "/protected/ca.pem")
    client = MagicMock()
    tls = MagicMock()
    monkeypatch.setattr(controller.httpx, "Client", client)
    monkeypatch.setattr(controller.ssl, "create_default_context", tls)
    return project, other, path, tokens, client, tls


def test_valid_local_project_reference_selects_only_its_token(config):
    project, _, _, tokens, client, tls = config
    with controller.configured_authority(project, "authority-a") as actual:
        assert actual is client.return_value.__enter__.return_value
    kwargs = client.call_args.kwargs
    assert kwargs["headers"] == {"Authorization": "Bearer " + tokens[str(project)]["token"]}
    assert kwargs["base_url"] == "https://authority.test/"
    assert kwargs["verify"] is tls.return_value
    assert kwargs["follow_redirects"] is False
    assert kwargs["timeout"].connect == 10 and kwargs["timeout"].read == 30
    tls.assert_called_once_with(cafile="/protected/ca.pem")
    client.return_value.__exit__.assert_called_once()


@pytest.mark.parametrize(
    "fault",
    [
        "foreign_reference",
        "foreign_project",
        "missing_project",
        "legacy",
        "root_list",
        "invalid_json",
        "record_string",
        "missing_reference",
        "extra",
        "empty_token",
        "token_type",
        "token_newline",
        "token_unicode",
        "invalid_project",
        "missing_file",
    ],
)
def test_invalid_binding_never_constructs_a_transport_or_discloses_tokens(config, fault):
    project, other, path, tokens, client, tls = config
    reference = "authority-a"
    if fault == "foreign_reference":
        reference = "authority-b"
    elif fault == "foreign_project":
        project = other
    elif fault == "missing_project":
        project = uuid.uuid4()
    elif fault == "legacy":
        tokens = {"authority-a": "private-allocator-a", "authority-b": "private-allocator-b"}
    elif fault == "root_list":
        tokens = [tokens]
    elif fault == "record_string":
        tokens[str(project)] = "private-allocator-a"
    elif fault == "missing_reference":
        del tokens[str(project)]["project_reference"]
    elif fault == "extra":
        tokens[str(project)]["fallback"] = "private-allocator-b"
    elif fault == "empty_token":
        tokens[str(project)]["token"] = ""
    elif fault == "token_type":
        tokens[str(project)]["token"] = {"secret": "private-allocator-a"}
    elif fault == "token_newline":
        tokens[str(project)]["token"] += "\r\nInjected: value"
    elif fault == "token_unicode":
        tokens[str(project)]["token"] += "é"
    elif fault == "invalid_project":
        project = "private-allocator-a"
    path.write_text(
        "invalid-private-allocator-a" if fault == "invalid_json" else json.dumps(tokens)
    )
    if fault == "missing_file":
        path.unlink()
    with pytest.raises(ValueError) as error:
        with controller.configured_authority(project, reference):
            pytest.fail("Invalid binding must never yield a client")
    assert str(error.value) == "Missing or invalid project-bound allocator identity"
    assert "private-allocator" not in str(error.value)
    client.assert_not_called()
    tls.assert_not_called()


def test_explicitly_shared_reference_still_selects_by_local_uuid(config):
    project, other, path, tokens, client, _ = config
    tokens[str(other)]["project_reference"] = "authority-a"
    path.write_text(json.dumps(tokens))
    for local in (project, other):
        with controller.configured_authority(local, "authority-a"):
            pass
    assert [c.kwargs["headers"]["Authorization"] for c in client.call_args_list] == [
        "Bearer private-allocator-a",
        "Bearer private-allocator-b",
    ]


def test_controller_selects_persisted_project_ids_even_for_shared_references(
    phase1_engine,  # noqa: F811
    committed_tenant,  # noqa: F811
    tmp_path,
    monkeypatch,
):
    from automl_api.models.datasets import Dataset, DatasetVersion
    from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
    from automl_api.models.projects import Project
    from automl_api.models.workflows import DatasetSplitRevision, PromotionalScope, WorkflowAttempt
    from sqlalchemy import delete
    from sqlalchemy.orm import Session, sessionmaker

    owner, project = committed_tenant
    other = uuid.uuid4()
    scopes = {}
    with Session(phase1_engine) as db, db.begin():
        db.add(Project(id=other, owner_id=owner, created_by_id=owner, name="second-bound-project"))
        db.flush()
        for local in (project, other):
            dataset = Dataset(project_id=local, created_by_id=owner, name="selection-fixture")
            db.add(dataset)
            db.flush()
            version = DatasetVersion(
                project_id=local,
                dataset_id=dataset.id,
                created_by_id=owner,
                version_number=1,
                format="CSV",
                object_uri="embedded://test/selection.csv",
                content_hash="a" * 64,
            )
            db.add(version)
            db.flush()
            split = DatasetSplitRevision(
                project_id=local,
                dataset_version_id=version.id,
                revision=1,
                content_digest="b" * 64,
                digest_scope="selector_fixture",
                train_digest="c" * 64,
                validation_digest="d" * 64,
                final_test_digest="e" * 64,
            )
            db.add(split)
            db.flush()
            scope = PromotionalScope(
                project_id=local,
                scope_key=uuid.uuid4().hex,
                split_revision_id=split.id,
                canonical_provider="aws",
                mode="promotional",
                expected_members=1,
                status=ScopeStatus.RUNNING,
                comparison_policy={
                    "evaluation_policy": {
                        "final_data": {
                            "project_reference": "shared-reference",
                        }
                    }
                },
            )
            db.add(scope)
            db.flush()
            scopes[scope.id] = local
            db.add(
                WorkflowAttempt(
                    project_id=local,
                    scope_id=scope.id,
                    stage=WorkflowStage.CHAMPION_REFIT,
                    logical_key="selector:" + str(scope.id),
                    generation=1,
                    fencing_token=uuid.uuid4().hex,
                    workload_identity="selector-fixture",
                    status=AttemptStatus.SUCCEEDED,
                )
            )
    try:
        tokens = {
            str(local): {"project_reference": "shared-reference", "token": str(local)}
            for local in (project, other)
        }
        path = tmp_path / "tokens.json"
        path.write_text(json.dumps(tokens))
        key = tmp_path / "public.pem"
        key.write_text("selector-fixture-public-key")
        monkeypatch.setenv("EVALUATION_ALLOCATOR_TOKENS_FILE", str(path))
        monkeypatch.setenv("EVALUATION_AUTHORITY_PUBLIC_KEY_FILE", str(key))
        monkeypatch.setenv("EVALUATION_AUTHORITY_URL", "https://authority.test")
        monkeypatch.delenv("EVALUATION_CA_FILE", raising=False)
        monkeypatch.setattr(controller, "_next_sweep", float("inf"))
        seen = {}

        @contextmanager
        def transport(**kwargs):
            yield SimpleNamespace(headers=kwargs["headers"])

        def reconcile(_factory, _k8s, scope_id, authority, **_kwargs):
            assert phase1_engine.pool.checkedout() == 0
            seen[scope_id] = authority.headers["Authorization"]

        monkeypatch.setattr(controller.httpx, "Client", transport)
        monkeypatch.setattr(controller, "reconcile_scope", reconcile)
        assert (
            controller.reconcile_evaluation_jobs_once(
                sessionmaker(phase1_engine),
                object(),
                store=object(),
                limit=100,
            )
            == 2
        )
        assert seen == {scope: "Bearer " + str(local) for scope, local in scopes.items()}
    finally:
        with Session(phase1_engine) as db, db.begin():
            db.execute(delete(Project).where(Project.id == other))
