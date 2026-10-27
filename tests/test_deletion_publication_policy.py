"""Current policy applies equally to fresh native results and known-fact replay."""

import copy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import DeletionStage, DeletionTombstone, OutboxEntry
from automl_api.services import deletion_workflow
from sqlalchemy import select
from test_deletion_factory import _dispatch
from test_deletion_factory import factory_case as factory_case
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


def _object_stage(c, db):
    return db.scalar(
        select(DeletionStage).where(
            DeletionStage.project_id == c.project_id, DeletionStage.resource_class == "object_store"
        )
    )


def test_known_fact_replay_cannot_skip_current_provider_ownership(factory_case, monkeypatch):
    c = factory_case
    original = deletion_workflow.assert_provider_uri_cleanup_safe
    protected = {"current": False}
    target = c.store.uri_for_key(f"projects/{c.project_id}")

    def policy(db, uri):
        original(db, uri)
        if uri == target and protected["current"]:
            raise ValueError("Protected provider copies intersect the requested cleanup prefix")

    monkeypatch.setattr(deletion_workflow, "assert_provider_uri_cleanup_safe", policy)
    _dispatch(c)

    def protect():
        c.callback = None
        protected["current"] = True

    c.callback = protect
    _dispatch(c)
    with c.factory() as db:
        stage = _object_stage(c, db)
        assert stage.status == "pending"
        assert stage.custody["attempts"][0]["outcome"]["absent"] is True
    _dispatch(c, 6)
    with c.factory() as db:
        assert db.get(Project, c.project_id) is not None
        stage = _object_stage(c, db)
        assert stage.status == "pending" and "published" not in stage.custody
    assert c.deleted == [f"projects/{c.project_id}"]
    protected["current"] = False
    c.errors.clear()
    _dispatch(c, 8)
    assert not c.errors
    assert c.deleted.count(f"projects/{c.project_id}") == 1
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


@pytest.mark.parametrize("completed", [2, 3])
def test_previous_storage_proof_cannot_bypass_policy_before_next_stage(
    factory_case, monkeypatch, completed
):
    c = factory_case
    _dispatch(c, completed)
    with c.factory() as db:
        assert _object_stage(c, db).status == "completed"
    deleted = list(c.deleted)
    original = deletion_workflow.assert_provider_uri_cleanup_safe
    target = c.store.uri_for_key(f"projects/{c.project_id}")

    def policy(db, uri):
        original(db, uri)
        if uri == target:
            raise ValueError("Protected provider copies intersect the requested cleanup prefix")

    monkeypatch.setattr(deletion_workflow, "assert_provider_uri_cleanup_safe", policy)
    _dispatch(c, 3)
    assert c.errors and "Protected provider copies" in c.errors[-1]
    assert c.deleted == deleted
    with c.factory() as db:
        assert db.get(Project, c.project_id) is not None
        assert _object_stage(c, db).status == "completed"
        metadata = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id, DeletionStage.resource_class == "metadata"
            )
        )
        assert metadata.status == "pending" and "published" not in metadata.custody
    monkeypatch.setattr(deletion_workflow, "assert_provider_uri_cleanup_safe", original)
    c.errors.clear()
    _dispatch(c, 8)
    assert not c.errors
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


@pytest.mark.parametrize(
    "fault", ["actor", "source", "store", "binding", "binding_shape", "outcome"]
)
def test_known_fact_requires_current_authority_and_original_receipt(
    factory_case, monkeypatch, fault
):
    c = factory_case
    _dispatch(c)

    def hold():
        c.callback = None
        with c.factory() as db, db.begin():
            db.get(DeletionTombstone, c.tombstone_id).legal_hold = True

    c.callback = hold
    _dispatch(c)
    with c.factory() as db, db.begin():
        stage = _object_stage(c, db)
        original_record = copy.deepcopy(stage.custody)
        assert original_record["attempts"][0]["outcome"]["absent"] is True
        db.get(DeletionTombstone, c.tombstone_id).legal_hold = False
        entry = db.get(OutboxEntry, c.entry_id)
        original_payload = copy.deepcopy(entry.payload)
        if fault == "actor":
            db.get(User, c.actor_id).is_active = False
        elif fault == "source":
            entry.payload = {**entry.payload, "changed": True}
        elif fault in {"binding", "binding_shape", "outcome"}:
            changed = copy.deepcopy(stage.custody)
            if fault == "binding":
                changed["binding"]["uris"] = [c.foreign_uri]
            elif fault == "binding_shape":
                changed["binding"]["uris"] = []
            else:
                changed["attempts"][0]["outcome"]["intent_sha256"] = "0" * 64
            stage.custody = changed
    settings = deletion_workflow.get_settings()
    if fault == "store":
        monkeypatch.setattr(
            deletion_workflow,
            "get_settings",
            lambda: replace(settings, object_store_bucket="changed-store"),
        )
    _dispatch(c, 2)
    assert c.errors
    assert c.deleted == [f"projects/{c.project_id}"]
    assert c.store.exists(c.foreign_uri)
    with c.factory() as db, db.begin():
        assert db.get(Project, c.project_id) is not None
        stage = _object_stage(c, db)
        assert stage.status == "pending" and "published" not in stage.custody
        assert len(stage.custody["attempts"]) == 1
        stage.custody = original_record
        db.get(User, c.actor_id).is_active = True
        entry = db.get(OutboxEntry, c.entry_id)
        entry.payload = original_payload
        entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    monkeypatch.setattr(deletion_workflow, "get_settings", lambda: settings)
    c.errors.clear()
    _dispatch(c, 8)
    assert not c.errors
    assert c.deleted.count(f"projects/{c.project_id}") == 1
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None
