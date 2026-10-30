"""Original submission evidence must fence Ray observation and fresh publication."""

import copy

import pytest
from automl_api.models.workflows import OutboxEntry, WorkflowCommand, WorkflowEvent
from automl_api.services import reconciler
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.workflow_reconciler import observe_once
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from test_ray_observer_identity import snapshot
from test_ray_observer_inspection import (
    committed_tenant as committed_tenant,
)
from test_ray_observer_inspection import (
    factory_for,
)
from test_ray_observer_inspection import (
    phase1_engine as phase1_engine,
)
from test_ray_observer_inspection import (
    profile_source as profile_source,
)
from test_ray_observer_inspection import (
    ray_scope as ray_scope,
)
from test_ray_submission_factory import factory_case as factory_case
from test_ray_submission_receipts import committed_submission as committed_submission


def receipt(db, attempt_id):
    return db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt_id, WorkflowEvent.event_key == "ray.submitted"
        )
    )


def test_replacement_uid_cannot_update_original_workflow(ray_scope):
    engine, _, _, _, k8s = ray_scope
    before = snapshot(ray_scope)
    job = copy.deepcopy(k8s.ray_job.return_value)
    job["metadata"]["uid"] = "replacement-before-observation"
    k8s.ray_job.return_value = job
    observe_once(factory_for(engine), k8s=k8s)
    assert snapshot(ray_scope) == before


@pytest.mark.parametrize("changed", ["receipt", "source", "command"])
def test_inflight_original_evidence_change_drops_stale_native_result(ray_scope, changed):
    engine, attempt_id, _, _, k8s = ray_scope
    before = snapshot(ray_scope)
    job = copy.deepcopy(k8s.ray_job.return_value)
    checkouts = []

    def read(*_a, **_kw):
        checkouts.append(engine.pool.checkedout())
        with Session(engine) as db, db.begin():
            fact = receipt(db, attempt_id)
            source = db.get(OutboxEntry, fact.payload["outbox_id"])
            if changed == "receipt":
                fact.payload = {**fact.payload, "uid": "changed-original-evidence"}
            elif changed == "source":
                source.payload = {**source.payload, "changed": True}
                fact.payload = {
                    **fact.payload,
                    "request_sha256": canonical_request_hash(source.payload),
                }
            else:
                command = db.get(WorkflowCommand, source.command_id)
                command.request_payload = {**command.request_payload, "changed": True}
                command.request_hash = canonical_request_hash(command.request_payload)
            fact.result_digest = canonical_request_hash(fact.payload)
        return job

    k8s.ray_job.side_effect = read
    observe_once(factory_for(engine), k8s=k8s)
    assert checkouts == [0]
    assert snapshot(ray_scope) == before


@pytest.mark.parametrize("bad", ["missing", "digest", "namespace", "source", "command"])
def test_missing_or_corrupt_original_evidence_blocks_native_read(ray_scope, bad):
    engine, attempt_id, _, _, k8s = ray_scope
    with Session(engine) as db, db.begin():
        fact = receipt(db, attempt_id)
        if bad == "missing":
            db.execute(delete(WorkflowEvent).where(WorkflowEvent.id == fact.id))
        elif bad == "digest":
            fact.result_digest = "0" * 64
        elif bad == "namespace":
            fact.payload = {**fact.payload, "namespace": "foreign-namespace"}
            fact.result_digest = canonical_request_hash(fact.payload)
        else:
            source = db.get(OutboxEntry, fact.payload["outbox_id"])
            if bad == "source":
                source.payload = {**source.payload, "fencing_token": "stale"}
            else:
                db.get(WorkflowCommand, source.command_id).request_hash = "0" * 64
    before = snapshot(ray_scope)
    observe_once(factory_for(engine), k8s=k8s)
    assert not k8s.ray_job.called
    assert snapshot(ray_scope) == before


def test_original_observation_replays_without_changing_submission_evidence(ray_scope):
    engine, attempt_id, _, _, k8s = ray_scope
    with Session(engine) as db:
        original = copy.deepcopy(receipt(db, attempt_id).payload)
    for _ in range(2):
        observe_once(factory_for(engine), k8s=k8s)
    with Session(engine) as db:
        assert receipt(db, attempt_id).payload == original
        assert (
            reconciler._ray_submission(db, db.get(reconciler.WorkflowAttempt, attempt_id))[0]
            == original
        )


@pytest.mark.parametrize("changed", ["receipt", "command", "target"])
def test_factory_creation_fact_cannot_be_rebound_before_collection(factory_case, changed):
    from automl_api import workflow_reconciler
    from automl_api.services import ray_submission

    c = factory_case
    workflow_reconciler.run_once(c.factory, worker_id="owned-observer-source")
    c.checkouts.clear()
    c.k8s.ray_job.return_value = copy.deepcopy(c.native)
    with c.factory() as db, db.begin():
        source = db.get(OutboxEntry, c.entry_id)
        if changed == "receipt":
            fact = receipt(db, c.attempt_id)
            fact.payload = {**fact.payload, "uid": "rebound-replacement"}
            fact.result_digest = canonical_request_hash(fact.payload)
            c.k8s.ray_job.return_value["metadata"]["uid"] = "rebound-replacement"
        elif changed == "command":
            command = db.get(WorkflowCommand, source.command_id)
            command.request_payload = {**command.request_payload, "changed": True}
            command.request_hash = canonical_request_hash(command.request_payload)
        else:
            model = (
                ray_submission.ModelRun
                if source.aggregate_type == "model_run"
                else ray_submission.ProfilingJob
            )
            db.get(model, source.aggregate_id).target_column = "changed-target"
    observe_once(c.factory, k8s=c.k8s)
    c.k8s.ray_job.assert_not_called()
