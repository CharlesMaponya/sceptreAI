"""Known native replies must remain bound to their original Ray submission."""

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import WorkflowStage
from automl_api.models.workflows import WorkflowEvent
from automl_api.services import reconciler
from automl_api.services.workflow_state import canonical_request_hash
from digest_fixtures import verified_raw_version
from ray_submission_helpers import submit_dataset_ray_job, submit_training_ray_job
from test_local_profiling_delivery import profile_source as profile_source
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_phase1_reconciler import _attempt, _entry, _run
from test_phase3_preparation import _dataset_attempt, _dataset_entry, _profile_and_version


@pytest.fixture(params=["training", "splitter", "preparation"])
def receipt_case(request):
    kind = request.param
    if kind == "training":
        run = _run()
        attempt = _attempt(run)
        entry = _entry(attempt)
        version = verified_raw_version(id=run.dataset_version_id, project_id=run.project_id)
        resources = {"ModelRun": run, "WorkflowAttempt": attempt, "DatasetVersion": version}
        submit = submit_training_ray_job
    else:
        attempt = _dataset_attempt(WorkflowStage(kind))
        profile, version = _profile_and_version(attempt)
        entry = _dataset_entry(attempt, profile.id)
        resources = {"ProfilingJob": profile, "WorkflowAttempt": attempt, "DatasetVersion": version}
        submit = submit_dataset_ray_job
    db = MagicMock()
    db.scalar.side_effect = lambda statement: resources.get(
        statement.column_descriptions[0]["entity"].__name__
    )
    db.scalars.return_value = []
    db.get.return_value = version
    c = SimpleNamespace(db=db, attempt=attempt, entry=entry, submit=submit, kind=kind)
    c.k8s = MagicMock()
    c.patch = lambda reply: None

    def create(manifest):
        c.manifest = copy.deepcopy(manifest)
        c.reply = copy.deepcopy(manifest)
        c.reply["metadata"]["uid"] = "original-native-uid"
        c.patch(c.reply)
        return c.reply

    c.k8s.create_ray_job.side_effect = create
    return c


def _events(db):
    return [
        call.args[0] for call in db.add.call_args_list if isinstance(call.args[0], WorkflowEvent)
    ]


def test_both_ray_producers_retain_original_known_identity(receipt_case):
    c = receipt_case
    assert c.submit(c.db, c.entry, c.k8s) is c.attempt
    events = _events(c.db)
    assert len(events) == 1
    event = events[0]
    assert event.event_key == event.event_type == "ray.submitted"
    assert event.project_id == c.attempt.project_id and event.attempt_id == c.attempt.id
    assert event.payload == {
        "uid": "original-native-uid",
        "namespace": c.manifest["metadata"]["namespace"],
        "job_name": c.manifest["metadata"]["name"],
        "outbox_id": str(c.entry.id),
        "request_sha256": canonical_request_hash(c.entry.payload),
        "manifest_sha256": canonical_request_hash(c.manifest),
    }
    assert event.result_digest == canonical_request_hash(event.payload)
    c.k8s.create_ray_job.assert_called_once()


def test_submitted_replay_keeps_original_identity_without_native_reissuance(receipt_case):
    c = receipt_case
    c.submit(c.db, c.entry, c.k8s)
    original = copy.deepcopy(_events(c.db)[0].payload)
    c.patch = lambda reply: reply["metadata"].update(uid="replacement-native-uid")
    assert c.submit(c.db, c.entry, c.k8s) is c.attempt
    assert [event.payload for event in _events(c.db)] == [original]
    c.k8s.create_ray_job.assert_called_once()


@pytest.mark.parametrize(
    "field,value",
    [
        ("uid", None),
        ("uid", ""),
        ("uid", "  "),
        ("uid", True),
        ("name", "replacement-name"),
        ("namespace", "replacement-namespace"),
    ],
)
def test_invalid_known_identity_never_advances_logical_submission(receipt_case, field, value):
    from automl_api.models.enums import AttemptStatus
    from automl_api.services.workflow_state import StaleFence

    c = receipt_case
    c.patch = lambda reply: reply["metadata"].update({field: value})
    with pytest.raises(StaleFence, match="native identity"):
        c.submit(c.db, c.entry, c.k8s)
    assert c.attempt.status != AttemptStatus.SUBMITTED and not _events(c.db)


def test_original_receipt_on_a_claimable_attempt_prevents_any_native_reissuance(receipt_case):
    from automl_api.services.workflow_state import StaleFence

    c = receipt_case
    ordinary = c.db.scalar.side_effect
    c.db.scalar.side_effect = lambda statement: (
        WorkflowEvent(event_key="ray.submitted")
        if statement.column_descriptions[0]["entity"] is WorkflowEvent
        else ordinary(statement)
    )
    with pytest.raises(StaleFence, match="prevents native reissuance"):
        c.submit(c.db, c.entry, c.k8s)
    assert not c.k8s.mock_calls and not _events(c.db)


def test_source_changed_during_native_call_cannot_be_adopted_as_the_original(receipt_case):
    from automl_api.services.workflow_state import StaleFence

    c = receipt_case
    c.patch = lambda _: c.entry.payload.update(unexpected_source="changed")
    with pytest.raises(StaleFence, match="request changed"):
        c.submit(c.db, c.entry, c.k8s)
    assert not _events(c.db)


@pytest.fixture(params=["training", "splitter", "preparation"])
def committed_submission(profile_source, request):
    import uuid

    from automl_api.models.datasets import ProfilingJob
    from automl_api.models.enums import AttemptStatus, RunKind, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand
    from sqlalchemy.orm import Session

    engine, actor_id, project_id, dataset_id, version_id, _ = profile_source
    kind = request.param
    attempt_id = uuid.uuid4()
    with Session(engine) as db, db.begin():
        if kind == "training":
            resource = ModelRun(
                project_id=project_id,
                dataset_version_id=version_id,
                created_by_id=actor_id,
                run_kind=RunKind.TRAINING,
                task_type="regression",
                status=RunStatus.QUEUED,
                k8s_namespace="original-submission",
                params={},
                tags={},
            )
        else:
            resource = ProfilingJob(
                project_id=project_id,
                dataset_id=dataset_id,
                dataset_version_id=version_id,
                created_by_id=actor_id,
                status="queued",
                overview_json={"workflow_attempt_id": str(attempt_id)},
            )
        db.add(resource)
        db.flush()
        attempt = WorkflowAttempt(
            id=attempt_id,
            project_id=project_id,
            model_run_id=resource.id if kind == "training" else None,
            dataset_version_id=version_id,
            stage=WorkflowStage.TRAINING_RUN if kind == "training" else WorkflowStage(kind),
            logical_key=f"training-run:{resource.id}"
            if kind == "training"
            else f"profile:{resource.id}:{kind}",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            workload_identity="original-submission",
            status=AttemptStatus.CLAIMED,
        )
        db.add(attempt)
        command = WorkflowCommand(
            project_id=project_id,
            actor_id=actor_id,
            operation="owned-ray-submission-test",
            idempotency_key=uuid.uuid4().hex,
            request_hash="a" * 64,
        )
        db.add(command)
        db.flush()
        payload = {"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token}
        if kind != "training":
            payload["profiling_job_id"] = str(resource.id)
        entry = OutboxEntry(
            project_id=project_id,
            command_id=command.id,
            event_key=uuid.uuid4().hex,
            aggregate_type="model_run" if kind == "training" else "profiling_job",
            aggregate_id=resource.id,
            topic="ray.training.submit"
            if kind == "training"
            else "ray.dataset.split.submit"
            if kind == "splitter"
            else "ray.dataset.prepare.submit",
            payload=payload,
        )
        db.add(entry)
        db.flush()
        entry_id = entry.id
    k8s = MagicMock()
    k8s.create_ray_job.side_effect = lambda body: {
        **copy.deepcopy(body),
        "metadata": {**body["metadata"], "uid": "original-committed-uid"},
    }
    return SimpleNamespace(
        engine=engine,
        attempt_id=attempt_id,
        entry_id=entry_id,
        k8s=k8s,
        submit=submit_training_ray_job if kind == "training" else submit_dataset_ray_job,
    )


def _submit_committed(c, *, commit=True):
    from automl_api.models.workflows import OutboxEntry
    from sqlalchemy.orm import Session

    with Session(c.engine) as db:
        c.submit(db, db.get(OutboxEntry, c.entry_id), c.k8s)
        db.flush()
        if commit:
            db.commit()
        else:
            db.rollback()


@pytest.mark.parametrize("commit", [False, True])
def test_actual_sql_commit_and_rollback_keep_receipt_and_projection_together(
    committed_submission, commit
):
    from automl_api.models.enums import AttemptStatus
    from automl_api.models.workflows import WorkflowAttempt
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    c = committed_submission
    _submit_committed(c, commit=commit)
    with Session(c.engine) as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        events = list(
            db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
        )
        if not commit:
            assert attempt.status == AttemptStatus.CLAIMED
            assert not attempt.ray_job_name and not events
            return
        assert attempt.status == AttemptStatus.SUBMITTED and len(events) == 1
        receipt, source = reconciler._ray_submission(db, attempt)
        assert receipt["uid"] == "original-committed-uid" and source.id == c.entry_id
        original = copy.deepcopy(events[0].payload)
    _submit_committed(c)
    assert c.k8s.create_ray_job.call_count == 1
    with Session(c.engine) as db:
        receipt, _ = reconciler._ray_submission(db, db.get(WorkflowAttempt, c.attempt_id))
        assert receipt == original


@pytest.mark.parametrize(
    "corruption", ["uid", "request", "source_scope", "event_type", "outbox_id"]
)
def test_original_receipt_reader_rejects_corrupt_or_retargeted_sources(
    committed_submission, corruption
):
    from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
    from automl_api.services.workflow_state import StaleFence
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    c = committed_submission
    _submit_committed(c)
    with Session(c.engine) as db, db.begin():
        event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == c.attempt_id))
        source = db.get(OutboxEntry, c.entry_id)
        if corruption in {"uid", "outbox_id"}:
            event.payload = {**event.payload, corruption: "changed"}
            if corruption == "outbox_id":
                event.result_digest = canonical_request_hash(event.payload)
        elif corruption == "request":
            source.payload = {**source.payload, "fencing_token": "different"}
        elif corruption == "source_scope":
            source.aggregate_type = "foreign-resource"
        else:
            event.event_type = "foreign-event"
    with Session(c.engine) as db, pytest.raises(StaleFence, match="Original Ray"):
        reconciler._ray_submission(db, db.get(WorkflowAttempt, c.attempt_id))


def test_new_receipt_appends_after_existing_attempt_history(committed_submission):
    from automl_api.models.workflows import WorkflowAttempt
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    c = committed_submission
    with Session(c.engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        db.add(
            WorkflowEvent(
                project_id=attempt.project_id,
                attempt_id=attempt.id,
                sequence=7,
                event_key="existing.observation",
                event_type="existing.observation",
                payload={},
            )
        )
    _submit_committed(c)
    with Session(c.engine) as db:
        values = db.execute(
            select(WorkflowEvent.event_key, WorkflowEvent.sequence)
            .where(WorkflowEvent.attempt_id == c.attempt_id)
            .order_by(WorkflowEvent.sequence)
        ).all()
        assert values == [("existing.observation", 7), ("ray.submitted", 8)]


def test_lost_commit_acknowledgement_replays_the_committed_original_receipt(committed_submission):
    from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    c = committed_submission
    with Session(c.engine) as db:
        c.submit(db, db.get(OutboxEntry, c.entry_id), c.k8s)

        @event.listens_for(db, "after_commit", once=True)
        def lose_ack(_):
            raise RuntimeError("owned test lost SQL commit acknowledgement")

        with pytest.raises(RuntimeError, match="lost SQL commit acknowledgement"):
            db.commit()
    _submit_committed(c)
    assert c.k8s.create_ray_job.call_count == 1
    with Session(c.engine) as db:
        receipt, _ = reconciler._ray_submission(db, db.get(WorkflowAttempt, c.attempt_id))
        assert receipt["uid"] == "original-committed-uid"


def test_reader_refreshes_cached_original_receipt_before_validation(committed_submission):
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.workflow_state import StaleFence
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    c = committed_submission
    _submit_committed(c)
    with Session(c.engine) as cached:
        original = cached.scalar(
            select(WorkflowEvent).where(WorkflowEvent.attempt_id == c.attempt_id)
        )
        with Session(c.engine) as writer, writer.begin():
            changed = writer.get(WorkflowEvent, original.id)
            changed.payload = {**changed.payload, "uid": "corrupted-after-cache"}
        with pytest.raises(StaleFence, match="evidence"):
            reconciler._ray_submission(cached, cached.get(WorkflowAttempt, c.attempt_id))
