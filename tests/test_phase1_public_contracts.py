from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.api.routes import contracts as routes
from automl_api.models.enums import CommandStatus, ScopeStatus, TaskType
from automl_api.models.workflows import (
    TrainingCandidate,
    TrainingTrial,
    WorkflowEvent,
)
from automl_api.schemas.contracts import (
    EvaluationScopeCreate,
    ExperimentSpecCreate,
    FeatureContractCreate,
    RevisionRead,
    SearchSpaceCreate,
)
from automl_api.services import contracts
from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import StreamingResponse


def _revision(**overrides):
    values = {
        "id": uuid.uuid4(),
        "project_id": uuid.uuid4(),
        "name": "revision",
        "revision": 1,
        "digest_algorithm": "sha256",
        "digest_scope": "feature-contract-v1",
        "content_digest": "a" * 64,
        "specification": {"feature": "safe"},
        "created_at": datetime.now(UTC),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_capabilities_are_stable_and_do_not_expose_infrastructure(monkeypatch) -> None:
    monkeypatch.setattr(
        routes,
        "get_settings",
        lambda: SimpleNamespace(
            simple_auth_enabled=True,
            max_concurrent_jobs=15,
            environment="local",
        ),
    )
    result = routes.capabilities()
    assert result.auth_modes == ["simple"]
    assert result.max_qualified_concurrency == 15
    assert result.environment_qualified is False
    assert result.upload_data_region == "local"
    assert result.upload_storage_driver == "embedded"
    assert "hostname" not in result.model_dump()


def test_revision_create_is_canonical_and_checks_optimistic_version(monkeypatch) -> None:
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    db = MagicMock()
    db.scalar.return_value = None
    project_id = uuid.uuid4()
    payload = FeatureContractCreate(
        name="contract",
        task_type=TaskType.REGRESSION,
        target_column="target",
        specification={"columns": ["x"]},
        expected_revision=0,
    )
    revision = contracts.create_revision(
        db, SimpleNamespace(), project_id, "feature-contract", payload
    )
    assert revision.revision == 1
    assert revision.task_type == "regression"
    assert len(revision.content_digest) == 64
    db.add.assert_called_once_with(revision)

    db.scalar.return_value = 2
    with pytest.raises(HTTPException) as error:
        contracts.create_revision(db, SimpleNamespace(), project_id, "feature-contract", payload)
    assert error.value.status_code == 409
    assert error.value.detail["current_revision"] == 2


@pytest.mark.parametrize("kind", ["feature-search-space", "search-objective"])
def test_metric_revision_fields_are_typed(monkeypatch, kind: str) -> None:
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    db = MagicMock()
    db.scalar.return_value = 0
    payload = SearchSpaceCreate(
        name="objective",
        metric_name="rmse",
        metric_direction="minimize",
        specification={"fold_aggregation": "mean"},
    )
    revision = contracts.create_revision(db, SimpleNamespace(), uuid.uuid4(), kind, payload)
    assert revision.metric_name == "rmse"
    assert revision.metric_direction == "minimize"


def test_revision_read_is_project_scoped(monkeypatch) -> None:
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    db = MagicMock()
    row = _revision()
    db.scalar.return_value = row
    assert (
        contracts.get_revision(db, SimpleNamespace(), row.project_id, "feature-contract", row.id)
        is row
    )
    db.scalar.return_value = None
    with pytest.raises(HTTPException) as error:
        contracts.get_revision(db, SimpleNamespace(), row.project_id, "feature-contract", row.id)
    assert error.value.status_code == 404


def test_experiment_spec_requires_every_bound_revision(monkeypatch) -> None:
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    payload = ExperimentSpecCreate(
        name="experiment",
        specification={},
        dataset_version_id=uuid.uuid4(),
        split_revision_id=uuid.uuid4(),
        feature_contract_revision_id=uuid.uuid4(),
        feature_search_space_revision_id=uuid.uuid4(),
        search_objective_revision_id=uuid.uuid4(),
        catalog_revision_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        target_column="target",
        primary_metric="rmse",
    )
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(HTTPException) as error:
        contracts.create_experiment_spec(db, SimpleNamespace(), uuid.uuid4(), payload)
    assert error.value.detail["code"] == "experiment_spec_mismatch"

    bindings = [uuid.uuid4()] * 5 + [0]
    db.scalar.side_effect = bindings
    spec = contracts.create_experiment_spec(db, SimpleNamespace(), uuid.uuid4(), payload)
    assert spec.task_type == "regression"
    assert spec.primary_metric == "rmse"
    db.scalar.side_effect = [*([uuid.uuid4()] * 5), 2]
    conflicting = payload.model_copy(update={"expected_revision": 1})
    with pytest.raises(HTTPException) as error:
        contracts.create_experiment_spec(db, SimpleNamespace(), uuid.uuid4(), conflicting)
    assert error.value.detail["code"] == "experiment_spec_revision_changed"


def test_scope_validation_and_cancel_are_explicit(monkeypatch) -> None:
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    db = MagicMock()
    base = {
        "scope_key": "qualification",
        "split_revision_id": uuid.uuid4(),
        "experiment_spec_revision_id": uuid.uuid4(),
        "canonical_provider": "local",
        "expected_member_count": 1,
        "mode": "promotional",
        "membership_deadline_at": datetime.now(UTC) + timedelta(minutes=10),
    }
    with pytest.raises(HTTPException, match="final-threshold"):
        contracts.create_scope(db, SimpleNamespace(), uuid.uuid4(), EvaluationScopeCreate(**base))
    past = {**base, "membership_deadline_at": datetime.now(UTC) - timedelta(seconds=1)}
    with pytest.raises(HTTPException, match="future"):
        contracts.create_scope(db, SimpleNamespace(), uuid.uuid4(), EvaluationScopeCreate(**past))

    payload = EvaluationScopeCreate(**base, final_threshold_revision="threshold-v1")
    scope = contracts.create_scope(db, SimpleNamespace(), uuid.uuid4(), payload)
    assert scope.mode == "promotional"
    scope.status = ScopeStatus.SUCCEEDED
    db.scalar.return_value = scope
    with pytest.raises(HTTPException) as error:
        contracts.cancel_scope(db, SimpleNamespace(), scope.project_id, scope.id)
    assert error.value.detail["code"] == "scope_terminal"
    scope.status = ScopeStatus.OPEN
    assert contracts.cancel_scope(db, SimpleNamespace(), scope.project_id, scope.id).status == (
        ScopeStatus.CANCELLED
    )


def test_cursor_round_trip_and_invalid_cursor() -> None:
    identifier = uuid.uuid4()
    cursor = contracts.encode_cursor("Ridge", identifier)
    assert contracts.decode_cursor(cursor) == ("Ridge", str(identifier))
    assert contracts.decode_cursor(None) is None
    with pytest.raises(HTTPException) as error:
        contracts.decode_cursor("not-base64")
    assert error.value.detail["code"] == "invalid_cursor"
    malformed = __import__("base64").urlsafe_b64encode(b'{"not":"a-list"}').decode()
    with pytest.raises(HTTPException) as error:
        contracts.decode_cursor(malformed)
    assert error.value.detail["code"] == "invalid_cursor"


def test_route_mutation_replays_stored_response(monkeypatch) -> None:
    revision = _revision()
    response = RevisionRead.model_validate(revision)
    command = SimpleNamespace(
        response_payload=response.model_dump(mode="json"),
        status=CommandStatus.SUCCEEDED,
    )
    monkeypatch.setattr(routes, "begin_contract_mutation", lambda *_args: (command, True))
    create = MagicMock()
    result = routes._mutation(
        MagicMock(),
        SimpleNamespace(),
        revision.project_id,
        "feature-contract.create",
        "key",
        {},
        create,
        RevisionRead,
    )
    assert result.id == revision.id
    create.assert_not_called()


def test_route_mutation_persists_terminal_response(monkeypatch) -> None:
    revision = _revision()
    command = SimpleNamespace(
        response_payload={},
        status=CommandStatus.PENDING,
        resource_type=None,
        resource_id=None,
        response_status=None,
    )
    monkeypatch.setattr(routes, "begin_contract_mutation", lambda *_args: (command, False))
    db = MagicMock()
    result = routes._mutation(
        db,
        SimpleNamespace(),
        revision.project_id,
        "feature-contract.create",
        "key",
        {},
        lambda: revision,
        RevisionRead,
    )
    assert result.id == revision.id
    assert command.status == CommandStatus.SUCCEEDED
    assert command.response_payload["id"] == str(revision.id)
    db.commit.assert_called_once()


def test_contract_route_wrappers_cover_public_revision_and_scope_mutations(monkeypatch) -> None:
    revision = _revision()
    scope = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=revision.project_id,
        scope_key="scope",
        split_revision_id=uuid.uuid4(),
        experiment_spec_revision_id=uuid.uuid4(),
        canonical_provider="local",
        mode="validation_only",
        expected_members=1,
        status="open",
        membership_digest=None,
        membership_deadline_at=datetime.now(UTC) + timedelta(minutes=5),
        scope_started_at=None,
        scope_deadline_at=None,
        comparison_policy={},
        final_threshold_revision=None,
        created_at=datetime.now(UTC),
    )
    mutate = MagicMock(
        side_effect=[
            RevisionRead.model_validate(revision),
            routes.EvaluationScopeRead.model_validate(scope),
        ]
    )
    monkeypatch.setattr(routes, "_mutation", mutate)
    payload = FeatureContractCreate(
        name="contract",
        task_type=TaskType.REGRESSION,
        target_column="target",
        specification={},
    )
    assert routes.post_feature_contract(
        revision.project_id, payload, MagicMock(), SimpleNamespace(), "key"
    ).id == revision.id
    scope_payload = EvaluationScopeCreate(
        scope_key="scope",
        split_revision_id=scope.split_revision_id,
        experiment_spec_revision_id=scope.experiment_spec_revision_id,
        canonical_provider="local",
        expected_member_count=1,
        mode="validation_only",
        membership_deadline_at=scope.membership_deadline_at,
    )
    assert routes.post_scope(
        revision.project_id, scope_payload, MagicMock(), SimpleNamespace(), "scope-key"
    ).id == scope.id


def test_contract_read_routes_cover_found_and_missing_resources(monkeypatch) -> None:
    monkeypatch.setattr(routes, "require_project_role", lambda *_args: None)
    row = _revision()
    db = MagicMock()
    db.scalar.return_value = row
    assert routes.read_experiment_spec(
        row.project_id, row.id, db, SimpleNamespace()
    ).id == row.id
    assert routes._revision_read(
        db, SimpleNamespace(), row.project_id, routes.FeatureRegistryRevision, row.id
    ).id == row.id
    db.scalar.return_value = None
    with pytest.raises(HTTPException, match="Experiment spec not found"):
        routes.read_experiment_spec(row.project_id, row.id, db, SimpleNamespace())
    with pytest.raises(HTTPException, match="Revision not found"):
        routes._revision_read(
            db, SimpleNamespace(), row.project_id, routes.FeatureRecipeRevision, row.id
        )

    scope = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=row.project_id,
        scope_key="scope",
        split_revision_id=uuid.uuid4(),
        experiment_spec_revision_id=uuid.uuid4(),
        canonical_provider="local",
        mode="validation_only",
        expected_members=1,
        status="open",
        membership_digest=None,
        membership_deadline_at=datetime.now(UTC) + timedelta(minutes=5),
        scope_started_at=None,
        scope_deadline_at=None,
        comparison_policy={},
        final_threshold_revision=None,
        created_at=datetime.now(UTC),
    )
    db.scalar.return_value = scope
    assert routes.read_scope(row.project_id, scope.id, db, SimpleNamespace()).id == scope.id
    db.scalar.return_value = None
    with pytest.raises(HTTPException, match="Evaluation scope not found"):
        routes.read_scope(row.project_id, scope.id, db, SimpleNamespace())


def test_contract_idempotency_conflict_is_typed(monkeypatch) -> None:
    monkeypatch.setattr(
        contracts,
        "begin_command",
        MagicMock(side_effect=contracts.IdempotencyConflict("changed")),
    )
    with pytest.raises(HTTPException) as error:
        contracts.begin_contract_mutation(
            MagicMock(),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            "scope.create",
            "key",
            {},
        )
    assert error.value.status_code == 409


def test_scope_membership_failure_matrix_and_sealing(monkeypatch) -> None:
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    project_id, scope_id, run_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(HTTPException, match="scope not found"):
        contracts.add_scope_member(db, SimpleNamespace(), project_id, scope_id, run_id)

    scope = SimpleNamespace(
        id=scope_id,
        project_id=project_id,
        status=ScopeStatus.OPEN,
        membership_deadline_at=datetime.now(UTC) + timedelta(minutes=5),
        expected_members=2,
        cas_version=0,
    )
    existing = SimpleNamespace(id=uuid.uuid4())
    db.scalar.side_effect = [scope, existing]
    assert (
        contracts.add_scope_member(db, SimpleNamespace(), project_id, scope_id, run_id)
        is existing
    )

    scope.status = ScopeStatus.SEALED
    db.scalar.side_effect = [scope, None]
    with pytest.raises(HTTPException) as error:
        contracts.add_scope_member(db, SimpleNamespace(), project_id, scope_id, run_id)
    assert error.value.detail["code"] == "scope_membership_closed"

    scope.status = ScopeStatus.OPEN
    db.scalar.side_effect = [scope, None, None]
    with pytest.raises(HTTPException, match="run not found"):
        contracts.add_scope_member(db, SimpleNamespace(), project_id, scope_id, run_id)

    run = SimpleNamespace(id=run_id)
    db.scalar.side_effect = [scope, None, run, 2]
    with pytest.raises(HTTPException) as error:
        contracts.add_scope_member(db, SimpleNamespace(), project_id, scope_id, run_id)
    assert error.value.detail["code"] == "scope_membership_full"

    seal = MagicMock()
    monkeypatch.setattr(contracts, "seal_promotional_scope", seal)
    db.scalar.side_effect = [scope, None, run, 1]
    member = contracts.add_scope_member(db, SimpleNamespace(), project_id, scope_id, run_id)
    assert member.ordinal == 1
    seal.assert_called_once_with(db, scope_id, expected_cas_version=0)
    seal.reset_mock()
    scope.expected_members = 3
    db.scalar.side_effect = [scope, None, run, 1]
    assert contracts.add_scope_member(
        db, SimpleNamespace(), project_id, scope_id, uuid.uuid4()
    ).ordinal == 1
    seal.assert_not_called()

    db.scalar.side_effect = [None]
    with pytest.raises(HTTPException, match="scope not found"):
        contracts.cancel_scope(db, SimpleNamespace(), project_id, scope_id)


def test_collection_pagination_filters_and_resource_lookup(monkeypatch) -> None:
    require_run = contracts._require_run
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(contracts, "_require_run", lambda *_args: None)
    project_id, run_id = uuid.uuid4(), uuid.uuid4()
    candidates = []
    for name in ("A", "B"):
        row = TrainingCandidate(
            project_id=project_id,
            model_run_id=run_id,
            candidate_key=name,
            estimator_key=name,
            catalog_revision_id=uuid.uuid4(),
            feature_recipe_revision_id=uuid.uuid4(),
            params={"resource_class": "low"},
            status="pending",
        )
        row.id = uuid.uuid4()
        candidates.append(row)
    db = MagicMock()
    db.scalars.return_value = candidates
    rows, cursor = contracts.run_collection(
        db,
        SimpleNamespace(),
        project_id,
        run_id,
        "candidates",
        cursor=contracts.encode_cursor("0", uuid.uuid4()),
        limit=1,
        status_filter="pending",
        resource_class="low",
    )
    assert rows == candidates[:1]
    assert contracts.decode_cursor(cursor)[0] == "A"

    trial = TrainingTrial(
        project_id=project_id,
        candidate_id=candidates[0].id,
        suggestion_id="suggestion-1",
        params_digest="a" * 64,
    )
    trial.id = uuid.uuid4()
    db.scalars.return_value = [trial]
    rows, cursor = contracts.run_collection(
        db,
        SimpleNamespace(),
        project_id,
        run_id,
        "trials",
        cursor=contracts.encode_cursor("suggestion-0", uuid.uuid4()),
        limit=100,
    )
    assert rows == [trial] and cursor is None

    event = WorkflowEvent(
        project_id=project_id,
        attempt_id=uuid.uuid4(),
        event_key="event-1",
        event_type="attempt.running",
        sequence=1,
    )
    event.id = uuid.uuid4()
    db.scalars.return_value = [event]
    rows, _ = contracts.run_collection(
        db,
        SimpleNamespace(),
        project_id,
        run_id,
        "events",
        cursor=contracts.encode_cursor("event-0", uuid.uuid4()),
        limit=100,
    )
    assert rows == [event]

    db.scalars.return_value = [candidates[0]]
    rows, cursor = contracts.run_collection(
        db,
        SimpleNamespace(),
        project_id,
        run_id,
        "candidates",
        cursor=None,
        limit=100,
    )
    assert rows == [candidates[0]] and cursor is None
    db.scalars.return_value = [trial]
    contracts.run_collection(
        db, SimpleNamespace(), project_id, run_id, "trials", cursor=None, limit=100
    )
    db.scalars.return_value = [event]
    contracts.run_collection(
        db, SimpleNamespace(), project_id, run_id, "events", cursor=None, limit=100
    )

    db.scalar.return_value = candidates[0]
    assert (
        contracts.candidate_for_run(
            db, SimpleNamespace(), project_id, run_id, candidates[0].id
        )
        is candidates[0]
    )
    db.scalar.return_value = None
    with pytest.raises(HTTPException, match="Candidate not found"):
        contracts.candidate_for_run(db, SimpleNamespace(), project_id, run_id, uuid.uuid4())
    with pytest.raises(HTTPException, match="Training run not found"):
        require_run(db, project_id, run_id)
    db.scalar.return_value = run_id
    assert require_run(db, project_id, run_id) is None


def test_event_resume_cursor_and_sse_route(monkeypatch) -> None:
    event = WorkflowEvent(
        project_id=uuid.uuid4(),
        attempt_id=uuid.uuid4(),
        event_key="event-1",
        event_type="attempt.running",
        sequence=1,
        payload={"safe": True},
    )
    event.id = uuid.uuid4()
    db = MagicMock()
    monkeypatch.setattr(contracts, "require_project_role", lambda *_args: None)
    db.scalar.return_value = event
    cursor = contracts.event_cursor_after_id(
        db, SimpleNamespace(), event.project_id, uuid.uuid4(), event.id
    )
    assert contracts.decode_cursor(cursor) == (event.event_key, str(event.id))
    db.scalar.return_value = None
    with pytest.raises(HTTPException) as error:
        contracts.event_cursor_after_id(
            db, SimpleNamespace(), event.project_id, uuid.uuid4(), event.id
        )
    assert error.value.detail["code"] == "invalid_last_event_id"

    monkeypatch.setattr(routes, "run_collection", lambda *_args, **_kwargs: ([event], None))
    request = Request({"type": "http", "headers": [(b"accept", b"text/event-stream")]})
    response = routes.events(
        event.project_id,
        uuid.uuid4(),
        db,
        SimpleNamespace(),
        request,
    )
    assert isinstance(response, StreamingResponse)

    async def body() -> str:
        chunks = [chunk async for chunk in response.body_iterator]
        return "".join(chunk.decode() if isinstance(chunk, bytes) else chunk for chunk in chunks)

    assert f"id: {event.id}" in asyncio.run(body())
    json_request = Request({"type": "http", "headers": []})
    assert routes.events(
        event.project_id, uuid.uuid4(), db, SimpleNamespace(), json_request
    ).items[0]["id"] == event.id

    cursor = contracts.encode_cursor(event.event_key, event.id)
    monkeypatch.setattr(
        routes,
        "event_cursor_after_id",
        lambda *_args: cursor,
    )
    resumed = routes.events(
        event.project_id,
        uuid.uuid4(),
        db,
        SimpleNamespace(),
        json_request,
        last_event_id=str(event.id),
    )
    assert resumed.items[0]["id"] == event.id

    monkeypatch.setattr(
        routes,
        "event_cursor_after_id",
        MagicMock(side_effect=ValueError("invalid")),
    )
    with pytest.raises(HTTPException) as error:
        routes.events(
            event.project_id,
            uuid.uuid4(),
            db,
            SimpleNamespace(),
            json_request,
            last_event_id=str(event.id),
        )
    assert error.value.detail["code"] == "invalid_last_event_id"


def test_scope_member_route_replays_a_completed_mutation(monkeypatch) -> None:
    command = SimpleNamespace(response_payload={"member_id": str(uuid.uuid4()), "ordinal": 2})
    monkeypatch.setattr(routes, "begin_contract_mutation", lambda *_args: (command, True))
    payload = routes.EvaluationScopeMemberCreate(run_id=uuid.uuid4())

    assert routes.post_scope_member(
        uuid.uuid4(), uuid.uuid4(), payload, MagicMock(), SimpleNamespace(), "same-key"
    ) == command.response_payload
