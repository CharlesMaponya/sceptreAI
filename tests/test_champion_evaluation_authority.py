from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.qualification import FinalTestAllocation
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.qualification_control import create_app
from automl_api.services.champion_planning import plan_scope_refit
from automl_api.services.evaluation_authority import prepare_evaluator
from automl_api.services.workflow_state import InvalidTransition
from automl_api.training.champion_refit import (
    FrozenPipeline,
    RefitPlan,
    execute_refit,
    publish_frozen_pipeline,
)
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


@pytest.fixture(autouse=True)
def compatible_uri_fixture(monkeypatch):
    # Exercise the cloud URI contract with a local byte store; this is not S3 evidence.
    from automl_api.storage.embedded import EmbeddedObjectStoreDriver

    original = EmbeddedObjectStoreDriver._path_from_uri
    monkeypatch.setattr(
        EmbeddedObjectStoreDriver, "_uri", lambda self, key: f"s3c://{self.bucket}/{self._key(key)}"
    )
    monkeypatch.setattr(
        EmbeddedObjectStoreDriver,
        "_path_from_uri",
        lambda self, uri: original(self, uri.replace("s3c://", "embedded://", 1)),
    )


@pytest.fixture
def handoff_case(policy_case, authority_api):
    db, store, scope, policy = policy_case
    _, _, kwargs = authority_api
    scope.scope_deadline_at = datetime.now(UTC) + timedelta(seconds=90)
    refit = plan_scope_refit(db, scope.id, store)
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == refit.id, WorkflowEvent.event_key == "refit_plan"
        )
    )
    plan = RefitPlan.model_validate(event.payload)
    refit.status = AttemptStatus.RUNNING
    refit.lease_expires_at = scope.scope_deadline_at
    output = FrozenPipeline.model_validate(execute_refit(plan, store))
    publish_frozen_pipeline(db, plan, output, fencing_token=refit.fencing_token)
    db.flush()

    @contextmanager
    def factory():
        with Session(bind=db.get_bind(), join_transaction_mode="create_savepoint") as session:
            yield session

    allocator = kwargs["principals"][0].model_copy(
        update={"project_reference": policy.final_data.project_reference}
    )
    app = create_app(**{**kwargs, "principals": [allocator]})
    public_key = Ed25519PrivateKey.generate().public_key().public_bytes_raw().hex()
    try:
        with TestClient(
            app, base_url="https://authority.test/", headers={"Authorization": "Bearer allocator"}
        ) as client:
            yield db, scope, factory, client, kwargs["signing_secret"].public_key(), public_key
    finally:
        with kwargs["session_factory"]() as session, session.begin():
            session.execute(
                delete(FinalTestAllocation).where(
                    FinalTestAllocation.project_reference == policy.final_data.project_reference
                )
            )


@pytest.mark.parametrize("lost_path", ["allocations", "/refit", "/evaluators"])
def test_handoff_recovers_lost_authority_response_without_new_attempt(handoff_case, lost_path):
    db, scope, factory, client, authority_key, result_key = handoff_case
    lost = False

    def post(path, **kwargs):
        nonlocal lost
        assert not db.get_bind().in_nested_transaction()
        with factory() as session:
            assert (
                session.scalar(
                    select(WorkflowEvent)
                    .join(WorkflowAttempt, WorkflowEvent.attempt_id == WorkflowAttempt.id)
                    .where(
                        WorkflowAttempt.scope_id == scope.id,
                        WorkflowEvent.event_key == "evaluation_authority_intent",
                    )
                )
                is not None
            )
        response = client.post(path, **kwargs)
        assert response.status_code == 200, response.text
        if path.endswith(lost_path) and not lost:
            lost = True
            raise ConnectionError("Lost acknowledgement")
        return response

    transport = SimpleNamespace(base_url=client.base_url, post=post, get=client.get)
    args = dict(authority_public_key=authority_key, result_public_key=result_key)
    with pytest.raises(ConnectionError):
        prepare_evaluator(factory, scope.id, transport, **args)
    plan, token = prepare_evaluator(factory, scope.id, transport, **args)
    replay, same_token = prepare_evaluator(factory, scope.id, transport, **args)
    assert replay == plan and same_token == token
    registered = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == plan.attempt_id,
            WorkflowEvent.event_key == "authority_registered",
        )
    )
    assert registered.payload["claims"]["exp"] <= int(scope.scope_deadline_at.timestamp())
    assert token not in str(registered.payload)
    assert (
        db.scalar(
            select(func.count())
            .select_from(WorkflowAttempt)
            .where(
                WorkflowAttempt.scope_id == scope.id, WorkflowAttempt.stage == "champion_evaluation"
            )
        )
        == 1
    )
    state = client.get(f"allocations/{plan.allocation_id}").json()
    assert state["status"] == "allocated" and state["cas_version"] == 0
    assert not {"open", "grant_claim", "grant_issued"} & {r["operation"] for r in state["receipts"]}
    with pytest.raises(InvalidTransition, match="intent"):
        prepare_evaluator(
            factory,
            scope.id,
            transport,
            authority_public_key=authority_key,
            result_public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw().hex(),
        )


def test_unexpected_allocation_state_stops_before_refit_or_identity(handoff_case):
    _, scope, factory, client, authority_key, result_key = handoff_case
    posts = []

    def post(path, **kwargs):
        posts.append(path)
        return client.post(path, **kwargs)

    def get(path):
        response = client.get(path)
        payload = response.json()
        payload["project_reference"] = "another-project"
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)

    with pytest.raises(InvalidTransition, match="allocation"):
        prepare_evaluator(
            factory,
            scope.id,
            SimpleNamespace(base_url=client.base_url, post=post, get=get),
            authority_public_key=authority_key,
            result_public_key=result_key,
        )
    assert posts == ["allocations"]
