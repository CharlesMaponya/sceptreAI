import base64
from types import SimpleNamespace

import pytest
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services.evaluation_keys import (
    KEY_FIELD,
    ensure_evaluator_key,
    prepare_evaluator_credentials,
)
from automl_api.services.workflow_state import InvalidTransition
from kubernetes.client import ApiException
from sqlalchemy import select
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


class SecretAPI:
    def __init__(self, before_create):
        self.secret = None
        self.before_create = before_create
        self.creates = 0
        self.lose_reply = False
        self.race = False

    def read_namespaced_secret(self, name, namespace):
        if self.secret is None:
            raise ApiException(status=404)
        return self.secret

    def create_namespaced_secret(self, namespace, body):
        self.before_create()
        self.creates += 1
        self.secret = SimpleNamespace(
            immutable=body["immutable"],
            metadata=SimpleNamespace(**body["metadata"]),
            data={
                key: base64.b64encode(value.encode()).decode()
                for key, value in body["stringData"].items()
            },
        )
        if self.lose_reply:
            raise ApiException(status=500)
        if self.race:
            raise ApiException(status=409)
        return self.secret


@pytest.fixture
def key_case(handoff_case):
    db, scope, factory, *_ = handoff_case

    def before_create():
        assert not db.get_bind().in_nested_transaction()
        with factory() as session:
            assert (
                session.scalar(
                    select(WorkflowEvent)
                    .join(WorkflowAttempt, WorkflowEvent.attempt_id == WorkflowAttempt.id)
                    .where(
                        WorkflowAttempt.scope_id == scope.id,
                        WorkflowEvent.event_key == "evaluation_key_intent",
                    )
                )
                is not None
            )

    k8s = SimpleNamespace(
        settings=SimpleNamespace(training_namespace="qa-evaluation"), core=SecretAPI(before_create)
    )
    return handoff_case, k8s


@pytest.mark.parametrize("outcome", ["normal", "lost_reply", "race"])
def test_key_survives_creation_replay_and_binds_authority_plan(key_case, outcome):
    (db, scope, factory, authority, authority_key, _), k8s = key_case
    k8s.core.lose_reply = outcome == "lost_reply"
    k8s.core.race = outcome == "race"
    if k8s.core.lose_reply:
        with pytest.raises(ApiException):
            ensure_evaluator_key(factory, scope.id, k8s)
    name, public_key = ensure_evaluator_key(factory, scope.id, k8s)
    assert ensure_evaluator_key(factory, scope.id, k8s) == (name, public_key)
    plan, token, secret_name = prepare_evaluator_credentials(
        factory, scope.id, k8s, authority, authority_public_key=authority_key
    )
    assert plan.result_public_key == public_key and secret_name == name
    same_plan, same_token, same_name = prepare_evaluator_credentials(
        factory, scope.id, k8s, authority, authority_public_key=authority_key
    )
    assert (same_plan, same_token, same_name) == (plan, token, name)
    assert k8s.core.creates == 1
    events = list(
        db.scalars(
            select(WorkflowEvent)
            .join(WorkflowAttempt, WorkflowEvent.attempt_id == WorkflowAttempt.id)
            .where(WorkflowAttempt.scope_id == scope.id)
        )
    )
    # Check secrets are absent without including any secret value in an assertion message.
    serialized = str([event.payload for event in events])
    assert "PRIVATE KEY" not in serialized
    assert k8s.core.secret.data[KEY_FIELD] not in serialized
    assert token not in serialized


@pytest.mark.parametrize("fault", ["lost_key", "mutable", "owner", "malformed", "namespace"])
def test_recorded_key_cannot_be_rotated_or_rebound(key_case, fault):
    (_, scope, factory, *_), k8s = key_case
    ensure_evaluator_key(factory, scope.id, k8s)
    if fault == "lost_key":
        k8s.core.secret = None
    elif fault == "mutable":
        k8s.core.secret.immutable = False
    elif fault == "owner":
        k8s.core.secret.metadata.labels["automl.platform/evaluation-scope"] = "other"
    elif fault == "malformed":
        k8s.core.secret.data[KEY_FIELD] = base64.b64encode(b"not a key").decode()
    else:
        k8s.settings.training_namespace = "other-namespace"
    with pytest.raises(InvalidTransition):
        ensure_evaluator_key(factory, scope.id, k8s)
    assert k8s.core.creates == 1
