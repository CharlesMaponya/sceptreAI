"""Committed Kubernetes submission custody and database-free native calls."""

import pytest
from automl_api.models.runs import RunArtifact
from sqlalchemy import event
from test_deployment_object_custody import (
    bound_run as bound_run,
)
from test_deployment_object_custody import (
    capacity_client as capacity_client,
)
from test_deployment_object_custody import (
    committed_tenant as committed_tenant,
)
from test_deployment_object_custody import (
    context_api as context_api,
)
from test_deployment_object_custody import deployment_object as deployment_object
from test_deployment_object_custody import (
    model_api as model_api,
)
from test_deployment_object_custody import (
    phase1_engine as phase1_engine,
)
from test_deployment_object_custody import (
    qualified_model as qualified_model,
)
from test_deployment_object_custody import (
    selected_scope as selected_scope,
)
from test_deployment_object_custody import (
    serving_model as serving_model,
)


def test_deployment_kubernetes_creation_releases_sql(deployment_object):
    c = deployment_object
    active = []

    def checkout(*_):
        active.append(True)

    def checkin(*_):
        active.pop()

    event.listen(c.engine, "checkout", checkout)
    event.listen(c.engine, "checkin", checkin)
    observed = []
    original = c.kubernetes.create_model_deployment

    def create(manifests, **kwargs):
        observed.append(len(active))
        return original(manifests, **kwargs)

    c.kubernetes.create_model_deployment = create
    try:
        assert c.deliver() == 1
        assert observed and set(observed) == {0}
    finally:
        event.remove(c.engine, "checkout", checkout)
        event.remove(c.engine, "checkin", checkin)


def test_deployment_saves_original_kubernetes_creation_receipts(deployment_object):
    c = deployment_object
    assert c.deliver() == 1
    with c.factory() as db:
        journal = db.get(RunArtifact, c.dockerfile_id).artifact_metadata.get(
            "deployment_kubernetes"
        )
        assert isinstance(journal, dict)
        assert journal["intent"]["namespace_uid"]
        assert journal["resources"]
        assert all(
            value["started"] is True and value["receipt"]["uid"]
            for value in journal["resources"].values()
        )


def test_lost_native_creation_ack_recovers_original_uid_without_posting_twice(deployment_object):
    c = deployment_object
    c.object_state["lose_native_ack"] = True
    assert c.deliver() == 1
    assert [kind for kind, _ in c.native_posts] == ["deployment", "service"]
    assert set(count for _, count in c.native_posts + c.native_reads) == {0}
    with c.factory() as db:
        journal = db.get(RunArtifact, c.dockerfile_id).artifact_metadata["deployment_kubernetes"]
        assert all(
            journal["resources"][kind]["receipt"]["uid"] == body["metadata"]["uid"]
            for kind, body in c.native_resources.items()
        )


def test_unknown_creation_reads_only_and_blocks_retirement(deployment_object):
    from automl_api.services.deployment_objects import has_unretired_deployment_writes

    c = deployment_object
    lost = {}

    def disappear(kind):
        lost[kind] = c.native_resources.pop(kind)
        raise OSError("Owned create outcome is unknown")

    c.object_state["during_native_create"] = disappear
    assert c.deliver() == 1
    assert len(c.native_posts) == 1
    assert c.retry() == 1 and len(c.native_posts) == 1
    with c.factory() as db:
        assert has_unretired_deployment_writes(db, c.project_id, [c.deployment_id])
    c.native_resources.update(lost)
    assert c.retry() == 1
    assert [kind for kind, _ in c.native_posts] == ["deployment", "service"]


@pytest.mark.parametrize("fault", ["uid", "spec", "annotation", "namespace", "missing"])
def test_recovery_rejects_replaced_or_changed_native_identity(deployment_object, fault):
    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    c = deployment_object
    c.object_state["kubernetes_failures"] = 1
    assert c.deliver() == 1 and len(c.native_posts) == 2
    original = c.native_resources["deployment"]
    if fault == "uid":
        original["metadata"]["uid"] = "replacement-uid"
    elif fault == "spec":
        original["spec"]["replicas"] += 1
    elif fault == "annotation":
        original["metadata"]["annotations"].clear()
    elif fault == "namespace":
        c.namespace_uid = "replacement-namespace"
    else:
        c.native_resources.pop("deployment")
    assert c.retry() == 1 and len(c.native_posts) == 2
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
        assert db.get(ModelRun, c.deployment_id).status == RunStatus.QUEUED


@pytest.mark.parametrize("change", ["cancel", "source", "lease"])
def test_partial_native_fact_survives_authority_change(deployment_object, change):
    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    c = deployment_object

    def changed(kind):
        assert kind == "deployment"
        with c.factory() as db, db.begin():
            run = db.get(ModelRun, c.deployment_id)
            if change == "cancel":
                run.status = RunStatus.CANCELLED
            elif change == "source":
                run.params = {**run.params, "replicas": 99}
            else:
                db.get(OutboxEntry, c.entry_id).lease_owner = "successor-owner"

    c.object_state["during_native_create"] = changed
    assert c.deliver() == 1 and len(c.native_posts) == 1
    with c.factory() as db:
        journal = db.get(RunArtifact, c.dockerfile_id).artifact_metadata["deployment_kubernetes"]
        assert set(journal["resources"]) == {"deployment"}
        assert (
            journal["resources"]["deployment"]["receipt"]["uid"]
            == c.native_resources["deployment"]["metadata"]["uid"]
        )
        assert db.get(ModelRun, c.deployment_id).status != RunStatus.RUNNING
        if change == "cancel":
            assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        else:
            assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
    if change == "lease":
        from automl_api.services.deployment_objects import reconcile_deployment_object

        reconcile_deployment_object(c.factory, c.entry_id, "successor-owner")
        assert [kind for kind, _ in c.native_posts] == ["deployment", "service"]
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


def test_original_native_receipt_recovers_after_real_sql_backend_death(deployment_object):
    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import OutboxEntry
    from sqlalchemy import text

    c = deployment_object
    updates, killed = [], []

    def terminate(connection, _cursor, statement, parameters, _context, _many):
        if not statement.lstrip().lower().startswith("update run_artifacts "):
            return
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if not any(str(value) == str(c.dockerfile_id) for value in values):
            return
        updates.append(True)
        if len(updates) == 5:
            pid = connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            with c.engine.begin() as control:
                assert control.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(c.engine, "before_cursor_execute", terminate)
    try:
        assert c.deliver() == 1 and len(killed) == 1
        assert [kind for kind, _ in c.native_posts] == ["deployment"]
        assert c.retry() == 1
        assert [kind for kind, _ in c.native_posts] == ["deployment", "service"]
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    finally:
        event.remove(c.engine, "before_cursor_execute", terminate)


@pytest.mark.parametrize("fault", ["missing", "started", "receipt", "request"])
def test_malformed_started_submission_cannot_be_retired(fault):
    import copy
    from types import SimpleNamespace

    from automl_api.services.deployment_submission import unretired_submission

    intent = dict(
        protocol="deployment-kubernetes-v1",
        write_id="a" * 8 + "-aaaa-aaaa-aaaa-" + "a" * 12,
        outbox_id="b" * 8 + "-bbbb-bbbb-bbbb-" + "b" * 12,
        source_digest="c" * 64,
        namespace="owned",
        namespace_uid="original",
        manifest_digest="d" * 64,
        object_receipt_digest="e" * 64,
    )
    metadata = {
        "kubernetes_creation_started": True,
        "deployment_kubernetes": {
            "intent": intent,
            "resources": {"deployment": {"started": True, "request_digest": "f" * 64}},
        },
    }
    if fault == "missing":
        metadata.pop("deployment_kubernetes")
    elif fault == "started":
        metadata.pop("kubernetes_creation_started")
    elif fault == "receipt":
        metadata["deployment_kubernetes"]["resources"]["deployment"]["receipt"] = {"uid": "forged"}
    else:
        metadata["deployment_kubernetes"]["resources"]["deployment"]["request_digest"] = None
    original = copy.deepcopy(metadata)
    assert unretired_submission(SimpleNamespace(artifact_metadata=metadata))
    assert metadata == original


def test_known_create_response_uid_cannot_be_rebound_by_following_read(deployment_object):
    import copy

    c = deployment_object
    original = c.kubernetes.apps.read_namespaced_deployment
    accepted = []

    def replaced(**kwargs):
        resource = original(**kwargs)
        accepted.append(resource["metadata"]["uid"])
        resource = copy.deepcopy(resource)
        resource["metadata"]["uid"] = "replacement-after-accepted-response"
        return resource

    c.kubernetes.apps.read_namespaced_deployment = replaced
    assert c.deliver() == 1 and len(c.native_posts) == 1
    with c.factory() as db:
        receipt = db.get(RunArtifact, c.dockerfile_id).artifact_metadata["deployment_kubernetes"][
            "resources"
        ]["deployment"]["receipt"]
        assert receipt["uid"] == accepted[0]
    assert c.native_resources["deployment"]["metadata"]["uid"] == accepted[0]


def test_ingress_creation_has_its_own_committed_start_and_uid_receipt(deployment_object):
    import copy

    from automl_api.models.workflows import OutboxEntry

    c = deployment_object
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        payload = copy.deepcopy(entry.payload)
        service = payload["manifests"]["service"]
        ingress = dict(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            metadata=copy.deepcopy(service["metadata"]),
            spec={
                "rules": [
                    {
                        "host": "owned.invalid",
                        "http": {
                            "paths": [
                                {
                                    "path": "/",
                                    "pathType": "Prefix",
                                    "backend": {
                                        "service": {
                                            "name": service["metadata"]["name"],
                                            "port": {"number": 8080},
                                        }
                                    },
                                }
                            ]
                        },
                    }
                ]
            },
        )
        payload["manifests"]["ingress"] = ingress
        entry.payload = payload
    assert c.deliver() == 1
    assert [kind for kind, _ in c.native_posts] == ["deployment", "service", "ingress"]
    with c.factory() as db:
        journal = db.get(RunArtifact, c.dockerfile_id).artifact_metadata["deployment_kubernetes"]
        assert (
            journal["resources"]["ingress"]["receipt"]["uid"]
            == c.native_resources["ingress"]["metadata"]["uid"]
        )
    assert set(count for _, count in c.native_reads + c.native_posts) == {0}


def test_submission_transport_disables_sdk_retries(monkeypatch):
    from automl_api.services import kubernetes_training

    monkeypatch.setattr(kubernetes_training.config, "load_incluster_config", lambda: None)
    k8s = kubernetes_training.KubernetesTrainingClient()
    assert k8s._configured
    api = k8s.core.api_client
    try:
        assert api.configuration.retries == 0
        assert api.rest_client.pool_manager.connection_pool_kw["retries"].total == 0
        assert all(
            getattr(k8s, name).api_client is api
            for name in ("apps", "batch", "networking", "scheduling", "custom")
        )
    finally:
        api.close()


def test_deployment_state_publishes_only_after_all_original_native_receipts(deployment_object):
    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    c = deployment_object
    assert c.deliver() == 1
    with c.factory() as db:
        run = db.get(ModelRun, c.deployment_id)
        journal = db.get(RunArtifact, c.dockerfile_id).artifact_metadata["deployment_kubernetes"]
        assert run.status == RunStatus.RUNNING and run.tags["desired_state"] == "deployment_active"
        assert set(journal["resources"]) == {"deployment", "service"}
        assert all(value["receipt"]["uid"] for value in journal["resources"].values())
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    before = list(c.native_posts)
    assert c.deliver() == 0 and c.deliver() == 0
    assert c.native_posts == before


@pytest.mark.parametrize("fault", ["run", "artifact", "content"])
def test_missing_original_deployment_authority_cannot_create_native_resources(
    deployment_object, fault
):
    from automl_api.models.runs import ModelRun
    from sqlalchemy import delete

    c = deployment_object
    with c.factory() as db, db.begin():
        if fault == "run":
            db.execute(delete(ModelRun).where(ModelRun.id == c.deployment_id))
        elif fault == "artifact":
            db.execute(delete(RunArtifact).where(RunArtifact.id == c.dockerfile_id))
        else:
            artifact = db.get(RunArtifact, c.dockerfile_id)
            artifact.artifact_metadata = {**artifact.artifact_metadata, "content": ""}
    assert c.deliver() == 1
    assert not c.put_calls and not c.native_posts


def test_changed_receipt_after_native_return_cannot_publish(deployment_object):
    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    c = deployment_object
    original = c.kubernetes.create_model_deployment

    def changed(manifests, **kwargs):
        result = original(manifests, **kwargs)
        with c.factory() as db, db.begin():
            artifact = db.get(RunArtifact, c.dockerfile_id)
            metadata = dict(artifact.artifact_metadata)
            metadata.pop("deployment_kubernetes")
            artifact.artifact_metadata = metadata
        return result

    c.kubernetes.create_model_deployment = changed
    assert c.deliver() == 1 and len(c.native_posts) == 2
    with c.factory() as db:
        assert db.get(ModelRun, c.deployment_id).status == RunStatus.QUEUED
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


@pytest.mark.parametrize("field", ["uid", "spec_digest"])
def test_well_formed_receipt_change_after_native_return_cannot_publish(deployment_object, field):
    import copy

    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    c = deployment_object
    original = c.kubernetes.create_model_deployment

    def changed(manifests, **kwargs):
        result = original(manifests, **kwargs)
        with c.factory() as db, db.begin():
            artifact = db.get(RunArtifact, c.dockerfile_id)
            metadata = copy.deepcopy(artifact.artifact_metadata)
            metadata["deployment_kubernetes"]["resources"]["deployment"]["receipt"][field] = (
                "0" * 64
            )
            artifact.artifact_metadata = metadata
        return result

    c.kubernetes.create_model_deployment = changed
    assert c.deliver() == 1 and len(c.native_posts) == 2
    with c.factory() as db:
        assert db.get(ModelRun, c.deployment_id).status == RunStatus.QUEUED
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


def test_changed_dockerfile_receipt_after_native_return_cannot_publish(deployment_object):
    import copy

    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    c = deployment_object
    original = c.kubernetes.create_model_deployment

    def changed(manifests, **kwargs):
        result = original(manifests, **kwargs)
        with c.factory() as db, db.begin():
            artifact = db.get(RunArtifact, c.dockerfile_id)
            metadata = copy.deepcopy(artifact.artifact_metadata)
            metadata["deployment_native_write"]["receipt"]["object_version"] = (
                "changed-native-dockerfile-version"
            )
            artifact.artifact_metadata = metadata
        return result

    c.kubernetes.create_model_deployment = changed
    assert c.deliver() == 1 and len(c.native_posts) == 2
    with c.factory() as db:
        assert db.get(ModelRun, c.deployment_id).status == RunStatus.QUEUED
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


def test_pending_deployment_observer_cannot_promote_partial_creation(deployment_object):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from automl_api.models.enums import RunStatus
    from automl_api.models.iam import User
    from automl_api.models.runs import ModelRun
    from automl_api.services import operations

    c = deployment_object
    view = SimpleNamespace(
        settings=c.kubernetes.settings,
        model_deployment_state=Mock(return_value="ready"),
        model_deployment_urls=Mock(return_value={}),
    )
    observations = []

    def observe(kind):
        assert kind == "deployment"
        with c.factory() as db, db.begin():
            results = operations.list_model_deployments(
                db, db.get(User, c.user_id), c.project_id, view
            )
            observations.extend(
                (item.status, item.runtime_state)
                for item in results
                if item.run.id == c.deployment_id
            )

    c.object_state["during_native_create"] = observe
    assert c.deliver() == 1
    assert observations == [(RunStatus.QUEUED, "progressing")]
    view.model_deployment_state.assert_not_called()
    assert [kind for kind, _ in c.native_posts] == ["deployment", "service"]
    with c.factory() as db:
        assert db.get(ModelRun, c.deployment_id).status == RunStatus.RUNNING


@pytest.mark.parametrize("field", ["status", "desired_state"])
def test_changed_phase_after_native_return_cannot_be_overwritten(deployment_object, field):
    from automl_api.models.enums import OutboxStatus, RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    c = deployment_object
    original = c.kubernetes.create_model_deployment
    expected = RunStatus.SUCCEEDED if field == "status" else RunStatus.QUEUED

    def changed(manifests, **kwargs):
        result = original(manifests, **kwargs)
        with c.factory() as db, db.begin():
            run = db.get(ModelRun, c.deployment_id)
            if field == "status":
                run.status = expected
            else:
                run.tags = {**run.tags, "desired_state": "deployment_stopped"}
        return result

    c.kubernetes.create_model_deployment = changed
    assert c.deliver() == 1 and len(c.native_posts) == 2
    with c.factory() as db:
        assert db.get(ModelRun, c.deployment_id).status == expected
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
        if field == "desired_state":
            assert db.get(ModelRun, c.deployment_id).tags["desired_state"] == "deployment_stopped"
