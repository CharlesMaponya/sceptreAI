"""Keep legacy formatter/projection tests independent of native custody fixtures."""

import copy
import uuid
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from automl_api.services import operations
from automl_api.services.kubernetes_training import (
    _model_deployment_state,
    _model_deployment_urls,
)
from kubernetes.client import ApiException


def native_dict(value):
    if isinstance(value, SimpleNamespace):
        value = vars(value)
    if isinstance(value, dict):
        return {
            key.split("_")[0] + "".join(part.title() for part in key.split("_")[1:]): native_dict(
                item
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [native_dict(item) for item in value]
    return value


def deployment_state(client, name):
    try:
        deployment = client.apps.read_namespaced_deployment_status(name=name)
    except ApiException as exc:
        if exc.status == 404:
            return "missing"
        raise
    desired = deployment.spec.replicas or 0
    available = deployment.status.available_replicas or 0
    pods = [] if desired > 0 and available >= desired else client.core.list_namespaced_pod().items
    return _model_deployment_state(native_dict(deployment), native_dict(pods))


def deployment_urls(client, name):
    service = client.core.read_namespaced_service(name=name)
    ingress = None
    if client.settings.inference_ingress_enabled:
        try:
            ingress = client.networking.read_namespaced_ingress_status(name=name)
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise
    return (
        _model_deployment_urls(native_dict(service), native_dict(ingress), client.settings) or None
    )


def unit_deployments(db, user, project_id, client):
    actor = SimpleNamespace(id=uuid.uuid4(), token_version=1)

    def request(_db, _actor, run):
        return copy.deepcopy(
            dict(
                run_id=str(run.id),
                snapshot=dict(k8s_job_name=run.k8s_job_name),
                status=run.status,
                tags=run.tags,
            )
        )

    batches = list(db.scalars_values)
    with (
        patch.object(operations, "_deployment_reader", return_value=actor),
        patch.object(operations, "_deployment_observation_request", side_effect=request),
    ):
        selected = operations.list_model_deployments(db, actor, project_id, client)
        if not isinstance(selected, operations.DeploymentInspection):
            return selected
        reads = []
        for read in selected.reads:
            try:
                name = read.request["snapshot"]["k8s_job_name"]
                state = client.model_deployment_state(name)
                urls = (client.model_deployment_urls(name) or {}) if state == "ready" else {}
            except Exception:
                state, urls = "unavailable", {}
            reads.append(replace(read, runtime_state=state, urls=urls))
        db.scalars_values = batches
        return operations.list_model_deployments(
            db, actor, project_id, client, inspection=replace(selected, reads=tuple(reads))
        )
