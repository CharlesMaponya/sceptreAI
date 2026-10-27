"""Original runtime identities and UID-fenced cleanup; native helpers never use SQL."""

from kubernetes.client import ApiException
from sqlalchemy import select

from automl_api.models.enums import CommandStatus, RunKind
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.storage.contracts import DeletionPending


def runtime_targets(db, project_id, runs, *, whole_project):
    from automl_api.services import legacy_analysis, reconciler

    query = select(WorkflowAttempt).where(WorkflowAttempt.project_id == project_id)
    if not whole_project:
        query = query.where(WorkflowAttempt.model_run_id.in_([run.id for run in runs]))
    attempts = list(db.scalars(query))
    from automl_api.services.ray_submission import assert_no_unpublished_creations

    assert_no_unpublished_creations(db, attempts)
    targets = {}

    def add(kind, identity, run, attempt_id, origin):
        name, namespace, uid = (identity.get(key) for key in ("job_name", "namespace", "uid"))
        if any(not isinstance(value, str) or not value.strip() for value in (name, namespace, uid)):
            raise StaleFence("Original runtime identity is incomplete.")
        if run is not None and run.k8s_namespace != namespace:
            raise StaleFence("Original runtime namespace changed.")
        target = dict(
            kind=kind,
            name=name,
            namespace=namespace,
            uid=uid,
            project_id=str(project_id),
            run_id=str(run.id) if run else None,
            attempt_id=str(attempt_id) if attempt_id else None,
            origin=origin,
        )
        labels = {"automl.platform/project-id": str(project_id)}
        if run is not None:
            labels["automl.platform/run-id"] = str(run.id)
        if kind == "RayJob":
            labels["automl.platform/attempt-id"] = str(attempt_id)
            labels["automl.platform/generation"] = str(origin["attempt"]["generation"])
            if run is None:
                labels.update(
                    {
                        "automl.platform/profile-job-id": origin["request"]["profiling_job_id"],
                        "automl.platform/workflow-stage": origin["attempt"]["stage"],
                        "automl.platform/dataset-version-id": origin["attempt"][
                            "dataset_version_id"
                        ],
                    }
                )
        target["labels"] = labels
        key = kind, namespace, name
        if key in targets and targets[key] != target:
            raise StaleFence("Runtime name has conflicting original identity evidence.")
        targets[key] = target

    def origin(attempt, identity, source, *, ray):
        command = db.get(WorkflowCommand, source.command_id, populate_existing=True)
        if command is None or command.project_id != project_id:
            raise StaleFence("Original runtime submission command disappeared.")
        value = dict(
            identity=identity,
            source_id=str(source.id),
            source_digest=canonical_request_hash(source.payload),
            command_id=str(command.id),
            command_actor_id=str(command.actor_id),
            command_request_hash=command.request_hash,
            attempt=dict(
                id=str(attempt.id),
                stage=attempt.stage.value,
                generation=attempt.generation,
                fencing_token=attempt.fencing_token,
                logical_key=attempt.logical_key,
                workload_identity=attempt.workload_identity,
                model_run_id=str(attempt.model_run_id) if attempt.model_run_id else None,
                dataset_version_id=str(attempt.dataset_version_id)
                if attempt.dataset_version_id
                else None,
            ),
        )
        if ray:
            if not set(source.payload) <= {
                "attempt_id",
                "fencing_token",
                "profiling_job_id",
                "credential_protocol",
            }:
                raise StaleFence("Original Ray request has unqualified additional fields.")
            value["request"] = source.payload
        return value

    by_id = {run.id: run for run in runs}
    for attempt in attempts:
        if not attempt.ray_job_name:
            continue
        identity, source = reconciler._ray_submission(db, attempt)
        add(
            "RayJob",
            identity,
            by_id.get(attempt.model_run_id),
            attempt.id,
            origin(attempt, identity, source, ray=True),
        )
    for run in runs:
        if not run.k8s_job_name or run.run_kind == RunKind.DEPLOYMENT:
            continue
        if any(
            target["run_id"] == str(run.id)
            and target["name"] == run.k8s_job_name
            and target["namespace"] == run.k8s_namespace
            for target in targets.values()
        ):
            continue
        kind = "RayJob" if run.tags.get("orchestrator") == "kuberay" else "Job"
        if (kind, run.k8s_namespace, run.k8s_job_name) in targets:
            continue
        analysis = [attempt for attempt in attempts if attempt.model_run_id == run.id]
        found = False
        for attempt in analysis:
            try:
                identity, source = reconciler._analysis_submission(db, attempt)
            except StaleFence:
                continue
            if identity["job_name"] == run.k8s_job_name and kind == "Job":
                add(
                    "Job",
                    identity,
                    run,
                    attempt.id,
                    origin(attempt, identity, source, ray=False),
                )
                found = True
        if found:
            continue
        command_id = run.tags.get("legacy_cleanup_command_id")
        if not command_id or kind != "Job":
            raise DeletionPending("Original runtime UID requires qualification before deletion.")
        command = db.get(WorkflowCommand, command_id, populate_existing=True)
        target = command.request_payload if command else {}
        retired = command.response_payload.get("retired") if command else None
        if (
            command is None
            or command.project_id != project_id
            or command.operation != legacy_analysis.OPERATION
            or command.resource_id != run.id
            or command.resource_type != "model_run"
            or command.status != CommandStatus.SUCCEEDED
            or command.request_hash != canonical_request_hash(target)
            or not isinstance(retired, dict)
            or target.get("project_id") != str(project_id)
            or target.get("run_id") != str(run.id)
            or target.get("job_name") != run.k8s_job_name
            or any(retired.get(key) != target.get(key) for key in ("uid", "namespace", "job_name"))
            or retired.get("request_hash") != command.request_hash
            or not isinstance(retired.get("observed_at"), str)
        ):
            raise StaleFence("Original legacy runtime retirement evidence changed.")
        add(
            "Job",
            target,
            run,
            None,
            dict(command_id=str(command.id), request_hash=command.request_hash, retired=retired),
        )
    return [targets[key] for key in sorted(targets)]


def _metadata(value):
    meta = value.get("metadata") if isinstance(value, dict) else value.metadata
    if isinstance(meta, dict):
        uid, namespace, name = (meta.get(key) for key in ("uid", "namespace", "name"))
        labels = meta.get("labels") or {}
        owners = [owner.get("uid") for owner in meta.get("ownerReferences", [])]
    else:
        uid, namespace, name = meta.uid, meta.namespace, meta.name
        labels = meta.labels or {}
        owners = [owner.uid for owner in meta.owner_references or []]
    if any(not isinstance(v, str) or not v for v in (uid, namespace, name)):
        raise StaleFence("Runtime API returned an incomplete native identity.")
    if not isinstance(labels, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()
    ):
        raise StaleFence("Runtime API returned malformed ownership labels.")
    return dict(uid=uid, namespace=namespace, name=name, labels=labels, owners=owners)


def _read(k8s, target):
    try:
        value = (
            k8s.ray_job(target["name"], namespace=target["namespace"])
            if target["kind"] == "RayJob"
            else k8s.batch.read_namespaced_job(
                name=target["name"], namespace=target["namespace"], _request_timeout=5
            )
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
        return None
    meta = _metadata(value)
    if any(meta[key] != target[key] for key in ("uid", "namespace", "name")):
        raise StaleFence("Original runtime object was replaced or retargeted.")
    if any(meta["labels"].get(key) != value for key, value in target["labels"].items()):
        raise StaleFence("Original runtime ownership labels changed.")
    return meta


def _inventory(k8s, target):
    readers = [k8s.core.list_namespaced_pod]
    if target["kind"] == "RayJob":
        readers.extend(
            [
                k8s.batch.list_namespaced_job,
                lambda **kwargs: k8s.custom.list_namespaced_custom_object(
                    group="ray.io", version="v1", plural="rayclusters", **kwargs
                ),
            ]
        )
    result = []
    for read in readers:
        cursor, seen = None, set()
        for _ in range(100):
            page = read(
                namespace=target["namespace"], limit=250, _continue=cursor, _request_timeout=5
            )
            items = page["items"] if isinstance(page, dict) else page.items
            for item in items:
                meta = _metadata(item)
                if meta["namespace"] != target["namespace"]:
                    raise StaleFence("Runtime inventory escaped its original namespace.")
                result.append(meta)
            cursor = (
                page.get("metadata", {}).get("continue")
                if isinstance(page, dict)
                else page.metadata._continue
                if page.metadata
                else None
            )
            if not cursor:
                break
            if not isinstance(cursor, str) or cursor in seen:
                raise StaleFence("Runtime inventory pagination did not advance.")
            seen.add(cursor)
        else:
            raise DeletionPending("Runtime inventory exceeds the bounded qualification scan.")
    owned = {target["uid"]}
    while True:
        found = {
            item["uid"]
            for item in result
            if owned.intersection(item["owners"])
            or (
                item["labels"].get("automl.platform/project-id") == target["project_id"]
                and (
                    item["labels"].get("automl.platform/attempt-id") == target["attempt_id"]
                    if target["attempt_id"]
                    else item["labels"].get("automl.platform/run-id") == target["run_id"]
                )
            )
        }
        if found <= owned:
            return any(item["uid"] in owned for item in result)
        owned |= found


def cleanup_runtime(k8s, targets, *, inspect_only=False):
    absent = True
    for target in targets:
        current = _read(k8s, target)
        if current is not None and not inspect_only:
            if target["kind"] == "RayJob":
                k8s.delete_ray_job(target["name"], namespace=target["namespace"], uid=target["uid"])
            else:
                k8s.delete_job(target["name"], namespace=target["namespace"], uid=target["uid"])
            current = _read(k8s, target)
        if current is not None or _inventory(k8s, target):
            absent = False
    return absent
