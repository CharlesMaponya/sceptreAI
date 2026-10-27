"""Component tests replay pure SQL plans with mocks; production uses only the factory."""

from unittest.mock import Mock

from automl_api.services import reconciler


def _submit(db, entry, k8s, prepare):
    assert isinstance(k8s, Mock), "Component adapter must never call a real native client"
    plan = prepare(db, entry)
    if plan.manifest is None:
        return plan.attempt
    k8s.ensure_service_account(plan.attempt.workload_identity)
    created = k8s.create_ray_job(plan.manifest)
    return reconciler.publish_ray_submission(db, entry, plan, created)


def submit_training_ray_job(db, entry, k8s):
    return _submit(db, entry, k8s, reconciler.prepare_training_ray_job)


def submit_dataset_ray_job(db, entry, k8s):
    return _submit(db, entry, k8s, reconciler.prepare_dataset_ray_job)
