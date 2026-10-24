"""Single-transaction legacy fixture setup, not SQL-boundary qualification."""

from automl_api.services import reconciler
from sqlalchemy.orm import Session


def _fixture_read(db, k8s, request):
    from types import SimpleNamespace

    from automl_api.models.workflows import WorkflowAttempt
    from test_ray_observer_identity import labeled_observation

    def ray_job(*args, **kwargs):
        value = k8s.ray_job(*args, **kwargs)
        if isinstance(value, dict) and "metadata" not in value:
            attempt = (
                db.get(WorkflowAttempt, request.attempt_id)
                if isinstance(db, Session)
                else next(a for a in db.scalars.return_value if a.id == request.attempt_id)
            )
            value = {**labeled_observation(attempt, request.namespace, {}), **value}
        return value

    return reconciler.inspect_ray_observation(SimpleNamespace(ray_job=ray_job), request)


def observe_training_ray_jobs(db, k8s, **kwargs):
    return reconciler.observe_training_ray_jobs(
        db, read=lambda request: _fixture_read(db, k8s, request), **kwargs
    )


def observe_dataset_ray_jobs(db, k8s, **kwargs):
    return reconciler.observe_dataset_ray_jobs(
        db, read=lambda request: _fixture_read(db, k8s, request), **kwargs
    )
