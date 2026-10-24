"""Single-transaction setup for older fixtures, not SQL-boundary evidence."""

from automl_api.services import reconciler


def observe_analysis_attempts(db, k8s, **kwargs):
    return reconciler.observe_analysis_attempts(
        db,
        k8s,
        read=lambda request: reconciler.inspect_analysis_observation(k8s, request),
        **kwargs,
    )
