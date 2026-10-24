"""Explicit legacy read data for the older status-only mock fixture."""

from dataclasses import replace

from automl_api.models.runs import ModelRun
from automl_api.services.reconciler import observe_analysis_jobs as collect_jobs


def observe_analysis_jobs(db, client):
    for run in db.scalars.return_value:
        for column in ModelRun.__table__.columns:
            if not hasattr(run, column.key):
                setattr(run, column.key, None)
        run.k8s_namespace = "fixture-namespace"
    return collect_jobs(
        db,
        read=lambda request: replace(
            request,
            uid="fixture-only-uid",
            state=client.job_state(request.job_name),
        ),
    )
