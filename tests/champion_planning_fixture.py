"""Complete plan setup for rollback fixtures; not transaction-boundary evidence."""

from dataclasses import replace

from automl_api.services.champion_planning import RefitInspection
from automl_api.services.champion_planning import plan_scope_refit as select_refit


def plan_scope_refit(db, scope_id, store):
    selected = select_refit(db, scope_id, store)
    if isinstance(selected, RefitInspection):
        selected = select_refit(
            db,
            scope_id,
            store,
            inspection=replace(selected, byte_size=store.stat(selected.uri).byte_size),
        )
    return selected
