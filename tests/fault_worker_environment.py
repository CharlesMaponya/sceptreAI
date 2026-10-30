"""Portable subprocess settings for owned PostgreSQL fault controls."""

import os
from pathlib import Path

from sqlalchemy.engine import make_url


def database_url():
    value = os.environ["SCEPTRE_TEST_DATABASE_URL"]
    if not (make_url(value).database or "").endswith("_tests"):
        raise ValueError("Fault controls require a dedicated _tests database.")
    return value


def environment(engine):
    result = dict(os.environ)
    url = engine.url.render_as_string(hide_password=False)
    if not (engine.url.database or "").endswith("_tests"):
        raise ValueError("Fault controls require a dedicated _tests database.")
    root = Path(__file__).resolve().parents[1]
    result["SCEPTRE_TEST_DATABASE_URL"] = url
    result["PYTHONPATH"] = os.pathsep.join(
        filter(None, (result.get("PYTHONPATH", ""), str(root / "apps/api"), str(root / "tests")))
    )
    return result
