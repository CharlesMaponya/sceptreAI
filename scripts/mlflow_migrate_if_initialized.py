from __future__ import annotations

import os
import subprocess

from sqlalchemy import create_engine, inspect, text

database_url = os.environ["MLFLOW_DATABASE_URL"]
engine = create_engine(database_url)
tables = set(inspect(engine).get_table_names())
engine.dispose()

if not tables:
    print("MLflow database is empty; the server will initialize it.", flush=True)
elif "experiments" not in tables:
    raise RuntimeError(
        "MLflow database has a partial schema. Restore it or recreate the dedicated database."
    )
else:
    subprocess.run(["mlflow", "db", "upgrade", database_url], check=True)
    with engine.begin() as connection:
        migrated = connection.execute(
            text(
                """
                UPDATE experiments
                SET artifact_location =
                    'mlflow-artifacts:/' || experiment_id
                WHERE artifact_location =
                    '/mlflow/artifacts/' || experiment_id
                """
            )
        ).rowcount
    print(
        f"Migrated {migrated} MLflow experiment artifact roots to the HTTP proxy.",
        flush=True,
    )
    engine.dispose()
