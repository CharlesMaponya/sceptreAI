"""Provision the bundled analysis worker role after schema migrations.

External databases supply an operator-provisioned worker Secret instead.
This role limits tables and columns; per-run row isolation remains separate.
"""

import os

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url


def provision(admin_url: str, worker_url: str) -> None:
    admin, worker = make_url(admin_url), make_url(worker_url)
    if not worker.username or not worker.password or worker.username == admin.username:
        raise ValueError("The worker requires a separate database login and password.")
    if (admin.host, admin.port, admin.database) != (worker.host, worker.port, worker.database):
        raise ValueError("The worker credential must reference the application database.")
    role = sql.Identifier(worker.username)
    with psycopg.connect(
        admin.set(drivername="postgresql").render_as_string(hide_password=False)
    ) as db:
        with db.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (worker.username,))
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE ROLE {}").format(role))
            cursor.execute(
                sql.SQL(
                    "ALTER ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS PASSWORD {}"
                ).format(role, sql.Literal(worker.password))
            )
            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(admin.database), role
                )
            )
            cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
            cursor.execute(
                sql.SQL(
                    "GRANT SELECT ON datasets, dataset_versions, dataset_split_revisions, "
                    "model_runs TO {}"
                ).format(role)
            )
            cursor.execute(
                sql.SQL(
                    "GRANT UPDATE (status, started_at, finished_at, updated_at, tags, "
                    "failure_code, failure_message, plain_english_failure) ON model_runs TO {}"
                ).format(role)
            )
            cursor.execute(
                sql.SQL("GRANT SELECT, INSERT ON metrics, run_artifacts TO {}").format(role)
            )


if __name__ == "__main__":
    provision(os.environ["DATABASE_URL"], os.environ["WORKER_DATABASE_URL"])
    print("Analysis worker database grants are ready.")
