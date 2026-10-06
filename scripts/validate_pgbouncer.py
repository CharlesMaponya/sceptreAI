"""Bounded transaction-pooler qualification against explicitly isolated local *_tests databases."""

import json
import os
import time
from dataclasses import replace
from pathlib import Path

import psycopg
from automl_api.core.config import Settings
from automl_api.db import qualification_session, session
from sqlalchemy import make_url, text
from sqlalchemy.exc import DBAPIError


def main():
    url = make_url(os.environ["PGBOUNCER_TEST_DATABASE_URL"])
    direct = make_url(os.environ["PGBOUNCER_TEST_DIRECT_URL"])
    for target in (url, direct):
        assert target.host in {"localhost", "127.0.0.1"} and target.database.endswith("_tests")
    ca = Path(os.environ["PGBOUNCER_TEST_CA"]).resolve()
    settings = Settings(
        environment="production",
        database_url=url.render_as_string(hide_password=False),
        pgbouncer_transaction_mode=True,
        database_ssl_mode="verify-full",
        database_ssl_root_cert=ca,
        database_application_name="pool-probe-a",
        database_pool_size=1,
        database_max_overflow=0,
        database_statement_timeout_ms=80,
        database_lock_timeout_ms=40,
        database_idle_transaction_timeout_ms=500,
    )
    engines = []

    def engine(config):
        session._engine = None
        session.get_settings = lambda: config
        value = session.get_engine()
        engines.append(value)
        return value

    a = engine(settings)
    b = engine(
        replace(
            settings,
            database_statement_timeout_ms=500,
            database_lock_timeout_ms=120,
            database_idle_transaction_timeout_ms=1000,
            database_application_name="pool-probe-b",
        )
    )
    raw_options = dict(
        host=url.host,
        port=url.port,
        dbname=url.database,
        user=url.username,
        password=url.password,
        sslmode="verify-full",
        sslrootcert=str(ca),
    )
    try:
        with psycopg.connect(**raw_options, autocommit=True) as raw:
            baseline = raw.execute("select current_setting('statement_timeout')").fetchone()[0]
            pids = []
            for candidate, expected, app_name in [
                (a, ("80ms", "40ms", "500ms"), "pool-probe-a"),
                (b, ("500ms", "120ms", "1s"), "pool-probe-b"),
                (a, ("80ms", "40ms", "500ms"), "pool-probe-a"),
            ]:
                with candidate.begin() as connection:
                    assert connection.connection.driver_connection.prepare_threshold is None
                    row = connection.execute(
                        text(
                            "select current_setting('statement_timeout'), "
                            "current_setting('lock_timeout'), "
                            "current_setting('idle_in_transaction_session_timeout')"
                        )
                    ).one()
                    assert tuple(row) == expected
                    assert (
                        connection.execute(text("show application_name")).scalar_one() == app_name
                    )
                    assert connection.execute(
                        text("select ssl from pg_stat_ssl where pid=pg_backend_pid()")
                    ).scalar_one()
                    pids.append(connection.execute(text("select pg_backend_pid()")).scalar_one())
                    for _ in range(8):
                        assert (
                            connection.execute(text("select :value"), {"value": 7}).scalar_one()
                            == 7
                        )
                assert (
                    raw.execute("select current_setting('statement_timeout')").fetchone()[0]
                    == baseline
                )
            assert len(set(pids)) == 1, "Single-backend pool must actually reuse a backend"
            with a.connect() as connection:
                try:
                    connection.execute(text("select pg_sleep(0.3)"))
                except DBAPIError as error:
                    assert error.orig.sqlstate == "57014"
                    connection.rollback()
                else:
                    raise AssertionError("Statement timeout was not enforced")
                assert connection.execute(text("select 1")).scalar_one() == 1
            with b.connect() as connection:
                connection.execute(text("select pg_sleep(0.12)"))
            # A raw client sharing the backend must not inherit either application's limits.
            raw.execute("select pg_sleep(0.12)")
        with psycopg.connect(**{**raw_options, "host": direct.host, "port": direct.port}) as holder:
            holder.execute(
                "create table if not exists phase1_pooler_probe (id integer primary key)"
            )
            holder.execute("insert into phase1_pooler_probe values (1) on conflict do nothing")
            holder.commit()
            holder.execute("select * from phase1_pooler_probe where id=1 for update")
            with a.connect() as connection:
                try:
                    connection.execute(
                        text("select * from phase1_pooler_probe where id=1 for update")
                    )
                except DBAPIError as error:
                    assert error.orig.sqlstate == "55P03"
                    connection.rollback()
                else:
                    raise AssertionError("Lock timeout was not enforced")
            holder.rollback()
        with a.connect() as connection:
            connection.execute(text("select 1"))
            time.sleep(0.8)
            try:
                connection.execute(text("select 1"))
            except DBAPIError:
                connection.rollback()
            else:
                raise AssertionError("Idle transaction timeout was not enforced")
        with a.connect() as connection:
            assert connection.execute(text("select 1")).scalar_one() == 1
        qualification_session._engine = None
        qualification_session.get_settings = lambda: replace(
            settings, environment="test", qualification_database_url=settings.database_url
        )
        authority = qualification_session.get_qualification_engine()
        engines.append(authority)
        with authority.begin() as connection:
            assert connection.execute(text("show statement_timeout")).scalar_one() == "80ms"
            assert connection.execute(text("show application_name")).scalar_one() == (
                "pool-probe-a-qualification"
            )
        with a.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            try:
                connection.execute(text("select 1"))
            except RuntimeError as error:
                assert "transactional" in str(error)
            else:
                raise AssertionError("Autocommit bypassed transaction limits")
        try:
            psycopg.connect(**{**raw_options, "sslmode": "disable"}).close()
        except psycopg.OperationalError:
            pass
        else:
            raise AssertionError("Pooler accepted plaintext")
        try:
            psycopg.connect(
                **{**raw_options, "sslrootcert": "/etc/ssl/certs/ca-certificates.crt"}
            ).close()
        except psycopg.OperationalError:
            pass
        else:
            raise AssertionError("Wrong CA accepted")
        print(
            json.dumps(
                dict(
                    status="passed",
                    backend_reused=True,
                    statement_timeout=True,
                    lock_timeout=True,
                    idle_timeout=True,
                    prepared_statements=False,
                    session_leakage=False,
                    tls_client_and_backend=True,
                    plaintext_rejected=True,
                    wrong_ca_rejected=True,
                )
            )
        )
    finally:
        for candidate in engines:
            candidate.dispose()


if __name__ == "__main__":
    main()
