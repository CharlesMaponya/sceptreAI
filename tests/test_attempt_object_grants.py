"""Committed grant issuance against signed safe copies; provider signing is doubled."""

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import jwt
import pytest
from automl_api.models.enums import AttemptStatus, RunStatus, TaskType, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowEvent,
)
from automl_api.services import attempt_identity
from automl_api.services import attempt_object_grants as grants
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.s3 import S3ObjectStoreDriver
from sqlalchemy import delete, func, select
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def grant_case(source_case, monkeypatch):
    c = source_case
    with c.factory() as db, db.begin():
        run = ModelRun(
            project_id=c.project_id,
            dataset_version_id=c.version_id,
            created_by_id=c.actor_id,
            task_type=TaskType.REGRESSION,
            status=RunStatus.RUNNING,
            target_column="target",
            k8s_namespace="grant-test",
            params={"split_revision_id": str(c.plan.split_revision_id)},
        )
        db.add(run)
        db.flush()
        attempt = WorkflowAttempt(
            project_id=c.project_id,
            model_run_id=run.id,
            stage=WorkflowStage.TRAINING_RUN,
            logical_key=f"training:{run.id}",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            workload_identity="approved-safe-reader",
            status=AttemptStatus.RUNNING,
            lease_owner="reader-owner",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
        )
        db.add(attempt)
        db.flush()
        c.attempt_id, c.run_id, c.fence = attempt.id, run.id, attempt.fencing_token
    c.token = jwt.encode(
        {"exp": int(datetime.now(UTC).timestamp()) + 600},
        "unit-grant-key-at-least-32-bytes-long",
        algorithm="HS256",
    )
    c.pod_uid = str(uuid.uuid4())
    user = SimpleNamespace(
        username="system:serviceaccount:grant-test:approved-safe-reader",
        extra={
            "authentication.kubernetes.io/pod-name": ["reader"],
            "authentication.kubernetes.io/pod-uid": [c.pod_uid],
        },
    )
    c.auth = MagicMock()
    c.auth.create_token_review.return_value = SimpleNamespace(
        status=SimpleNamespace(
            authenticated=True,
            audiences=[attempt_identity.attempt_audience(c.attempt_id, c.fence)],
            user=user,
        )
    )
    monkeypatch.setattr(attempt_identity.client, "AuthenticationV1Api", lambda: c.auth)
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            uid=c.pod_uid,
            namespace="grant-test",
            deletion_timestamp=None,
            labels={
                "automl.platform/project-id": str(c.project_id),
                "automl.platform/attempt-id": str(c.attempt_id),
                "automl.platform/generation": "1",
            },
        ),
        spec=SimpleNamespace(service_account_name="approved-safe-reader"),
    )
    c.k8s = SimpleNamespace(core=SimpleNamespace(read_namespaced_pod=lambda *_a, **_k: pod))
    c.signer = MagicMock(return_value="https://objects.invalid/approved?test-signature=private")

    def sign(*args, **kwargs):
        from urllib.parse import urlencode

        from automl_api.services import refit_access

        url = c.signer(*args, **kwargs)
        return (
            url
            + "&"
            + urlencode(
                {
                    "X-Amz-Date": refit_access.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
                    "X-Amz-Expires": str(kwargs["ExpiresIn"]),
                    "versionId": kwargs["Params"]["VersionId"],
                }
            )
        )

    store = S3ObjectStoreDriver(
        bucket="safe",
        compatible=True,
        endpoint_url="https://objects.invalid",
        access_key="unit-key",
        secret_key="unit-secret",
        presign_client=SimpleNamespace(generate_presigned_url=sign),
        client=SimpleNamespace(
            head_object=lambda **_: pytest.fail(
                "Pinned grants must not inspect the current version"
            )
        ),
    )
    c.capability_store = store
    c.request = grants.InputGrantRequest(request_id=uuid.uuid4(), role="train", object_index=0)
    try:
        yield c
    finally:
        # Fake signer issued no native capability; remove only this fixture's grant metadata.
        with c.factory() as db, db.begin():
            db.execute(
                delete(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == c.attempt_id
                )
            )


def issue(c, request=None, factory=None, store_factory=None):
    return grants.issue_input_grant(
        factory or c.factory,
        c.attempt_id,
        c.token,
        c.k8s,
        request if request is not None else c.request,
        store_factory=store_factory or (lambda: c.capability_store),
    )


def rows(c):
    with c.factory() as db:
        return list(
            db.scalars(
                select(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == c.attempt_id
                )
            )
        )


def test_grant_pins_registered_version_and_replay_returns_no_secret_or_new_deadline(grant_case):
    c = grant_case
    delivered = issue(c)
    assert delivered["url"].startswith("https://objects.invalid/")
    item = next(o for o in c.value.safe_data.objects if o.role == "train")
    sent = c.signer.call_args
    assert sent.args == ("get_object",)
    assert sent.kwargs["Params"]["VersionId"] == item.version
    assert sent.kwargs["Params"]["Bucket"] == "safe"
    assert sent.kwargs["HttpMethod"] == "GET"
    assert 0 < sent.kwargs["ExpiresIn"] <= 60
    replay = issue(c)
    assert replay == {**delivered, "url": None}
    assert c.signer.call_count == 1
    stored = rows(c)
    assert len(stored) == 1 and stored[0].status == "issued"
    assert stored[0].object_uri == item.uri and stored[0].object_version == item.version
    assert c.token not in str(vars(stored[0]))
    assert delivered["url"] not in str(vars(stored[0]))


def test_signer_observes_committed_claim_and_no_sql_connection_or_parent_lock(grant_case):
    c = grant_case

    def sign(*_args, **_kwargs):
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db, db.begin():
            db.scalar(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == c.attempt_id)
                .with_for_update(nowait=True)
            )
            record = db.scalar(
                select(WorkflowAttemptCredentialGrant)
                .where(WorkflowAttemptCredentialGrant.attempt_id == c.attempt_id)
                .with_for_update(nowait=True)
            )
            assert record.status == "issuing" and record.issuance_owner
        return "https://objects.invalid/committed?private-signature"

    c.signer.side_effect = sign
    issue(c)


@pytest.mark.parametrize("change", ["stop", "revoke", "owner", "expiry"])
def test_provider_work_cannot_disclose_after_authority_or_ownership_loss(
    grant_case, monkeypatch, change
):
    c = grant_case
    clock = datetime.now(UTC)

    def sign(*_args, **_kwargs):
        if change == "expiry":

            class Clock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return clock + timedelta(seconds=61)

            monkeypatch.setattr(grants, "datetime", Clock)
            monkeypatch.setattr(attempt_identity, "datetime", Clock)
        else:
            with c.factory() as db, db.begin():
                if change == "stop":
                    db.get(WorkflowAttempt, c.attempt_id).status = AttemptStatus.SUPERSEDED
                elif change == "revoke":
                    grants.revoke_attempt_grants(db, c.attempt_id)
                else:
                    record = db.scalar(
                        select(WorkflowAttemptCredentialGrant).where(
                            WorkflowAttemptCredentialGrant.attempt_id == c.attempt_id
                        )
                    )
                    record.issuance_owner = uuid.uuid4()
        return "https://objects.invalid/should-never-disclose?private-signature"

    c.signer.side_effect = sign
    with pytest.raises(grants.ObjectGrantUnavailable) as error:
        issue(c)
    assert "private-signature" not in str(error.value)
    assert "should-never-disclose" not in str(vars(rows(c)[0]))
    assert (
        rows(c)[0].status
        == {"stop": "unknown", "revoke": "revoked", "owner": "issuing", "expiry": "expired"}[change]
    )


def test_provider_failure_is_unknown_and_same_nonce_never_mints_again(grant_case):
    c = grant_case
    c.signer.side_effect = RuntimeError("https://private.invalid/?signature=secret-provider-value")
    with pytest.raises(grants.ObjectGrantUnavailable) as error:
        issue(c)
    assert str(error.value) == "Object capability unavailable"
    assert rows(c)[0].status == "unknown" and "signature" not in rows(c)[0].failure_reason
    with pytest.raises(StaleFence):
        issue(c)
    assert c.signer.call_count == 1


@pytest.mark.parametrize("autoflush", [True, False])
def test_revocation_transaction_repeated_call_preserves_one_event(grant_case, autoflush):
    c = grant_case
    first = issue(c)
    with c.factory() as db, db.begin():
        db.autoflush = autoflush
        assert grants.revoke_attempt_grants(db, c.attempt_id) == 1
        assert grants.revoke_attempt_grants(db, c.attempt_id) == 0
        db.flush()
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowEvent)
                .where(
                    WorkflowEvent.attempt_id == c.attempt_id,
                    WorkflowEvent.event_key == "attempt.credentials.revoked",
                )
            )
            == 1
        )
        assert grants.unretired_grant_deadline(db, c.attempt_id).isoformat() == first["expires_at"]
    with pytest.raises(StaleFence):
        issue(c, c.request.model_copy(update={"request_id": uuid.uuid4()}))
    assert c.signer.call_count == 1


@pytest.mark.parametrize("autoflush", [True, False])
def test_revocation_transaction_preserves_pending_event_sequence(grant_case, autoflush):
    c = grant_case
    issue(c)
    with c.factory() as db, db.begin():
        db.autoflush = autoflush
        sequence = 1 + db.scalar(
            select(func.max(WorkflowEvent.sequence)).where(WorkflowEvent.attempt_id == c.attempt_id)
        )
        payload = {"attempt_id": str(c.attempt_id)}
        db.add(
            WorkflowEvent(
                project_id=c.project_id,
                attempt_id=c.attempt_id,
                sequence=sequence,
                event_key="test.pending_before_revocation",
                event_type="test.pending_before_revocation",
                payload=payload,
                result_digest=grants.canonical_request_hash(payload),
            )
        )
        assert grants.revoke_attempt_grants(db, c.attempt_id) == 1
        db.flush()
    with c.factory() as db:
        events = list(
            db.scalars(
                select(WorkflowEvent)
                .where(
                    WorkflowEvent.attempt_id == c.attempt_id,
                    WorkflowEvent.sequence >= sequence,
                )
                .order_by(WorkflowEvent.sequence)
            )
        )
        assert [(event.event_key, event.sequence) for event in events] == [
            ("test.pending_before_revocation", sequence),
            ("attempt.credentials.revoked", sequence + 1),
        ]
        assert events[0].payload == payload


def test_revocation_is_idempotent_and_blocks_new_nonces_but_does_not_fake_retirement(grant_case):
    c = grant_case
    first = issue(c)
    with c.factory() as db, db.begin():
        assert grants.revoke_attempt_grants(db, c.attempt_id) == 1
    with c.factory() as db, db.begin():
        assert grants.revoke_attempt_grants(db, c.attempt_id) == 0
        assert grants.unretired_grant_deadline(db, c.attempt_id).isoformat() == first["expires_at"]
    with pytest.raises(StaleFence):
        issue(c, c.request.model_copy(update={"request_id": uuid.uuid4()}))
    assert c.signer.call_count == 1
    with c.factory() as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowEvent).where(
                            WorkflowEvent.attempt_id == c.attempt_id,
                            WorkflowEvent.event_key == "attempt.credentials.revoked",
                        )
                    )
                )
            )
            == 1
        )


@pytest.mark.parametrize(
    "update",
    [
        {"role": "raw"},
        {"role": "final_input"},
        {"role": "final_label"},
        {"method": "PUT"},
        {"object_uri": "s3c://private/raw"},
        {"object_version": "unapproved"},
        {"object_index": True},
        {"object_index": -1},
        {"object_index": 10000},
    ],
)
def test_untrusted_request_cannot_supply_capability_authority(grant_case, update):
    c = grant_case
    body = {**c.request.model_dump(), **update}
    with pytest.raises(ValueError):
        issue(c, body)
    c.auth.create_token_review.assert_not_called()
    c.signer.assert_not_called()
    assert not rows(c)


def test_changed_base_identity_cannot_reuse_a_head_grant(grant_case):
    c = grant_case
    with c.factory() as db, db.begin():
        db.get(WorkflowAttempt, c.attempt_id).ray_job_name = "recorded-ray-job"
    pod = c.k8s.core.read_namespaced_pod()
    pod.spec.service_account_name = "ray-cluster"
    pod.metadata.labels["ray.io/node-type"] = "head"
    pod.metadata.owner_references = [
        SimpleNamespace(kind="RayCluster", controller=True, name="ray-cluster", uid="cluster-uid")
    ]
    review = c.auth.create_token_review.return_value
    review.status.user.username = "system:serviceaccount:grant-test:ray-cluster"
    cluster = {
        "metadata": {
            "name": "ray-cluster",
            "uid": "cluster-uid",
            "ownerReferences": [
                {"kind": "RayJob", "controller": True, "name": "recorded-ray-job", "uid": "job-uid"}
            ],
        }
    }
    job = {"metadata": {"uid": "job-uid", "labels": dict(pod.metadata.labels)}}
    c.k8s.custom = SimpleNamespace(
        get_namespaced_custom_object=lambda *args, **_kwargs: (
            cluster if args[3] == "rayclusters" else job
        )
    )

    def sign(*_args, **_kwargs):
        with c.factory() as db, db.begin():
            db.get(WorkflowAttempt, c.attempt_id).workload_identity = "changed-base-issuer"
        return "https://objects.invalid/not-disclosed?private-signature"

    c.signer.side_effect = sign
    with pytest.raises(grants.ObjectGrantUnavailable):
        issue(c)
    assert c.signer.call_count == 1
    assert rows(c)[0].status == "unknown"


@pytest.mark.parametrize("phase", ["intent", "claim", "publication"])
def test_actual_commit_acknowledgement_loss_preserves_original_intent(grant_case, phase):
    from contextlib import contextmanager

    c = grant_case
    target = {"intent": "pending", "claim": "issuing", "publication": "issued"}[phase]
    lost = []

    @contextmanager
    def factory():
        with c.factory() as db:
            original = db.begin

            @contextmanager
            def transaction():
                with original():
                    yield
                    drop = not lost and any(
                        isinstance(row, WorkflowAttemptCredentialGrant) and row.status == target
                        for row in db.identity_map.values()
                    )
                if drop:
                    lost.append(True)
                    raise OSError("Lost grant commit acknowledgement")

            db.begin = transaction
            yield db

    with pytest.raises((OSError, grants.ObjectGrantUnavailable)):
        issue(c, factory=factory)
    assert lost == [True]
    stored = rows(c)
    assert len(stored) == 1 and stored[0].status == target
    deadline = stored[0].expires_at
    if phase == "intent":
        assert c.signer.call_count == 0
        assert issue(c)["url"] is not None
        assert c.signer.call_count == 1
    elif phase == "claim":
        with pytest.raises(StaleFence):
            issue(c)
        assert c.signer.call_count == 0
    else:
        assert issue(c)["url"] is None
        assert c.signer.call_count == 1
    assert rows(c)[0].expires_at == deadline


def test_concurrent_same_nonce_never_mints_twice(grant_case):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    c = grant_case
    entered, release = Event(), Event()

    def signing(*args, **kwargs):
        entered.set()
        assert release.wait(10), "Concurrent test did not release provider signing"
        return "https://objects.invalid/approved?test-signature=private"

    c.signer.side_effect = signing
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(issue, c)
        try:
            assert entered.wait(10), "First request did not reach signing"
            second = pool.submit(issue, c)
            with pytest.raises(StaleFence):
                second.result(timeout=10)
            assert len(rows(c)) == 1
            assert rows(c)[0].status == "issuing"
            assert c.signer.call_count == 1
        finally:
            release.set()
        delivered = first.result(timeout=10)
    assert delivered["url"]
    replay = issue(c)
    assert replay["url"] is None
    assert replay["grant_id"] == delivered["grant_id"]
    assert replay["expires_at"] == delivered["expires_at"]
    assert c.signer.call_count == 1


def test_same_nonce_cannot_change_registered_object_role(grant_case):
    c = grant_case
    delivered = issue(c)
    with pytest.raises(StaleFence):
        issue(c, c.request.model_copy(update={"role": "validation"}))
    assert c.signer.call_count == 1
    assert len(rows(c)) == 1
    assert issue(c)["expires_at"] == delivered["expires_at"]
