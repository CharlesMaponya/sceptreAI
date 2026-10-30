"""Managed local incarnations never adopt or unlink a replacement file."""

import errno
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from automl_api.storage.contracts import ByteRange, UploadCapabilityUnavailable, UploadContractError
from automl_api.storage.embedded import EmbeddedObjectStoreDriver


@pytest.fixture
def store(tmp_path):
    return EmbeddedObjectStoreDriver(root=tmp_path, bucket="owned")


@pytest.mark.parametrize("replacement", [b"original", b"replacement"])
def test_original_identity_survives_driver_restart_and_pins_open_file(store, replacement):
    result = store.create_bytes("projects/p/input", b"original")
    version = result.provider_headers["version_id"]
    reopened = EmbeddedObjectStoreDriver(root=store.root, bucket=store.bucket)
    assert reopened.stat(result.uri, version=version).provider_headers == {"version_id": version}
    with reopened.open_stream(result.uri, version=version) as pinned:
        reopened.put_bytes("projects/p/input", replacement)
        assert pinned.read() == b"original"
    for read in (reopened.stat, reopened.open_stream):
        with pytest.raises(FileNotFoundError):
            read(result.uri, version=version)
    reopened.delete(result.uri, version=version)
    assert reopened.read_bytes(result.uri) == replacement


def test_original_conditional_delete_replays_without_erasing_replacement(store):
    result = store.create_bytes("input", b"original")
    version = result.provider_headers["version_id"]
    assert store.open_stream(result.uri, ByteRange(1, 3), version=version).read() == b"rig"
    store.delete(result.uri, version=version)
    store.delete(result.uri, version=version)
    second = store.create_bytes("input", b"replacement")
    assert second.provider_headers["version_id"] != version
    store.delete(result.uri, version=version)
    assert store.read_bytes(second.uri) == b"replacement"


@pytest.mark.parametrize("version", [None, "", "null", "embedded-v1:abc", "x" * 32, 5])
def test_missing_or_malformed_original_version_cannot_be_claimed(store, version):
    from automl_api.storage.contracts import embedded_object_version

    with pytest.raises(UploadContractError):
        embedded_object_version(version)


@pytest.mark.parametrize("corruption", ["missing", "malformed"])
def test_lost_identity_holds_conditional_delete_instead_of_inferred_absence(store, corruption):
    result = store.create_bytes("input", b"original")
    if corruption == "missing":
        os.removexattr(result.storage_path, store._identity_attribute)
    else:
        os.setxattr(result.storage_path, store._identity_attribute, b"changed")
    with pytest.raises(UploadContractError):
        store.delete(result.uri, version=result.provider_headers["version_id"])
    assert store.read_bytes(result.uri) == b"original"


def test_unsupported_identity_filesystem_refuses_qualified_create_but_keeps_overwrite(
    store, monkeypatch
):
    def unavailable(*_):
        raise OSError(errno.ENOTSUP, "No durable identity attributes")

    monkeypatch.setattr(os, "setxattr", unavailable)
    with pytest.raises(UploadCapabilityUnavailable):
        store.create_bytes("new", b"unqualified")
    assert not store.exists(store.uri_for_key("new"))
    assert not list(store.root.rglob(".upload-*"))
    result = store.put_bytes("legacy", b"compatible")
    assert store.read_bytes(result.uri) == b"compatible"


@pytest.mark.parametrize("boundary", ["file", "directory"])
def test_durability_failure_never_returns_false_original_receipt(store, monkeypatch, boundary):
    from stat import S_ISDIR

    fsync = os.fsync

    def fail(descriptor):
        directory = S_ISDIR(os.fstat(descriptor).st_mode)
        if directory == (boundary == "directory"):
            raise OSError("Owned durability failure")
        fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError, match="durability"):
        store.create_bytes("input", b"original")
    assert not list(store.root.rglob(".upload-*"))
    assert store.exists(store.uri_for_key("input")) == (boundary == "directory")


def test_conditional_unlink_and_replacement_share_one_lock(store, monkeypatch):
    result = store.create_bytes("input", b"original")
    read_identity, release_delete, writer_started = (threading.Event() for _ in range(3))
    incarnation = store._incarnation

    def pause(descriptor):
        value = incarnation(descriptor)
        read_identity.set()
        assert release_delete.wait(5)
        return value

    monkeypatch.setattr(store, "_incarnation", pause)

    def replace():
        writer_started.set()
        return store.put_bytes("input", b"replacement")

    with ThreadPoolExecutor(max_workers=2) as pool:
        deleting = pool.submit(
            store.delete, result.uri, version=result.provider_headers["version_id"]
        )
        assert read_identity.wait(5)
        writing = pool.submit(replace)
        assert writer_started.wait(5)
        try:
            assert not writing.done()
            with pytest.raises(TimeoutError):
                writing.result(timeout=0.05)
            assert store.read_bytes(result.uri) == b"original"
        finally:
            release_delete.set()
        deleting.result(timeout=5)
        writing.result(timeout=5)
    assert store.read_bytes(result.uri) == b"replacement"


def test_process_death_releases_lock_without_deleting_original(store, tmp_path):
    import json

    result = store.create_bytes("input", b"original")
    worker = Path(__file__).parent / "support/embedded_incarnation_worker.py"
    child = subprocess.Popen(
        [sys.executable, str(worker), str(store.root), store.bucket],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        import select

        assert select.select([child.stdout], [], [], 10)[0], "Local lock worker did not start"
        assert child.stdout.readline().strip() == "ORIGINAL_LOCAL_LOCK_HELD"
        assert store.read_bytes(result.uri) == b"original"
        with ThreadPoolExecutor(max_workers=1) as pool:
            replacing = pool.submit(store.put_bytes, "input", b"replacement")
            with pytest.raises(TimeoutError):
                replacing.result(timeout=0.05)
            child.kill()
            assert child.wait(timeout=5) == -9
            replacing.result(timeout=5)
        store.delete(result.uri, version=result.provider_headers["version_id"])
        assert store.read_bytes(result.uri) == b"replacement"
        (tmp_path / "local-lock-checkpoint.json").write_text(
            json.dumps(
                dict(
                    pid=child.pid,
                    returncode=child.returncode,
                    replacement_preserved=True,
                    original_delete_replayed=True,
                )
            )
            + "\n"
        )
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        child.stdin.close()
        child.stdout.close()
        child.stderr.close()


@pytest.mark.parametrize("key", ["project/..", "../outside", "project/../input", "\x00"])
def test_incarnation_keys_cannot_escape_namespace(store, key):
    with pytest.raises(ValueError):
        store.create_bytes(key, b"foreign")


def test_symlink_alias_cannot_claim_or_delete_original_incarnation(store):
    original = store.create_bytes("original", b"preserve")
    alias = store.root / store.bucket / "alias"
    alias.symlink_to(original.storage_path)
    for operation in (store.stat, store.open_stream, store.delete):
        with pytest.raises(ValueError):
            operation(store.uri_for_key("alias"), version=original.provider_headers["version_id"])
    assert original.storage_path.read_bytes() == b"preserve"


def test_managed_prefix_deletion_preserves_neighbor_and_never_targets_store_root(store):
    owned = store.create_bytes("projects/p/input", b"owned")
    neighbor = store.create_bytes("projects/p2/input", b"neighbor")
    assert not store.prefix_absent("projects/p")
    for prefix in ("", ".", "..", "/", "projects/.."):
        with pytest.raises(ValueError):
            store.delete_prefix(prefix)
    store.delete_prefix("projects/p")
    store.delete_prefix("projects/p")
    assert store.prefix_absent("projects/p")
    assert not store.exists(owned.uri)
    assert store.read_bytes(neighbor.uri) == b"neighbor"
