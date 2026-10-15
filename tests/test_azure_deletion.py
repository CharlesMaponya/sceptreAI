from types import SimpleNamespace

import pytest
from automl_api.storage.azure import AzureBlobObjectStoreDriver
from automl_api.storage.contracts import DeletionPending
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.storage.blob import BlobProperties


class History:
    def __init__(self, *, retain=False, stuck=False, race=False):
        self.rows = set()
        for name in (
            "projects/p/object",
            "projects/p/object-extra",
            "projects/p/object/child",
            "projects/peer/object",
        ):
            for version in range(1002):
                self.rows.add((name, str(version), None, version == 1001, False))
            self.rows.add((name, None, "snapshot", None, False))
        self.rows.add(("projects/p/uncommitted", None, None, None, False))
        self.retain, self.stuck, self.race = retain, stuck, race
        self.calls = []
        self.list_error = self.delete_error = None

    def list_blobs(self, *, name_starts_with, include, results_per_page):
        assert set(include) == {"versions", "snapshots", "deleted", "uncommittedblobs"}
        assert results_per_page == 1000
        if self.list_error:
            raise self.list_error
        # Neighbor entries precede exact matches; consumers must scan through SDK pages.
        for name, version, snapshot, current, deleted in sorted(self.rows, key=repr, reverse=True):
            if name.startswith(name_starts_with):
                blob = BlobProperties(name=name)
                blob.version_id, blob.snapshot = version, snapshot
                blob.is_current_version, blob.deleted = current, deleted
                yield blob

    def get_blob_client(self, name, *, version_id=None, snapshot=None):
        def delete_blob(**kwargs):
            self.calls.append((name, version_id, snapshot, kwargs))
            if self.delete_error:
                raise self.delete_error
            assert not (version_id and snapshot)
            assert kwargs == ({} if version_id or snapshot else {"delete_snapshots": "include"})
            matches = [
                row
                for row in self.rows
                if row[0] == name
                and not row[4]
                and (
                    row[1] == version_id
                    if version_id
                    else row[2] == snapshot
                    if snapshot
                    else row[3] or row[1] is None
                )
            ]
            if not matches:
                raise ResourceNotFoundError("already deleted")
            if self.stuck:
                return
            for row in matches:
                self.rows.remove(row)
                if row[3] and not version_id:
                    self.rows.add((*row[:3], False, False))
                elif self.retain:
                    self.rows.add((*row[:4], True))
            if self.race:
                raise ResourceNotFoundError("another cleanup removed it")

        return SimpleNamespace(delete_blob=delete_blob)


def driver(client):
    store = object.__new__(AzureBlobObjectStoreDriver)
    store.container = client
    store.account_name, store.container_name = "account", "data"
    return store


@pytest.mark.parametrize("exact", [True, False])
@pytest.mark.parametrize("retain", [True, False])
def test_history_cleanup_and_retention_preserve_neighbors(exact, retain):
    client = History(retain=retain)
    store = driver(client)
    original = client.rows.copy()
    targets = {
        row
        for row in original
        if (row[0] == "projects/p/object" if exact else row[0].startswith("projects/p/"))
    }

    def cleanup():
        if exact:
            store.delete("az://account/data/projects/p/object")
        else:
            store.delete_prefix("projects/p")

    if retain:
        for _ in range(2):
            with pytest.raises(DeletionPending):
                cleanup()
        assert {row[:3] for row in client.rows if row[4]} == {row[:3] for row in targets}
        client.rows = {row for row in client.rows if not row[4]}
    cleanup()
    calls = client.calls.copy()
    cleanup()
    assert client.calls == calls
    assert client.rows == original - targets
    assert any(version == "1001" for _, version, _, _ in calls)
    assert any(kwargs == {"delete_snapshots": "include"} for _, _, _, kwargs in calls)


def test_no_progress_and_disappearing_snapshot_races():
    with pytest.raises(RuntimeError, match="no progress"):
        driver(History(stuck=True)).delete_prefix("projects/p")
    client = History(race=True)
    driver(client).delete_prefix("projects/p")
    assert all(row[0].startswith("projects/peer/") for row in client.rows)


@pytest.mark.parametrize("operation", ["list", "delete"])
def test_permission_failure_never_certifies_deletion(operation):
    client = History()
    setattr(client, operation + "_error", HttpResponseError("denied"))
    with pytest.raises(HttpResponseError, match="denied"):
        driver(client).delete_prefix("projects/p")


def test_exact_lookup_finds_retained_snapshot_after_neighbor_entries():
    client = History()
    client.rows = {
        ("projects/p/object-extra", None, None, None, True),
        ("projects/p/object", None, "old", None, True),
    }
    with pytest.raises(DeletionPending):
        driver(client).delete("az://account/data/projects/p/object")
    client.rows = {row for row in client.rows if row[0] != "projects/p/object"}
    driver(client).delete("az://account/data/projects/p/object")
    assert not client.calls
