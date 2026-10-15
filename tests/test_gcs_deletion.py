from types import SimpleNamespace

import pytest
from automl_api.storage.contracts import DeletionPending
from automl_api.storage.gcs import GCSObjectStoreDriver
from google.api_core.exceptions import BadRequest, Forbidden, NotFound


class Generations:
    def __init__(self, *, retained=False, stuck=False, race=False):
        self.live = {
            (name, generation)
            for name in (
                "projects/p/object",
                "projects/p/object-extra",
                "projects/p/object/child",
                "projects/peer/object",
            )
            for generation in range(1, 1003)
        }
        self.soft = set()
        self.deleted = []
        self.retained, self.stuck, self.race = retained, stuck, race
        self.error = None

    def list_blobs(self, bucket, *, prefix, page_size, versions=False, soft_deleted=False):
        assert bucket == "bucket" and page_size == 1000 and versions != soft_deleted
        if self.error and soft_deleted:
            raise self.error
        for name, generation in sorted(self.soft if soft_deleted else self.live, reverse=True):
            if name.startswith(prefix):

                def delete(*, if_generation_match, key=(name, generation)):
                    assert if_generation_match == key[1]
                    self.deleted.append(key)
                    if not self.stuck:
                        self.live.discard(key)
                    if self.retained:
                        self.soft.add(key)
                    if self.race:
                        raise NotFound("already removed")

                yield SimpleNamespace(name=name, generation=generation, delete=delete)


def driver(client):
    result = object.__new__(GCSObjectStoreDriver)
    result.client, result.bucket_name = client, "bucket"
    return result


@pytest.mark.parametrize("exact", [True, False])
@pytest.mark.parametrize("retained", [True, False])
def test_generations_retention_and_replay_preserve_neighbors(exact, retained):
    client = Generations(retained=retained)
    store = driver(client)
    original = client.live.copy()
    targets = {
        key
        for key in original
        if (key[0] == "projects/p/object" if exact else key[0].startswith("projects/p/"))
    }

    def cleanup():
        if exact:
            store.delete("gs://bucket/projects/p/object")
        else:
            store.delete_prefix("projects/p")

    if retained:
        for _ in range(2):
            with pytest.raises(DeletionPending):
                cleanup()
        assert client.soft == targets
        client.soft.clear()  # Provider retention expires between deliveries.
    cleanup()
    cleanup()
    assert client.live == original - targets
    assert set(client.deleted) == targets
    assert len(client.deleted) == len(targets)


def test_no_progress_is_a_failure_and_missing_generation_races_are_replayed():
    client = Generations(stuck=True)
    with pytest.raises(RuntimeError, match="no progress"):
        driver(client).delete_prefix("projects/p")
    client = Generations(race=True)
    driver(client).delete_prefix("projects/p")
    assert all(name.startswith("projects/peer/") for name, _ in client.live)


@pytest.mark.parametrize("error", [Forbidden("denied"), BadRequest("policy disabled")])
def test_retention_listing_errors_do_not_prove_absence(error):
    client = Generations()
    client.live.clear()
    client.error = error
    with pytest.raises(type(error)):
        driver(client).delete_prefix("projects/p")


def test_exact_cleanup_checks_retained_history_after_neighbor_only_pages():
    client = Generations()
    client.live.clear()
    client.soft = {("projects/p/object-extra", 1), ("projects/p/object", 1)}
    with pytest.raises(DeletionPending):
        driver(client).delete("gs://bucket/projects/p/object")
    client.soft.remove(("projects/p/object", 1))
    driver(client).delete("gs://bucket/projects/p/object")
