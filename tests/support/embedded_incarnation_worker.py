"""Hold only the parent's owned local test store lock until process termination."""

import sys
from pathlib import Path

from automl_api.storage.embedded import EmbeddedObjectStoreDriver

store = EmbeddedObjectStoreDriver(root=Path(sys.argv[1]), bucket=sys.argv[2])
with store._locked():
    print("ORIGINAL_LOCAL_LOCK_HELD", flush=True)
    sys.stdin.readline()
