from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "apps" / "api"))

from automl_api.db.base import Base
from automl_api.db.session import get_engine


def main() -> None:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    print(f"Created {len(Base.metadata.tables)} tables.")


if __name__ == "__main__":
    main()
