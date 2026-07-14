from __future__ import annotations

import sys
from pathlib import Path

from alembic.config import Config

from alembic import command

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    command.upgrade(config, "head")
    sys.path.insert(0, str(ROOT))
    from scripts.verify_database_schema import main as verify_database_schema

    verify_database_schema()


if __name__ == "__main__":
    main()
