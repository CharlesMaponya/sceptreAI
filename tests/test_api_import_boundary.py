"""Keep worker-only initialization out of every API replica."""

import os
import subprocess
import sys
from pathlib import Path


def test_api_import_does_not_initialize_training_runtime() -> None:
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(root / "apps/api"), str(root / "packages"), env.get("PYTHONPATH", "")]
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import automl_api.main; "
            "assert 'automl_api.training.pipeline' not in sys.modules; "
            "assert 'zenml' not in sys.modules",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert result.returncode == 0, result.stderr
