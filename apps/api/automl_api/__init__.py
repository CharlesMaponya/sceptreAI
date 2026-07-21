import os
from importlib.metadata import PackageNotFoundError, version

__all__ = ["__version__"]

try:
    _installed_version = version("smme-tabular-automl")
except PackageNotFoundError:
    _installed_version = "0.0.0"

__version__ = os.getenv("SCEPTRE_VERSION", _installed_version)
