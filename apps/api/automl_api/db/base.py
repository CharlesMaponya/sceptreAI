from __future__ import annotations

# Import model modules so Alembic can discover the complete metadata graph.
from automl_api.models import datasets as _datasets  # noqa: F401
from automl_api.models import iam as _iam  # noqa: F401
from automl_api.models import projects as _projects  # noqa: F401
from automl_api.models import runs as _runs  # noqa: F401
from automl_api.models.base import Base

__all__ = ["Base"]
