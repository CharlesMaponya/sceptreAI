from __future__ import annotations

import typing
from typing import Any

try:
    import typing_extensions
except Exception:  # pragma: no cover - optional dependency
    typing_extensions = None


def patch_typed_dict_compatibility() -> None:
    if getattr(typing, "_sceptre_typed_dict_patched", False):
        return

    original_meta_new = typing._TypedDictMeta.__new__

    def compat_meta_new(cls, typename: str, bases: tuple[type, ...], ns: dict[str, Any], /, **kwargs: Any):
        kwargs.pop("closed", None)
        kwargs.pop("extra_items", None)
        return original_meta_new(cls, typename, bases, ns, **kwargs)

    typing._TypedDictMeta.__new__ = compat_meta_new
    if typing_extensions is not None and hasattr(typing_extensions, "_TypedDictMeta"):
        typing_extensions._TypedDictMeta.__new__ = compat_meta_new

    original_typed_dict = typing.TypedDict

    def compat_typed_dict(typename: str, fields=None, /, *, total=True, **kwargs: Any):
        kwargs.pop("closed", None)
        kwargs.pop("extra_items", None)
        if fields is None:
            return original_typed_dict(typename, total=total, **kwargs)
        return original_typed_dict(typename, fields, total=total, **kwargs)

    typing.TypedDict = compat_typed_dict
    if typing_extensions is not None:
        typing_extensions.TypedDict = compat_typed_dict

    typing._sceptre_typed_dict_patched = True


patch_typed_dict_compatibility()
