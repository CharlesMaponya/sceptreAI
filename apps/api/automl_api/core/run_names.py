"""Readable version names for manually restarted training runs."""

import re


def versioned_run_name(name: str, *, legacy_restart: bool = False, increment: bool = False) -> str:
    base = name.strip()
    restarts = 0
    if legacy_restart:
        suffix = re.search(r"(?:\s+restart)+$", base, flags=re.IGNORECASE)
        if suffix:
            restarts = len(suffix.group().split())
            base = base[: suffix.start()].rstrip()
    version = re.search(r"(?:\s*-\s*|\s+)v(\d+)$", base, flags=re.IGNORECASE)
    number = int(version.group(1)) if version else 1
    if version:
        base = base[: version.start()].rstrip()
    number += restarts + int(increment)
    suffix = f"-v{number}"
    return f"{base[: 255 - len(suffix)].rstrip()}{suffix}"
