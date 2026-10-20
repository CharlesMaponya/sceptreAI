"""Validate native read capability bounds before disclosure."""

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit


def validate_native_read_url(url, driver, deadline, version, *, now):
    """Check the capability itself: SDK credential refresh can delay signing."""
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or not parts.netloc
        or parts.username is not None
        or parts.password is not None
        or parts.fragment
    ):
        raise ValueError("Object reads require HTTPS capabilities")
    params = parse_qs(parts.query, keep_blank_values=True)

    def one(name):
        values = params.get(name, [])
        if len(values) != 1 or not values[0]:
            raise ValueError("Provider capability has missing or ambiguous controls")
        return values[0]

    if driver in {"aws_s3", "s3_compatible", "gcs"}:
        prefix = "X-Goog-" if driver == "gcs" else "X-Amz-"
        date = datetime.strptime(one(prefix + "Date"), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        duration = one(prefix + "Expires")
        if not duration.isascii() or not duration.isdecimal() or not 0 < int(duration) <= 900:
            raise ValueError("Provider capability lifetime is invalid")
        expires = date + timedelta(seconds=int(duration))
        version_key = "generation" if driver == "gcs" else "versionId"
    elif driver == "azure_blob":
        expires = datetime.fromisoformat(one("se"))
        if one("sp") != "r" or one("spr") != "https":
            raise ValueError("Provider capability is not HTTPS read-only")
        version_key = "versionid"
    else:
        raise ValueError("Unsupported object capability provider")
    if version is not None and one(version_key) != version:
        raise ValueError("Provider capability selected another object version")
    if expires.tzinfo is None or not now < expires <= deadline:
        raise ValueError("Provider capability exceeds its original deadline or expired")
