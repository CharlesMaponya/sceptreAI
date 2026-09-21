from __future__ import annotations

import base64
import hashlib
import secrets

from argon2 import PasswordHasher as _Argon2Hasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from argon2.low_level import Type as _Argon2Type

# Hash-format identifier for legacy pbkdf2 hashes, not a credential.
PASSWORD_ALGORITHM = "pbkdf2_sha256"  # nosec B105 - algorithm identifier
PASSWORD_ITERATIONS = 390_000
SALT_BYTES = 16

# Argon2id parameters (P6-W04): RFC 9106-recommended profile —
# 64 MiB memory, 3 passes, 4 lanes.
ARGON2_TIME_COST = 3
ARGON2_MEMORY_COST = 65_536
ARGON2_PARALLELISM = 4

_argon2_hasher = _Argon2Hasher(
    time_cost=ARGON2_TIME_COST,
    memory_cost=ARGON2_MEMORY_COST,
    parallelism=ARGON2_PARALLELISM,
    hash_len=32,
    type=_Argon2Type.ID,
)


def hash_password(password: str) -> str:
    """Hash a password with Argon2id (P6-W04)."""
    return _argon2_hasher.hash(password)


def verify_and_rehash(password: str, password_hash: str | None) -> tuple[bool, str | None]:
    """Verify a password and return a fresh hash when parameters moved.

    Returns ``(ok, new_hash_or_None)``. Callers persist ``new_hash`` on the
    user row when it is not ``None`` (rehash-on-login).
    """
    if not password_hash:
        return False, None
    if password_hash.startswith("$argon2"):
        try:
            _argon2_hasher.verify(password_hash, password)
        except (VerifyMismatchError, InvalidHashError):
            return False, None
        needs_rehash_now = _argon2_hasher.check_needs_rehash(password_hash)
        return True, (_argon2_hasher.hash(password) if needs_rehash_now else None)
    # Legacy pbkdf2 hashes verify once and upgrade to Argon2id immediately.
    if _verify_pbkdf2(password, password_hash):
        return True, _argon2_hasher.hash(password)
    return False, None


def verify_password(password: str, password_hash: str | None) -> bool:
    """Backward-compatible boolean verification."""
    ok, _ = verify_and_rehash(password, password_hash)
    return ok


def _verify_pbkdf2(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations_raw, salt_b64, digest_b64 = password_hash.split("$", 3)
        if algorithm != PASSWORD_ALGORITHM:
            return False
        iterations = int(iterations_raw)
        salt = base64.urlsafe_b64decode(salt_b64.encode("ascii"))
        expected_digest = base64.urlsafe_b64decode(digest_b64.encode("ascii"))
    except (ValueError, TypeError):
        return False

    actual_digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return secrets.compare_digest(actual_digest, expected_digest)


def needs_rehash(password_hash: str | None) -> bool:
    """Whether a stored hash should be upgraded to current Argon2id params."""
    if not password_hash or not password_hash.startswith("$argon2"):
        return True
    return _argon2_hasher.check_needs_rehash(password_hash)
