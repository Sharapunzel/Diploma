import hashlib
import hmac
import secrets
from datetime import UTC, datetime

from pwdlib import PasswordHash

password_hash = PasswordHash.recommended()
_DUMMY_HASH = password_hash.hash("dummy-password-for-timing")


def hash_password(password: str) -> str:
    return password_hash.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return password_hash.verify(password, hashed)


def dummy_verify(password: str) -> None:
    password_hash.verify(password, _DUMMY_HASH)


def random_token() -> str:
    return secrets.token_urlsafe(32)


def token_digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def secure_digest_equal(left: bytes, right: bytes) -> bool:
    return hmac.compare_digest(left, right)


def utcnow() -> datetime:
    return datetime.now(UTC)
