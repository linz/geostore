from hashlib import sha256
from random import choice
from uuid import uuid4

from multihash import SHA2_256  # type: ignore[import]

from backend.dataset import DATASET_TYPES

from .general_generators import random_string


def any_hex_multihash() -> str:
    hex_digest = any_sha256_hex_digest()
    return sha256_hex_digest_to_multihash(hex_digest)


def any_sha256_hex_digest() -> str:
    return sha256(random_string(20).encode()).hexdigest()


def sha256_hex_digest_to_multihash(hex_digest: str) -> str:
    return f"{SHA2_256:x}{32:x}{hex_digest}"


def any_dataset_id() -> str:
    return uuid4().hex


def any_dataset_version_id() -> str:
    """Arbitrary-length string"""
    return uuid4().hex


def any_dataset_title() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_asset_name() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_valid_dataset_type() -> str:
    return choice(DATASET_TYPES)


def any_dataset_description() -> str:
    """Arbitrary-length string"""
    return random_string(100)


def any_dataset_owning_group() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_stac_relation() -> str:
    return choice(["child", "root", "self"])
