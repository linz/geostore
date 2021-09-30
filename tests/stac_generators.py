from hashlib import sha256
from random import choice, randrange
from uuid import uuid4

from multihash import SHA2_256

from backend.datasets.create import TITLE_CHARACTERS
from backend.datasets_model import DATASET_KEY_SEPARATOR
from backend.types import JsonObject

from .general_generators import (
    _random_string_choices,
    any_description,
    any_https_url,
    any_name,
    random_string,
)


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
    """Arbitrary-length string of valid dataset title characters"""
    return _random_string_choices(TITLE_CHARACTERS, 20)


def any_dataset_prefix() -> str:
    """Concatenation of dataset title and id"""
    return f"{any_dataset_title()}{DATASET_KEY_SEPARATOR}{any_dataset_id()}"


def any_asset_name() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_dataset_description() -> str:
    """Arbitrary-length string"""
    return random_string(100)


def any_security_classification() -> str:
    return choice(
        [
            "Unclassified",
            "IN-CONFIDENCE",
            "SENSITIVE",
            "RESTRICTED",
            "CONFIDENTIAL",
            "SECRET",
            "TOP-SECRET",
        ]
    )


def any_linz_lifecycle() -> str:
    return choice(["Under Development", "Preview", "Ongoing", "Completed", "Deprecated"])


def any_linz_provider() -> JsonObject:
    return {
        "name": any_name(),
        "description": any_description(),
        "roles": [any_linz_provider_role()],
        "url": any_https_url(),
    }


def any_linz_provider_role() -> str:
    return choice(["manager", "custodian"])


def any_quality_lineage() -> str:
    return random_string(20)


def any_version_version() -> str:
    return f"{randrange(1_000)}.{randrange(1_000)}.{randrange(1_000)}"
