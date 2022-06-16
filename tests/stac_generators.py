from hashlib import sha256
from random import choice, randrange
from uuid import uuid4

from multihash import SHA2_256

from geostore.dataset_properties import TITLE_CHARACTERS
from geostore.stac_format import (
    LINZ_STAC_CREATED_KEY,
    LINZ_STAC_UPDATED_KEY,
    STAC_MAXIMUM_KEY,
    STAC_MINIMUM_KEY,
)
from geostore.types import JsonObject

from .general_generators import (
    _random_string_choices,
    any_description,
    any_https_url,
    any_name,
    any_past_datetime,
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
    return any_dataset_title()


def any_asset_name() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_dataset_description() -> str:
    """Arbitrary-length string"""
    return random_string(100)


def any_linz_asset_summaries() -> JsonObject:
    """
    Semi-arbitrary dates:

    - The first creation date can't be after any of the other dates
    - The last created and first updated dates can be anywhere within the range
    - The last updated date can't be before any of the other dates
    """
    datetimes = [any_past_datetime(), any_past_datetime(), any_past_datetime(), any_past_datetime()]
    return {
        LINZ_STAC_CREATED_KEY: {
            STAC_MINIMUM_KEY: min(datetimes).isoformat(),
            STAC_MAXIMUM_KEY: choice(datetimes).isoformat(),
        },
        LINZ_STAC_UPDATED_KEY: {
            STAC_MINIMUM_KEY: choice(datetimes).isoformat(),
            STAC_MAXIMUM_KEY: max(datetimes).isoformat(),
        },
    }


def any_linz_geospatial_type() -> str:
    return choice(
        [
            "black and white image",
            "circular string",
            "color image",
            "compound curve",
            "curve polygon",
            "geometry",
            "geometry collection",
            "grayscale",
            "grid",
            "hyperspectral",
            "multicurve",
            "multilinestring",
            "multipoint",
            "multipolygon",
            "multispectral",
            "multisurface",
            "linestring",
            "point",
            "point cloud",
            "polygon",
            "polyhedral surface",
            "rgb",
            "tin",
            "triangle",
        ]
    )


def any_linz_history() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_linz_lifecycle() -> str:
    return choice(["under development", "preview", "ongoing", "completed", "deprecated"])


def any_provider(role: str) -> JsonObject:
    return {
        "name": any_name(),
        "description": any_description(),
        "roles": [role],
        "url": any_https_url(),
    }


def any_linz_provider_custodian() -> JsonObject:
    return any_provider("custodian")


def any_linz_provider_manager() -> JsonObject:
    return any_provider("manager")


def any_provider_licensor() -> JsonObject:
    return any_provider("licensor")


def any_provider_producer() -> JsonObject:
    return any_provider("producer")


def any_epsg() -> int:
    return randrange(1_000_000)


def any_version_version() -> str:
    return f"{randrange(1_000)}.{randrange(1_000)}.{randrange(1_000)}"
