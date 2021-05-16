from typing import Any, Dict

from backend.check_stac_metadata.utils import (
    STAC_ASSETS_KEY,
    STAC_CATALOG_TYPE,
    STAC_COLLECTION_TYPE,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_ITEM_TYPE,
    STAC_LINKS_KEY,
    STAC_TYPE_KEY,
)

from .aws_utils import any_s3_url
from .general_generators import any_past_datetime_string
from .stac_generators import (
    any_asset_name,
    any_dataset_description,
    any_dataset_id,
    any_hex_multihash,
)

STAC_VERSION = "1.0.0-rc.4"

MINIMAL_VALID_STAC_COLLECTION_OBJECT: Dict[str, Any] = {
    "description": any_dataset_description(),
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[any_past_datetime_string(), None]]},
    },
    "id": any_dataset_id(),
    "license": "MIT",
    STAC_LINKS_KEY: [],
    "stac_version": STAC_VERSION,
    STAC_TYPE_KEY: STAC_COLLECTION_TYPE,
}

MINIMAL_VALID_STAC_ITEM_OBJECT: Dict[str, Any] = {
    STAC_ASSETS_KEY: {
        any_asset_name(): {STAC_HREF_KEY: any_s3_url(), STAC_FILE_CHECKSUM_KEY: any_hex_multihash()}
    },
    "description": any_dataset_description(),
    "geometry": None,
    "id": any_dataset_id(),
    STAC_LINKS_KEY: [],
    "properties": {"datetime": any_past_datetime_string()},
    "stac_version": STAC_VERSION,
    STAC_TYPE_KEY: STAC_ITEM_TYPE,
}

MINIMAL_VALID_STAC_CATALOG_OBJECT: Dict[str, Any] = {
    "description": any_dataset_description(),
    "id": any_dataset_id(),
    STAC_LINKS_KEY: [],
    "stac_version": STAC_VERSION,
    STAC_TYPE_KEY: STAC_CATALOG_TYPE,
}
