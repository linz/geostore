from typing import Any, Dict

from backend.check_stac_metadata.utils import (
    STAC_CATALOG_TYPE,
    STAC_COLLECTION_TYPE,
    STAC_ITEM_TYPE,
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
    "links": [],
    "stac_version": STAC_VERSION,
    "type": STAC_COLLECTION_TYPE,
}

MINIMAL_VALID_STAC_ITEM_OBJECT: Dict[str, Any] = {
    "assets": {any_asset_name(): {"href": any_s3_url(), "file:checksum": any_hex_multihash()}},
    "description": any_dataset_description(),
    "geometry": None,
    "id": any_dataset_id(),
    "links": [],
    "properties": {"datetime": any_past_datetime_string()},
    "stac_version": STAC_VERSION,
    "type": STAC_ITEM_TYPE,
}

MINIMAL_VALID_STAC_CATALOG_OBJECT: Dict[str, Any] = {
    "description": any_dataset_description(),
    "id": any_dataset_id(),
    "links": [],
    "stac_version": STAC_VERSION,
    "type": STAC_CATALOG_TYPE,
}
