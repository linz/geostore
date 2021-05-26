from typing import Any, Dict

from backend.stac_format import (
    STAC_ASSETS_KEY,
    STAC_CATALOG_TYPE,
    STAC_COLLECTION_TYPE,
    STAC_DESCRIPTION_KEY,
    STAC_EXTENT_BBOX_KEY,
    STAC_EXTENT_KEY,
    STAC_EXTENT_SPATIAL_KEY,
    STAC_EXTENT_TEMPORAL_INTERVAL_KEY,
    STAC_EXTENT_TEMPORAL_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_GEOMETRY_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_ITEM_TYPE,
    STAC_LICENSE_KEY,
    STAC_LINKS_KEY,
    STAC_PROPERTIES_DATETIME_KEY,
    STAC_PROPERTIES_KEY,
    STAC_TYPE_KEY,
    STAC_VERSION_KEY,
)

from .aws_utils import any_s3_url
from .general_generators import any_past_datetime_string
from .stac_generators import (
    any_asset_name,
    any_dataset_description,
    any_dataset_id,
    any_hex_multihash,
)

STAC_VERSION = "1.0.0"

MINIMAL_VALID_STAC_COLLECTION_OBJECT: Dict[str, Any] = {
    STAC_DESCRIPTION_KEY: any_dataset_description(),
    STAC_EXTENT_KEY: {
        STAC_EXTENT_SPATIAL_KEY: {STAC_EXTENT_BBOX_KEY: [[-180, -90, 180, 90]]},
        STAC_EXTENT_TEMPORAL_KEY: {
            STAC_EXTENT_TEMPORAL_INTERVAL_KEY: [[any_past_datetime_string(), None]]
        },
    },
    STAC_ID_KEY: any_dataset_id(),
    STAC_LICENSE_KEY: "MIT",
    STAC_LINKS_KEY: [],
    STAC_VERSION_KEY: STAC_VERSION,
    STAC_TYPE_KEY: STAC_COLLECTION_TYPE,
}

MINIMAL_VALID_STAC_ITEM_OBJECT: Dict[str, Any] = {
    STAC_ASSETS_KEY: {
        any_asset_name(): {STAC_HREF_KEY: any_s3_url(), STAC_FILE_CHECKSUM_KEY: any_hex_multihash()}
    },
    STAC_DESCRIPTION_KEY: any_dataset_description(),
    STAC_GEOMETRY_KEY: None,
    STAC_ID_KEY: any_dataset_id(),
    STAC_LINKS_KEY: [],
    STAC_PROPERTIES_KEY: {STAC_PROPERTIES_DATETIME_KEY: any_past_datetime_string()},
    STAC_VERSION_KEY: STAC_VERSION,
    STAC_TYPE_KEY: STAC_ITEM_TYPE,
}

MINIMAL_VALID_STAC_CATALOG_OBJECT: Dict[str, Any] = {
    STAC_DESCRIPTION_KEY: any_dataset_description(),
    STAC_ID_KEY: any_dataset_id(),
    STAC_LINKS_KEY: [],
    STAC_VERSION_KEY: STAC_VERSION,
    STAC_TYPE_KEY: STAC_CATALOG_TYPE,
}
