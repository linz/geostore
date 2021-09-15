from typing import Any, Dict

from backend.stac_format import (
    LATEST_LINZ_STAC_EXTENSION_URL,
    LINZ_STAC_CREATED_KEY,
    LINZ_STAC_SECURITY_CLASSIFICATION_KEY,
    LINZ_STAC_UPDATED_KEY,
    STAC_ASSETS_KEY,
    STAC_DESCRIPTION_KEY,
    STAC_EXTENSIONS_KEY,
    STAC_EXTENT_BBOX_KEY,
    STAC_EXTENT_KEY,
    STAC_EXTENT_SPATIAL_KEY,
    STAC_EXTENT_TEMPORAL_INTERVAL_KEY,
    STAC_EXTENT_TEMPORAL_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_GEOMETRY_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_LICENSE_KEY,
    STAC_LINKS_KEY,
    STAC_PROPERTIES_DATETIME_KEY,
    STAC_PROPERTIES_KEY,
    STAC_TITLE_KEY,
    STAC_TYPE_CATALOG,
    STAC_TYPE_COLLECTION,
    STAC_TYPE_ITEM,
    STAC_TYPE_KEY,
    STAC_VERSION,
    STAC_VERSION_KEY,
)

from .aws_utils import any_s3_url
from .general_generators import any_past_datetime_string
from .stac_generators import (
    any_asset_name,
    any_dataset_description,
    any_dataset_id,
    any_dataset_title,
    any_hex_multihash,
    any_security_classification,
)

MINIMAL_VALID_STAC_COLLECTION_OBJECT: Dict[str, Any] = {
    LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
    STAC_DESCRIPTION_KEY: any_dataset_description(),
    STAC_EXTENT_KEY: {
        STAC_EXTENT_SPATIAL_KEY: {STAC_EXTENT_BBOX_KEY: [[-180, -90, 180, 90]]},
        STAC_EXTENT_TEMPORAL_KEY: {
            STAC_EXTENT_TEMPORAL_INTERVAL_KEY: [[any_past_datetime_string(), None]]
        },
    },
    STAC_EXTENSIONS_KEY: [LATEST_LINZ_STAC_EXTENSION_URL],
    STAC_ID_KEY: any_dataset_id(),
    STAC_LICENSE_KEY: "MIT",
    STAC_LINKS_KEY: [],
    LINZ_STAC_SECURITY_CLASSIFICATION_KEY: any_security_classification(),
    STAC_VERSION_KEY: STAC_VERSION,
    STAC_TITLE_KEY: any_dataset_title(),
    STAC_TYPE_KEY: STAC_TYPE_COLLECTION,
    LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
}

MINIMAL_VALID_STAC_ITEM_OBJECT: Dict[str, Any] = {
    STAC_ASSETS_KEY: {
        any_asset_name(): {STAC_HREF_KEY: any_s3_url(), STAC_FILE_CHECKSUM_KEY: any_hex_multihash()}
    },
    STAC_GEOMETRY_KEY: None,
    STAC_ID_KEY: any_dataset_id(),
    STAC_LINKS_KEY: [],
    STAC_PROPERTIES_KEY: {STAC_PROPERTIES_DATETIME_KEY: any_past_datetime_string()},
    STAC_VERSION_KEY: STAC_VERSION,
    STAC_TYPE_KEY: STAC_TYPE_ITEM,
}

MINIMAL_VALID_STAC_CATALOG_OBJECT: Dict[str, Any] = {
    STAC_DESCRIPTION_KEY: any_dataset_description(),
    STAC_ID_KEY: any_dataset_id(),
    STAC_LINKS_KEY: [],
    STAC_VERSION_KEY: STAC_VERSION,
    STAC_TYPE_KEY: STAC_TYPE_CATALOG,
}
