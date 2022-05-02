from typing import Any, Dict

from geostore.check_stac_metadata.stac_validators import (
    FILE_STAC_SCHEMA_PATH,
    PROJECTION_STAC_SCHEMA_PATH,
    VERSION_STAC_SCHEMA_PATH,
)
from geostore.stac_format import (
    LINZ_STAC_ASSET_SUMMARIES_KEY,
    LINZ_STAC_CREATED_KEY,
    LINZ_STAC_EXTENSIONS_BASE_URL,
    LINZ_STAC_EXTENSION_URL,
    LINZ_STAC_GEOSPATIAL_TYPE_KEY,
    LINZ_STAC_HISTORY_KEY,
    LINZ_STAC_LIFECYCLE_KEY,
    LINZ_STAC_PROVIDERS_KEY,
    LINZ_STAC_SECURITY_CLASSIFICATION_KEY,
    LINZ_STAC_SECURITY_CLASSIFICATION_UNCLASSIFIED,
    LINZ_STAC_UPDATED_KEY,
    PROJECTION_EPSG_KEY,
    QUALITY_SCHEMA_PATH,
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
    STAC_PROVIDERS_KEY,
    STAC_TITLE_KEY,
    STAC_TYPE_CATALOG,
    STAC_TYPE_COLLECTION,
    STAC_TYPE_ITEM,
    STAC_TYPE_KEY,
    STAC_VERSION,
    STAC_VERSION_KEY,
    VERSION_VERSION_KEY,
)

from .aws_utils import any_s3_url
from .general_generators import any_past_utc_datetime_string
from .stac_generators import (
    any_asset_name,
    any_dataset_description,
    any_dataset_id,
    any_dataset_title,
    any_epsg,
    any_hex_multihash,
    any_linz_asset_summaries,
    any_linz_geospatial_type,
    any_linz_history,
    any_linz_lifecycle,
    any_linz_provider_custodian,
    any_linz_provider_manager,
    any_provider_licensor,
    any_provider_producer,
    any_version_version,
)

STAC_EXTENSIONS_BASE_URL = "https://stac-extensions.github.io"
FILE_STAC_EXTENSION_URL = f"{STAC_EXTENSIONS_BASE_URL}/{FILE_STAC_SCHEMA_PATH}"
PROJECTION_STAC_EXTENSION_URL = f"{STAC_EXTENSIONS_BASE_URL}/{PROJECTION_STAC_SCHEMA_PATH}"
VERSION_STAC_EXTENSION_URL = f"{STAC_EXTENSIONS_BASE_URL}/{VERSION_STAC_SCHEMA_PATH}"

MINIMAL_VALID_STAC_COLLECTION_OBJECT: Dict[str, Any] = {
    LINZ_STAC_ASSET_SUMMARIES_KEY: any_linz_asset_summaries(),
    LINZ_STAC_GEOSPATIAL_TYPE_KEY: any_linz_geospatial_type(),
    LINZ_STAC_HISTORY_KEY: any_linz_history(),
    LINZ_STAC_LIFECYCLE_KEY: any_linz_lifecycle(),
    LINZ_STAC_PROVIDERS_KEY: [any_linz_provider_custodian(), any_linz_provider_manager()],
    LINZ_STAC_SECURITY_CLASSIFICATION_KEY: LINZ_STAC_SECURITY_CLASSIFICATION_UNCLASSIFIED,
    STAC_DESCRIPTION_KEY: any_dataset_description(),
    STAC_EXTENSIONS_KEY: [
        FILE_STAC_EXTENSION_URL,
        LINZ_STAC_EXTENSION_URL,
        f"{LINZ_STAC_EXTENSIONS_BASE_URL}/{QUALITY_SCHEMA_PATH}",
        PROJECTION_STAC_EXTENSION_URL,
        VERSION_STAC_EXTENSION_URL,
    ],
    STAC_EXTENT_KEY: {
        STAC_EXTENT_SPATIAL_KEY: {STAC_EXTENT_BBOX_KEY: [[-180, -90, 180, 90]]},
        STAC_EXTENT_TEMPORAL_KEY: {
            STAC_EXTENT_TEMPORAL_INTERVAL_KEY: [[any_past_utc_datetime_string(), None]]
        },
    },
    STAC_ID_KEY: any_dataset_id(),
    STAC_LICENSE_KEY: "MIT",
    STAC_LINKS_KEY: [],
    STAC_PROVIDERS_KEY: [any_provider_licensor(), any_provider_producer()],
    STAC_TITLE_KEY: any_dataset_title(),
    STAC_TYPE_KEY: STAC_TYPE_COLLECTION,
    STAC_VERSION_KEY: STAC_VERSION,
    VERSION_VERSION_KEY: any_version_version(),
}

MINIMAL_VALID_STAC_ITEM_OBJECT: Dict[str, Any] = {
    STAC_ASSETS_KEY: {
        any_asset_name(): {
            LINZ_STAC_CREATED_KEY: any_past_utc_datetime_string(),
            LINZ_STAC_UPDATED_KEY: any_past_utc_datetime_string(),
            STAC_HREF_KEY: any_s3_url(),
            STAC_FILE_CHECKSUM_KEY: any_hex_multihash(),
        },
    },
    STAC_EXTENSIONS_KEY: [
        FILE_STAC_EXTENSION_URL,
        LINZ_STAC_EXTENSION_URL,
        PROJECTION_STAC_EXTENSION_URL,
        VERSION_STAC_EXTENSION_URL,
    ],
    STAC_GEOMETRY_KEY: None,
    STAC_ID_KEY: any_dataset_id(),
    STAC_LINKS_KEY: [],
    STAC_PROPERTIES_KEY: {
        STAC_PROPERTIES_DATETIME_KEY: any_past_utc_datetime_string(),
        PROJECTION_EPSG_KEY: any_epsg(),
        VERSION_VERSION_KEY: any_version_version(),
    },
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
