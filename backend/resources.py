from enum import Enum

from .environment import ENV


class ResourceName(Enum):
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets-endpoint"
    DATASETS_TABLE_NAME = f"{ENV}-datasets"
    DATASET_STAGING_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake-staging"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset_versions-endpoint"
    PROCESSING_ASSETS_TABLE_NAME = f"{ENV}-processing-assets"
    STORAGE_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake"
