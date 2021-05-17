from enum import Enum

from .environment import ENV


class ResourceName(Enum):
    API_USERS_ROLE_NAME = f"{ENV}-api-users"
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset-versions"
    IMPORT_STATUS_ENDPOINT_FUNCTION_NAME = f"{ENV}-import-status"
    S3_USERS_ROLE_NAME = f"{ENV}-s3-users"
    STAGING_BUCKET_NAME = f"{ENV}-geostore-staging"
    STORAGE_BUCKET_NAME = f"{ENV}-geostore"
