from enum import Enum

from .environment import ENV

PRODUCTION_ENVIRONMENT_NAME = "prod"


def prefix_non_prod_name(name: str) -> str:
    if ENV == PRODUCTION_ENVIRONMENT_NAME:
        return name

    return f"{ENV}-{name}"


class ResourceName(Enum):
    API_USERS_ROLE_NAME = prefix_non_prod_name("api-users")
    DATASETS_ENDPOINT_FUNCTION_NAME = prefix_non_prod_name("datasets")
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = prefix_non_prod_name("dataset-versions")
    IMPORT_STATUS_ENDPOINT_FUNCTION_NAME = prefix_non_prod_name("import-status")
    S3_USERS_ROLE_NAME = prefix_non_prod_name("s3-users")
    STAGING_BUCKET_NAME = prefix_non_prod_name("geostore-staging")
    STORAGE_BUCKET_NAME = prefix_non_prod_name("geostore")
