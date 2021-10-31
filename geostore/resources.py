from enum import Enum

from .environment import environment_name

PRODUCTION_ENVIRONMENT_NAME = "prod"


def prefix_non_prod_name(name: str) -> str:
    env_name = environment_name()
    if env_name == PRODUCTION_ENVIRONMENT_NAME:
        return name

    return f"{env_name}-{name}"


class ResourceName(Enum):
    API_USERS_ROLE_NAME = prefix_non_prod_name("api-users")
    CLOUDWATCH_RULE_NAME = prefix_non_prod_name("geostore-cloudwatch-rule")
    DATASETS_ENDPOINT_FUNCTION_NAME = prefix_non_prod_name("datasets")
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = prefix_non_prod_name("dataset-versions")
    IMPORT_STATUS_ENDPOINT_FUNCTION_NAME = prefix_non_prod_name("import-status")
    S3_USERS_ROLE_NAME = prefix_non_prod_name("s3-users")
    STAGING_USERS_ROLE_NAME = prefix_non_prod_name("staging-users")
    STAGING_BUCKET_NAME = prefix_non_prod_name("linz-geostore-staging")
    STORAGE_BUCKET_NAME = prefix_non_prod_name("linz-geostore")
    SNS_TOPIC_NAME = prefix_non_prod_name("geostore-import-status")
