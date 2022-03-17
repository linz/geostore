from enum import Enum

from .environment import environment_name, is_production


def prefix_non_prod_name(name: str) -> str:
    if is_production():
        return name

    return f"{environment_name()}-{name}"


class Resource(Enum):
    @property
    def resource_name(self) -> str:
        return prefix_non_prod_name(self.value)

    API_USERS_ROLE_NAME = "api-users"
    CLOUDWATCH_RULE_NAME = "geostore-cloudwatch-rule"
    DATASETS_ENDPOINT_FUNCTION_NAME = "datasets"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = "dataset-versions"
    IMPORT_STATUS_ENDPOINT_FUNCTION_NAME = "import-status"
    S3_USERS_ROLE_NAME = "s3-users"
    STAGING_USERS_ROLE_NAME = "staging-users"
    STAGING_BUCKET_NAME = "linz-geostore-staging"
    STORAGE_BUCKET_NAME = "linz-geostore"
    SNS_TOPIC_NAME = "geostore-import-status"
