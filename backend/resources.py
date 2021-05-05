from enum import Enum

from .environment import ENV


class ResourceName(Enum):
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset-versions"
    IMPORT_STATUS_ENDPOINT_FUNCTION_NAME = f"{ENV}-import-status"
    USERS_ROLE_NAME = f"{ENV}-data-lake-users"
    READ_ONLY_ROLE_NAME = f"{ENV}-data-lake-readonly"
