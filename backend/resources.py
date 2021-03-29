from enum import Enum

from .environment import ENV


class ResourceName(Enum):
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets-endpoint"
    DATASETS_TABLE_NAME = f"{ENV}-datasets"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset-versions-endpoint"
    IMPORT_STATUS_ENDPOINT_FUNCTION_NAME = f"{ENV}-import-status-endpoint"
    USERS_ROLE_NAME = f"{ENV}-data-lake-users"
    VALIDATION_RESULTS_TABLE_NAME = f"{ENV}-validation-results"
