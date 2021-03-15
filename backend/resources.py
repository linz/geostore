from enum import Enum

from .environment import ENV


class ResourceName(Enum):
    """Humanly accessed resources with fixed names."""

    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets-endpoint"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset_versions-endpoint"
    IMPORT_STATUS_ENDPOINT_FUNCTION_NAME = (
        f"{ENV}-import_status-endpoint"  # FIXME: rename to start with "DATASET_"
    )
    USERS_ROLE_NAME = f"{ENV}-data-lake-users"

    # FIXME: move to separate class (these resources are not humanly accessed) ?
    DATASETS_TABLE_TITLE_INDEX_NAME = f"{ENV}_datasets_title"
    DATASETS_TABLE_OWNING_GROUP_INDEX_NAME = f"{ENV}_datasets_owning_group"
