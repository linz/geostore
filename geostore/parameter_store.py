from enum import Enum, auto
from functools import lru_cache
from logging import Logger
from typing import TYPE_CHECKING, Sequence

import boto3
from linz_logger import get_log

from .boto3_config import CONFIG
from .environment import environment_name

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_ssm import SSMClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SSMClient = object  # pragma: no mutate

LOGGER: Logger = get_log()
SSM_CLIENT: SSMClient = boto3.client("ssm", config=CONFIG)
LOG_MESSAGE_PARAMETER_NOT_FOUND = "Parameter:DoesNotExist"


class ParameterName(Enum):
    @staticmethod
    def _generate_next_value_(
        name: str, _start: int, _count: int, _last_values: Sequence[str]
    ) -> str:
        return f"/{environment_name()}/{name.lower()}"

    GIT_COMMIT = auto()
    PROCESSING_ASSETS_TABLE_NAME = auto()
    PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN = auto()
    PROCESSING_IMPORT_ASSET_FILE_FUNCTION_TASK_ARN = auto()
    PROCESSING_IMPORT_DATASET_ROLE_ARN = auto()
    PROCESSING_IMPORT_METADATA_FILE_FUNCTION_TASK_ARN = auto()
    UPDATE_CATALOG_MESSAGE_QUEUE_NAME = auto()
    S3_USERS_ROLE_ARN = auto()
    STATUS_SNS_TOPIC_ARN = auto()
    STORAGE_DATASETS_TABLE_NAME = auto()
    STORAGE_VALIDATION_RESULTS_TABLE_NAME = auto()


@lru_cache
def get_param(parameter: ParameterName) -> str:
    try:
        return SSM_CLIENT.get_parameter(Name=parameter.value)["Parameter"]["Value"]
    except SSM_CLIENT.exceptions.ParameterNotFound:
        LOGGER.error(LOG_MESSAGE_PARAMETER_NOT_FOUND, extra={"parameter_value": parameter.value})
        raise
