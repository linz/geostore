import json
from enum import Enum, auto
from typing import Sequence

import boto3

from .environment import ENV
from .log import set_up_logging

LOGGER = set_up_logging(__name__)
SSM_CLIENT = boto3.client("ssm")


class ParameterName(Enum):
    # Use @staticmethod instead of all the ignores on the next line once we move to Python 3.9
    # <https://github.com/python/mypy/issues/7591>.
    def _generate_next_value_(  # type: ignore[misc,override] # pylint:disable=no-self-argument,no-member
        name: str, _start: int, _count: int, _last_values: Sequence[str]
    ) -> str:
        return f"/{ENV}/{name.lower()}"

    DATASET_VERSION_CREATION_STEP_FUNCTION_ARN = auto()
    S3_BATCH_COPY_ROLE_ARN = auto()
    STORAGE_BUCKET_ARN = auto()


def get_param(parameter: ParameterName) -> str:
    parameter_response = SSM_CLIENT.get_parameter(Name=parameter.value)

    try:
        return parameter_response["Parameter"]["Value"]
    except KeyError as error:
        LOGGER.error(json.dumps({"error": error}, default=str))
        raise
