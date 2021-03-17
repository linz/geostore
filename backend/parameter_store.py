import json
import logging
from enum import Enum, auto
from typing import TYPE_CHECKING, Sequence

from .environment import ENV

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_ssm import SSMClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SSMClient = object


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


def get_param(parameter: ParameterName, ssm_client: SSMClient, logger: logging.Logger) -> str:
    parameter_response = ssm_client.get_parameter(Name=parameter.value)

    try:
        return parameter_response["Parameter"]["Value"]
    except KeyError as error:
        logger.error(json.dumps({"error": error}, default=str))
        raise
