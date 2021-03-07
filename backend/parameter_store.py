import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_ssm import SSMClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SSMClient = object


def get_param(parameter: str, ssm_client: SSMClient, logger: logging.Logger) -> str:
    parameter_response = ssm_client.get_parameter(Name=parameter)

    try:
        return parameter_response["Parameter"]["Value"]
    except KeyError as error:
        logger.error(json.dumps({"error": error}, default=str))
        raise
