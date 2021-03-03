"""Utility functions."""
import json
import logging
import os
from enum import Enum
from http.client import responses as http_responses
from typing import TYPE_CHECKING, Any, List, MutableMapping, Sequence, Union

ENV = os.environ.get("DEPLOY_ENV", "test")
DATASET_TYPES: Sequence[str] = ["IMAGE", "RASTER"]

JsonList = List[Any]
JsonObject = MutableMapping[str, Any]

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_ssm import SSMClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SSMClient = object


def error_response(code: int, message: str) -> JsonObject:
    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}"}}


def success_response(code: int, body: Union[JsonList, JsonObject]) -> JsonObject:
    return {"statusCode": code, "body": body}


class ResourceName(Enum):
    DATASETS_TABLE_NAME = f"{ENV}-datasets"
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets-endpoint"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset_versions-endpoint"
    PROCESSING_ASSETS_TABLE_NAME = f"{ENV}-processing-assets"
    STORAGE_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake"
    DATASET_STAGING_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake-staging"


def set_up_logging(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    log_handler = logging.StreamHandler()
    log_level = os.environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger


def get_param(parameter: str, ssm_client: SSMClient, logger: logging.Logger) -> str:
    parameter_response = ssm_client.get_parameter(Name=parameter)

    try:
        return parameter_response["Parameter"]["Value"]
    except KeyError as error:
        logger.warning(json.dumps({"error": error}, default=str))
        raise
