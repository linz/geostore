"""Utility functions."""
import json
import logging
import os
from enum import Enum
from http.client import responses as http_responses
from typing import Any, List, MutableMapping, Sequence, Union

import boto3

SSM_CLIENT = boto3.client("ssm")

ENV = os.environ.get("DEPLOY_ENV", "test")
DATASET_TYPES: Sequence[str] = ["IMAGE", "RASTER"]

JsonList = List[Any]
JsonObject = MutableMapping[str, Any]


def error_response(code: int, message: str) -> JsonObject:
    logger = set_up_logging(__name__)
    logger.warning(json.dumps({"error": message}, default=str))
    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}"}}


def success_response(code: int, body: Union[JsonList, JsonObject]) -> JsonObject:
    logger = set_up_logging(__name__)
    logger.debug(json.dumps({"success": body}, default=str))
    return {"statusCode": code, "body": body}


class ResourceName(Enum):
    DATASETS_TABLE_NAME = f"{ENV}-datasets"
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets-endpoint"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset_versions-endpoint"
    PROCESSING_ASSETS_TABLE_NAME = f"{ENV}-processing-assets"
    STORAGE_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake"
    DATASET_STAGING_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake-staging"


def get_param(parameter: str) -> str:
    parameter_response = SSM_CLIENT.get_parameter(Name=parameter)

    try:
        parameter = parameter_response["Parameter"]["Value"]
    except KeyError:
        print(parameter_response)
        raise

    return parameter


def set_up_logging(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    log_handler = logging.StreamHandler()
    log_level = os.environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger
