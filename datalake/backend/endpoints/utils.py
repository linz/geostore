"""Utility functions."""

import os
from enum import Enum
from http.client import responses as http_responses
from typing import Any, List, MutableMapping, Sequence, Union

ENV = os.environ["DEPLOY_ENV"]
DATASET_TYPES: Sequence[str] = ["IMAGE", "RASTER"]

JSON_LIST = List[Any]
JSON_OBJECT = MutableMapping[str, Any]


def error_response(code: int, message: str) -> JSON_OBJECT:
    """Return error response content as string."""

    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}"}}


def success_response(code: int, body: Union[JSON_LIST, JSON_OBJECT]) -> JSON_OBJECT:
    """Return success response content as string."""

    return {"statusCode": code, "body": body}


class ResourceName(Enum):
    DATASETS_TABLE_NAME = f"{ENV}-datasets"
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets-endpoint"
    DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME = f"{ENV}-dataset_versions-endpoint"
    PROCESSING_ASSETS_TABLE_NAME = f"{ENV}-processing-assets"
    STORAGE_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake"
    DATASET_STAGING_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake-staging"


DATASET_TYPES: Sequence[str] = ["IMAGE", "RASTER"]
