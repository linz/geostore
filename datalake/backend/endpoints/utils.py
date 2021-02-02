"""Utility functions."""

import os
from enum import Enum
from http.client import responses as http_responses

ENV = os.environ["DEPLOY_ENV"]


def error_response(code, message):
    """Return error response content as string."""

    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}"}}


def success_response(code, body):
    """Return success response content as string."""

    return {"statusCode": code, "body": body}


class ResourceName(Enum):
    DATASETS_TABLE_NAME = f"{ENV}-datasets"
    DATASETS_ENDPOINT_FUNCTION_NAME = f"{ENV}-datasets-endpoint"
    PROCESSING_ASSETS_TABLE_NAME = f"{ENV}-processing-assets"
    STORAGE_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake"
    DATASET_STAGING_BUCKET_NAME = f"{ENV}-linz-geospatial-data-lake-staging"
