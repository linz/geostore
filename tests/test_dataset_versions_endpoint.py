"""
Dataset Versions endpoint Lambda function tests.
"""

from copy import deepcopy
from datetime import datetime, timezone
from http import HTTPStatus
from logging import INFO, basicConfig, getLogger
from unittest.mock import patch

from pytest import mark
from pytest_subtests import SubTests

from geostore.api_keys import MESSAGE_KEY
from geostore.aws_keys import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from geostore.dataset_versions import entrypoint
from geostore.dataset_versions.create import create_dataset_version
from geostore.step_function_keys import (
    DATASET_ID_SHORT_KEY,
    METADATA_URL_KEY,
    NOW_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)

from .aws_utils import Dataset, any_lambda_context, any_role_arn, any_s3_url
from .stac_generators import any_dataset_id

basicConfig(level=INFO)
logger = getLogger(__name__)


def should_return_error_when_missing_required_property(subtests: SubTests) -> None:
    minimal_body = {
        DATASET_ID_SHORT_KEY: any_dataset_id(),
        METADATA_URL_KEY: any_s3_url(),
        S3_ROLE_ARN_KEY: any_role_arn(),
    }

    for key in minimal_body:
        with subtests.test(msg=key):
            # Given a missing property in the body
            body = deepcopy(minimal_body)
            body.pop(key)

            # When attempting to create the instance
            response = entrypoint.lambda_handler(
                {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
            )

            # Then the API should return an error message
            assert response == {
                STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
                BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{key}' is a required property"},
            }


@mark.infrastructure
def should_return_error_if_dataset_id_does_not_exist_in_db() -> None:
    body = {
        DATASET_ID_SHORT_KEY: any_dataset_id(),
        METADATA_URL_KEY: any_s3_url(),
        S3_ROLE_ARN_KEY: any_role_arn(),
    }

    response = entrypoint.lambda_handler(
        {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
    )
    logger.info("Response: %s", response)

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.NOT_FOUND,
        BODY_KEY: {
            MESSAGE_KEY: f"Not Found: dataset '{body[DATASET_ID_SHORT_KEY]}' could not be found"
        },
    }


@mark.infrastructure
def should_return_success_if_dataset_exists(subtests: SubTests) -> None:
    # Given a dataset instance
    now = datetime(2001, 2, 3, hour=4, minute=5, second=6, microsecond=789876, tzinfo=timezone.utc)

    with patch(
        "geostore.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution"
    ), Dataset() as dataset:
        body = {
            DATASET_ID_SHORT_KEY: dataset.dataset_id,
            METADATA_URL_KEY: any_s3_url(),
            NOW_KEY: now.isoformat(),
            S3_ROLE_ARN_KEY: any_role_arn(),
        }

        # When requesting the dataset by ID and type
        response = create_dataset_version(body)
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    with subtests.test(msg="Status code"):
        assert response[STATUS_CODE_KEY] == HTTPStatus.CREATED

    with subtests.test(msg="ID"):
        assert response[BODY_KEY][VERSION_ID_KEY].startswith("2001-02-03T04-05-06-789Z_")
