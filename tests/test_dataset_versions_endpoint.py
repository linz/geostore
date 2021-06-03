"""
Dataset Versions endpoint Lambda function tests.
"""

import logging
from datetime import datetime, timezone
from http import HTTPStatus
from unittest.mock import patch

from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.api_keys import MESSAGE_KEY
from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from backend.dataset_versions import entrypoint
from backend.dataset_versions.create import create_dataset_version
from backend.step_function import DATASET_ID_SHORT_KEY, METADATA_URL_KEY, NOW_KEY, VERSION_ID_KEY

from .aws_utils import Dataset, any_lambda_context, any_s3_url
from .stac_generators import any_dataset_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def should_return_required_property_error_when_missing_mandatory_metadata_url() -> None:
    # Given a missing "metadata_url" attribute in the body
    body = {DATASET_ID_SHORT_KEY: any_dataset_id()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler(
        {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
    )

    # Then the API should return an error message
    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{METADATA_URL_KEY}' is a required property"},
    }


def should_return_required_property_error_when_missing_mandatory_id_property() -> None:
    # Given a missing "id" attribute in the body
    body = {METADATA_URL_KEY: any_s3_url()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler(
        {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
    )

    # Then the API should return an error message
    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{DATASET_ID_SHORT_KEY}' is a required property"},
    }


@mark.infrastructure
def should_return_error_if_dataset_id_does_not_exist_in_db() -> None:
    body = {DATASET_ID_SHORT_KEY: any_dataset_id(), METADATA_URL_KEY: any_s3_url()}

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
        "backend.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution"
    ), Dataset() as dataset:
        body = {
            DATASET_ID_SHORT_KEY: dataset.dataset_id,
            METADATA_URL_KEY: any_s3_url(),
            NOW_KEY: now.isoformat(),
        }

        # When requesting the dataset by ID and type
        response = create_dataset_version(body)
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    with subtests.test(msg="Status code"):
        assert response[STATUS_CODE_KEY] == HTTPStatus.CREATED

    with subtests.test(msg="ID"):
        assert response[BODY_KEY][VERSION_ID_KEY].startswith("2001-02-03T04-05-06-789Z_")
