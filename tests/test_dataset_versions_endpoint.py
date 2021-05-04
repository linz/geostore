"""
Dataset Versions endpoint Lambda function tests.
"""

import logging
from datetime import datetime, timezone
from http import HTTPStatus
from unittest.mock import MagicMock, patch

from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.dataset_versions import entrypoint
from backend.dataset_versions.create import create_dataset_version

from .aws_utils import Dataset, any_lambda_context, any_s3_url
from .stac_generators import any_dataset_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def should_return_required_property_error_when_missing_mandatory_metadata_url() -> None:
    # Given a missing "metadata-url" attribute in the body
    body = {"id": any_dataset_id()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": HTTPStatus.BAD_REQUEST,
        "body": {"message": "Bad Request: 'metadata-url' is a required property"},
    }


def should_return_required_property_error_when_missing_mandatory_id_property() -> None:
    # Given a missing "id" attribute in the body
    body = {"metadata-url": any_s3_url()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": HTTPStatus.BAD_REQUEST,
        "body": {"message": "Bad Request: 'id' is a required property"},
    }


@mark.infrastructure
def should_return_error_if_dataset_id_does_not_exist_in_db() -> None:
    body = {"id": any_dataset_id(), "metadata-url": any_s3_url()}

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())
    logger.info("Response: %s", response)

    assert response == {
        "statusCode": HTTPStatus.NOT_FOUND,
        "body": {"message": f"Not Found: dataset '{body['id']}' could not be found"},
    }


@mark.infrastructure
@patch("backend.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution")
def should_return_success_if_dataset_exists(
    start_execution_mock: MagicMock, subtests: SubTests  # pylint:disable=unused-argument
) -> None:
    # Given a dataset instance
    now = datetime(2001, 2, 3, hour=4, minute=5, second=6, microsecond=789876, tzinfo=timezone.utc)

    with Dataset() as dataset:
        body = {"id": dataset.dataset_id, "metadata-url": any_s3_url(), "now": now.isoformat()}

        # When requesting the dataset by ID and type
        response = create_dataset_version({"body": body})
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    with subtests.test(msg="Status code"):
        assert response["statusCode"] == HTTPStatus.CREATED

    with subtests.test(msg="ID"):
        assert response["body"]["dataset_version"].startswith("2001-02-03T04-05-06-789Z_")
