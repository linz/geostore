"""
Dataset Versions endpoint Lambda function tests.
"""

import logging
from unittest.mock import MagicMock, patch

from pytest import mark

from backend.dataset_versions import entrypoint

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
        "statusCode": 400,
        "body": {"message": "Bad Request: 'metadata-url' is a required property"},
    }


def should_return_required_property_error_when_missing_mandatory_id_property() -> None:
    # Given a missing "id" attribute in the body
    body = {"metadata-url": any_s3_url()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'id' is a required property"},
    }


@mark.infrastructure
def should_return_error_if_dataset_id_does_not_exist_in_db() -> None:
    body = {"id": any_dataset_id(), "metadata-url": any_s3_url()}

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())
    logger.info("Response: %s", response)

    assert response == {
        "statusCode": 404,
        "body": {"message": f"Not Found: dataset '{body['id']}' could not be found"},
    }


@mark.infrastructure
@patch("backend.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution")
def should_return_success_if_dataset_exists(
    start_execution_mock: MagicMock,  # pylint:disable=unused-argument
) -> None:
    # Given a dataset instance
    dataset_id = any_dataset_id()
    body = {"id": dataset_id, "metadata-url": any_s3_url()}

    with Dataset(dataset_id=dataset_id):
        # When requesting the dataset by ID and type
        response = entrypoint.lambda_handler(
            {"httpMethod": "POST", "body": body}, any_lambda_context()
        )
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    assert response["statusCode"] == 201
