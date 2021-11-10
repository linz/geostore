from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pynamodb.exceptions import DoesNotExist
from pytest import mark

from geostore.aws_keys import BODY_KEY, HTTP_METHOD_KEY
from geostore.dataset_versions.create import create_dataset_version
from geostore.logging_keys import (
    LOG_MESSAGE_LAMBDA_FAILURE,
    LOG_MESSAGE_LAMBDA_START,
    LOG_MESSAGE_STEP_FUNCTION_RESPONSE,
)
from geostore.step_function_keys import DATASET_ID_SHORT_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY

from .aws_utils import Dataset, any_role_arn, any_s3_url
from .general_generators import any_error_message
from .stac_generators import any_dataset_id


@mark.infrastructure
def should_log_payload() -> None:
    # Given
    with patch(
        "geostore.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution"
    ), Dataset() as dataset, patch("geostore.dataset_versions.create.LOGGER.debug") as logger_mock:
        event = {
            HTTP_METHOD_KEY: "POST",
            BODY_KEY: {
                METADATA_URL_KEY: any_s3_url(),
                DATASET_ID_SHORT_KEY: dataset.dataset_id,
                S3_ROLE_ARN_KEY: any_role_arn(),
            },
        }

        # When
        create_dataset_version(event)

        # Then
        logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_START, lambda_input=event)


@mark.infrastructure
@patch("geostore.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution")
def should_log_step_function_state_machine_response(start_execution_mock: MagicMock) -> None:
    # Given
    start_execution_mock.return_value = step_function_response = {"executionArn": "Some Response"}

    with Dataset() as dataset, patch(
        "geostore.dataset_versions.create.LOGGER.debug"
    ) as logger_mock:
        event = {
            METADATA_URL_KEY: any_s3_url(),
            DATASET_ID_SHORT_KEY: dataset.dataset_id,
            S3_ROLE_ARN_KEY: any_role_arn(),
        }

        # When
        create_dataset_version(event)

        # Then
        logger_mock.assert_any_call(
            LOG_MESSAGE_STEP_FUNCTION_RESPONSE, response=step_function_response
        )


@patch("geostore.dataset_versions.create.validate")
def should_log_missing_argument_warning(validate_schema_mock: MagicMock) -> None:
    # given
    error_message = any_error_message()
    validate_schema_mock.side_effect = ValidationError(error_message)

    payload = {HTTP_METHOD_KEY: "POST", BODY_KEY: {}}

    with patch("geostore.dataset_versions.create.LOGGER.warning") as logger_mock:
        # when
        create_dataset_version(payload)

        # then
        logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_FAILURE, error=error_message)


@patch("geostore.dataset_versions.create.datasets_model_with_meta")
def should_log_warning_if_dataset_does_not_exist(datasets_model_mock: MagicMock) -> None:
    # given
    error_message = any_error_message()
    datasets_model_mock.return_value.get.side_effect = DoesNotExist(error_message)

    payload = {
        METADATA_URL_KEY: any_s3_url(),
        DATASET_ID_SHORT_KEY: any_dataset_id(),
        S3_ROLE_ARN_KEY: any_role_arn(),
    }

    with patch("geostore.dataset_versions.create.LOGGER.warning") as logger_mock:
        # when
        create_dataset_version(payload)

        # then
        logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_FAILURE, error=error_message)
