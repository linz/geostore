from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError

from geostore.aws_keys import BODY_KEY, HTTP_METHOD_KEY
from geostore.import_status.get import get_import_status
from geostore.logging_keys import (
    LOG_MESSAGE_LAMBDA_FAILURE,
    LOG_MESSAGE_LAMBDA_START,
    LOG_MESSAGE_STEP_FUNCTION_RESPONSE,
)
from geostore.step_function_keys import DATASET_ID_KEY, EXECUTION_ARN_KEY, NEW_VERSION_ID_KEY

from .aws_utils import any_arn_formatted_string
from .general_generators import any_error_message
from .stac_generators import any_dataset_id, any_dataset_version_id


@patch("geostore.step_function.STEP_FUNCTIONS_CLIENT.describe_execution")
def should_log_payload(describe_step_function_mock: MagicMock) -> None:
    # Given
    event = {
        HTTP_METHOD_KEY: "GET",
        BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()},
    }

    describe_step_function_mock.return_value = {
        "status": "RUNNING",
        "input": dumps(
            {DATASET_ID_KEY: any_dataset_id(), NEW_VERSION_ID_KEY: any_dataset_version_id()}
        ),
    }

    with patch("geostore.import_status.get.LOGGER.debug") as logger_mock, patch(
        "geostore.step_function.get_step_function_validation_results"
    ) as validation_mock:
        validation_mock.return_value = []

        # When
        get_import_status(event)

        # Then
        logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_START, extra={"lambda_input": event})


@patch("geostore.import_status.get.validate")
def should_log_schema_validation_warning(validate_schema_mock: MagicMock) -> None:
    # Given

    error_message = any_error_message()
    validate_schema_mock.side_effect = ValidationError(error_message)

    with patch("geostore.import_status.get.LOGGER.warning") as logger_mock:
        # When
        get_import_status(
            {
                HTTP_METHOD_KEY: "GET",
                BODY_KEY: {},
            }
        )

        # Then
        logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_FAILURE, extra={"error": error_message})


@patch("geostore.step_function.STEP_FUNCTIONS_CLIENT.describe_execution")
def should_log_stepfunctions_status_response(
    describe_execution_mock: MagicMock,
) -> None:
    # Given
    describe_execution_mock.return_value = describe_execution_response = {
        "status": "Some Response",
        "input": dumps(
            {DATASET_ID_KEY: any_dataset_id(), NEW_VERSION_ID_KEY: any_dataset_version_id()}
        ),
    }

    with patch("geostore.step_function.LOGGER.debug") as logger_mock, patch(
        "geostore.step_function.get_account_number"
    ), patch("geostore.step_function.get_step_function_validation_results") as validation_mock:
        validation_mock.return_value = []
        # When
        get_import_status({EXECUTION_ARN_KEY: any_arn_formatted_string()})

        # Then
        logger_mock.assert_any_call(
            LOG_MESSAGE_STEP_FUNCTION_RESPONSE, extra={"response": describe_execution_response}
        )
