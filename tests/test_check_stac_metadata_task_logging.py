from copy import deepcopy
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from jsonschema import ValidationError
from pytest_subtests import SubTests

from geostore.check_stac_metadata.task import lambda_handler
from geostore.error_response_keys import ERROR_MESSAGE_KEY
from geostore.logging_keys import (
    LOG_MESSAGE_LAMBDA_FAILURE,
    LOG_MESSAGE_LAMBDA_START,
    LOG_MESSAGE_VALIDATION_COMPLETE,
)
from geostore.step_function import Outcome
from geostore.step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    S3_ROLE_ARN_KEY,
)

from .aws_utils import (
    MockGeostoreS3Response,
    any_error_code,
    any_lambda_context,
    any_operation_name,
    any_role_arn,
    any_s3_url,
)
from .general_generators import any_error_message
from .stac_generators import any_dataset_id, any_dataset_prefix, any_dataset_version_id
from .stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

if TYPE_CHECKING:
    from botocore.exceptions import ClientErrorResponseError, ClientErrorResponseTypeDef
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict

MINIMAL_PAYLOAD = {
    DATASET_ID_KEY: any_dataset_id(),
    NEW_VERSION_ID_KEY: any_dataset_version_id(),
    METADATA_URL_KEY: any_s3_url(),
    S3_ROLE_ARN_KEY: any_role_arn(),
    DATASET_PREFIX_KEY: any_dataset_prefix(),
}


@patch("geostore.check_stac_metadata.task.get_s3_url_reader")
def should_log_event_payload(get_s3_url_reader_mock: MagicMock) -> None:
    payload = deepcopy(MINIMAL_PAYLOAD)
    get_s3_url_reader_mock.return_value.return_value = MockGeostoreS3Response(
        MINIMAL_VALID_STAC_COLLECTION_OBJECT, file_in_staging=True
    )

    with patch("geostore.check_stac_metadata.task.LOGGER.debug") as logger_mock, patch(
        "geostore.check_stac_metadata.task.STACDatasetValidator.run"
    ):
        lambda_handler(payload, any_lambda_context())

        logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_START, extra={"lambda_input": payload})


@patch("geostore.check_stac_metadata.task.validate")
def should_return_error_when_schema_validation_fails(
    validate_schema_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    error_message = any_error_message()
    error = ValidationError(error_message)
    validate_schema_mock.side_effect = error

    with patch("geostore.check_stac_metadata.task.LOGGER.warning") as logger_mock:
        # When
        with subtests.test(msg="response"):
            response = lambda_handler({}, any_lambda_context())
            # Then
            assert response == {ERROR_MESSAGE_KEY: error_message}

        # Then
        with subtests.test(msg="log"):
            logger_mock.assert_any_call(
                LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.FAILED, "error": error}
            )


@patch("geostore.check_stac_metadata.task.get_s3_url_reader")
def should_log_error_when_assuming_s3_role_fails(
    get_s3_url_reader_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    error_code = any_error_code()
    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code=error_code, Message=error_message)
        ),
        operation_name,
    )
    get_s3_url_reader_mock.side_effect = error
    expected_message = (
        f"An error occurred ({error_code}) when calling the {operation_name}"
        f" operation: {error_message}"
    )

    with patch("geostore.check_stac_metadata.task.LOGGER.warning") as logger_mock:
        # When
        with subtests.test(msg="response"):
            response = lambda_handler(deepcopy(MINIMAL_PAYLOAD), any_lambda_context())
            # Then
            assert response == {ERROR_MESSAGE_KEY: expected_message}

        # Then
        with subtests.test(msg="log"):
            logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_FAILURE, extra={"error": error})
