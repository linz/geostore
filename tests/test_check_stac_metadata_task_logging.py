from copy import deepcopy
from io import StringIO
from json import dumps
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from jsonschema import ValidationError
from pytest_subtests import SubTests

from geostore.api_keys import EVENT_KEY
from geostore.check_stac_metadata.task import lambda_handler
from geostore.error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from geostore.import_metadata_file.task import S3_BODY_KEY
from geostore.step_function_keys import (
    DATASET_ID_KEY,
    METADATA_URL_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)
from tests.aws_utils import (
    any_error_code,
    any_lambda_context,
    any_operation_name,
    any_role_arn,
    any_s3_url,
)
from tests.general_generators import any_error_message
from tests.stac_generators import any_dataset_id, any_dataset_version_id
from tests.stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

if TYPE_CHECKING:
    from botocore.exceptions import (  # pylint:disable=no-name-in-module,ungrouped-imports
        ClientErrorResponseError,
        ClientErrorResponseTypeDef,
    )
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict

MINIMAL_PAYLOAD = {
    DATASET_ID_KEY: any_dataset_id(),
    VERSION_ID_KEY: any_dataset_version_id(),
    METADATA_URL_KEY: any_s3_url(),
    S3_ROLE_ARN_KEY: any_role_arn(),
}


@patch("geostore.check_stac_metadata.task.get_s3_client_for_role")
def should_log_event_payload(get_s3_client_for_role_mock: MagicMock) -> None:
    payload = deepcopy(MINIMAL_PAYLOAD)
    expected_log = dumps({EVENT_KEY: payload})
    get_s3_client_for_role_mock.return_value.return_value = {
        S3_BODY_KEY: StringIO(initial_value=dumps(MINIMAL_VALID_STAC_COLLECTION_OBJECT))
    }

    with patch("geostore.check_stac_metadata.task.LOGGER.debug") as logger_mock, patch(
        "geostore.check_stac_metadata.task.STACDatasetValidator.run"
    ):
        lambda_handler(payload, any_lambda_context())

        logger_mock.assert_any_call(expected_log)


@patch("geostore.check_stac_metadata.task.validate")
def should_return_error_when_schema_validation_fails(
    validate_schema_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    error_message = any_error_message()
    error = ValidationError(error_message)
    validate_schema_mock.side_effect = error
    expected_log = dumps({ERROR_KEY: error}, default=str)

    with patch("geostore.check_stac_metadata.task.LOGGER.warning") as logger_mock:
        # When
        with subtests.test(msg="response"):
            response = lambda_handler({}, any_lambda_context())
            # Then
            assert response == {ERROR_MESSAGE_KEY: error_message}

        # Then
        with subtests.test(msg="log"):
            logger_mock.assert_any_call(expected_log)


@patch("geostore.check_stac_metadata.task.get_s3_client_for_role")
def should_log_error_when_assuming_s3_role_fails(
    get_s3_client_for_role_mock: MagicMock, subtests: SubTests
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
    get_s3_client_for_role_mock.side_effect = error
    expected_log = dumps({ERROR_KEY: error}, default=str)
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
            logger_mock.assert_any_call(expected_log)
