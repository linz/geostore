from copy import deepcopy
from json import dumps
from logging import getLogger
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pytest_subtests import SubTests

from backend.check_stac_metadata.task import lambda_handler
from backend.error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from backend.step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from tests.aws_utils import any_lambda_context, any_s3_url
from tests.general_generators import any_error_message
from tests.stac_generators import any_dataset_id, any_dataset_version_id

MINIMAL_PAYLOAD = {
    DATASET_ID_KEY: any_dataset_id(),
    VERSION_ID_KEY: any_dataset_version_id(),
    METADATA_URL_KEY: any_s3_url(),
}

LOGGER = getLogger("backend.check_stac_metadata.task")


def should_log_event_payload() -> None:
    payload = deepcopy(MINIMAL_PAYLOAD)
    expected_log = dumps({"event": payload})

    with patch.object(LOGGER, "debug") as logger_mock, patch(
        "backend.check_stac_metadata.task.STACDatasetValidator.run"
    ):
        lambda_handler(payload, any_lambda_context())

        logger_mock.assert_any_call(expected_log)


@patch("backend.check_stac_metadata.task.validate")
def should_return_error_when_schema_validation_fails(
    validate_schema_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    error_message = any_error_message()
    error = ValidationError(error_message)
    validate_schema_mock.side_effect = error
    expected_log = dumps({ERROR_KEY: error}, default=str)

    with patch.object(LOGGER, "warning") as logger_mock:
        # When
        with subtests.test(msg="response"):
            response = lambda_handler({}, any_lambda_context())
            # Then
            assert response == {ERROR_MESSAGE_KEY: error_message}

        # Then
        with subtests.test(msg="log"):
            logger_mock.assert_any_call(expected_log)
