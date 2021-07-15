from json import dumps
from logging import getLogger
from os import environ
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError

from backend.aws_keys import AWS_DEFAULT_REGION_KEY
from backend.error_response_keys import ERROR_KEY

from .aws_profile_utils import any_region_name
from .aws_utils import any_lambda_context
from .general_generators import any_error_message

with patch("backend.parameter_store.SSM_CLIENT"), patch.dict(
    environ, {AWS_DEFAULT_REGION_KEY: any_region_name()}
):
    from backend.import_dataset.task import lambda_handler

LOGGER = getLogger("backend.import_dataset.task")


@patch("backend.import_dataset.task.validate")
def should_log_schema_validation_warning(validate_schema_mock: MagicMock) -> None:
    # Given

    error_message = any_error_message()
    validate_schema_mock.side_effect = ValidationError(error_message)
    expected_log = dumps({ERROR_KEY: error_message})

    with patch.object(LOGGER, "warning") as logger_mock:
        # When
        lambda_handler({}, any_lambda_context())

        # Then
        logger_mock.assert_any_call(expected_log)
