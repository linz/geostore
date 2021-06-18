from json import dumps
from logging import Logger, getLogger
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]

from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY
from backend.error_response_keys import ERROR_KEY
from backend.import_status.get import EXECUTION_ARN_KEY, get_import_status
from backend.step_function import DATASET_ID_KEY, VERSION_ID_KEY

from .aws_utils import any_arn_formatted_string
from .general_generators import any_error_message
from .stac_generators import any_dataset_id, any_dataset_version_id


class TestLogging:
    logger: Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = getLogger("backend.import_status.get")

    @patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
    def should_log_payload(self, describe_step_function_mock: MagicMock) -> None:
        # Given
        event = {
            HTTP_METHOD_KEY: "GET",
            BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()},
        }

        expected_payload_log = dumps({"event": event})

        describe_step_function_mock.return_value = {
            "status": "RUNNING",
            "input": dumps(
                {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
            ),
        }

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.step_function.get_step_function_validation_results"
        ) as validation_mock:
            validation_mock.return_value = []

            # When
            get_import_status(event)

            # Then
            logger_mock.assert_any_call(expected_payload_log)

    @patch("backend.import_status.get.validate")
    def should_log_schema_validation_warning(self, validate_schema_mock: MagicMock) -> None:
        # Given

        error_message = any_error_message()
        validate_schema_mock.side_effect = ValidationError(error_message)
        expected_log = dumps({ERROR_KEY: error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # When
            get_import_status(
                {
                    HTTP_METHOD_KEY: "GET",
                    BODY_KEY: {},
                }
            )

            # Then
            logger_mock.assert_any_call(expected_log)

    @patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
    def should_log_stepfunctions_status_response(
        self,
        describe_execution_mock: MagicMock,
    ) -> None:
        # Given
        describe_execution_mock.return_value = describe_execution_response = {
            "status": "Some Response",
            "input": dumps(
                {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
            ),
        }
        expected_response_log = dumps({"step function response": describe_execution_response})

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.step_function.get_account_number"
        ), patch("backend.step_function.get_step_function_validation_results") as validation_mock:
            validation_mock.return_value = []
            # When
            get_import_status({EXECUTION_ARN_KEY: any_arn_formatted_string()})

            # Then
            logger_mock.assert_any_call(expected_response_log)
