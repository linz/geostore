import logging
from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist
from pytest import mark

from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY
from backend.dataset_versions.create import create_dataset_version
from backend.error_response_keys import ERROR_KEY

from .aws_utils import Dataset, any_s3_url
from .general_generators import any_error_message
from .stac_generators import any_dataset_id


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.dataset_versions.create")

    @mark.infrastructure
    def should_log_payload(self) -> None:
        # Given
        with patch(
            "backend.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution"
        ), Dataset() as dataset, patch.object(self.logger, "debug") as logger_mock:
            event = {
                HTTP_METHOD_KEY: "POST",
                BODY_KEY: {"metadata_url": any_s3_url(), "id": dataset.dataset_id},
            }
            expected_payload_log = dumps({"event": event})

            # When
            create_dataset_version(event)

            # Then
            logger_mock.assert_any_call(expected_payload_log)

    @mark.infrastructure
    @patch("backend.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution")
    def should_log_step_function_state_machine_response(
        self, start_execution_mock: MagicMock
    ) -> None:
        # Given
        start_execution_mock.return_value = step_function_response = {
            "executionArn": "Some Response"
        }

        expected_execution_log = dumps({"response": step_function_response})

        with Dataset() as dataset, patch.object(self.logger, "debug") as logger_mock:
            event = {"metadata_url": any_s3_url(), "id": dataset.dataset_id}

            # When
            create_dataset_version(event)

            # Then
            logger_mock.assert_any_call(expected_execution_log)

    @patch("backend.dataset_versions.create.validate")
    def should_log_missing_argument_warning(self, validate_schema_mock: MagicMock) -> None:
        # given
        metadata_url = any_s3_url()
        error_message = any_error_message()
        validate_schema_mock.side_effect = ValidationError(error_message)

        payload = {HTTP_METHOD_KEY: "POST", BODY_KEY: {"metadata_url": metadata_url}}

        expected_log = dumps({ERROR_KEY: error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # when
            create_dataset_version(payload)

            # then
            logger_mock.assert_any_call(expected_log)

    @patch("backend.dataset_versions.create.datasets_model_with_meta")
    def should_log_warning_if_dataset_does_not_exist(self, datasets_model_mock: MagicMock) -> None:
        # given
        dataset_id = any_dataset_id()
        metadata_url = any_s3_url()
        error_message = any_error_message()
        datasets_model_mock.return_value.get.side_effect = DoesNotExist(error_message)

        payload = {"metadata_url": metadata_url, "id": dataset_id}

        expected_log = dumps({ERROR_KEY: error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # when
            create_dataset_version(payload)

            # then
            logger_mock.assert_any_call(expected_log)
