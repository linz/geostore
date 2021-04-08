import logging
from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist
from pytest import mark

from backend.dataset_versions.create import create_dataset_version

from .aws_utils import Dataset, any_s3_url
from .stac_generators import any_dataset_id


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.dataset_versions.create")

    @mark.infrastructure
    @patch("backend.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution")
    def should_log_payload(
        self, start_execution_mock: MagicMock  # pylint:disable=unused-argument
    ) -> None:
        # Given
        dataset_id = any_dataset_id()
        metadata_url = any_s3_url()
        event = {"httpMethod": "POST", "body": {"metadata-url": metadata_url, "id": dataset_id}}

        expected_payload_log = dumps({"event": event})

        with Dataset(dataset_id=dataset_id), patch.object(self.logger, "debug") as logger_mock:
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
        dataset_id = any_dataset_id()
        metadata_url = any_s3_url()
        event = {"httpMethod": "POST", "body": {"metadata-url": metadata_url, "id": dataset_id}}

        start_execution_mock.return_value = step_function_response = {
            "executionArn": "Some Response"
        }

        expected_execution_log = dumps({"response": step_function_response})

        with Dataset(dataset_id=dataset_id), patch.object(self.logger, "debug") as logger_mock:
            # When
            create_dataset_version(event)

            # Then
            logger_mock.assert_any_call(expected_execution_log)

    @patch("backend.dataset_versions.create.validate")
    def should_log_missing_argument_warning(self, validate_schema_mock: MagicMock) -> None:
        # given
        metadata_url = any_s3_url()
        error_message = "Some error message"
        validate_schema_mock.side_effect = ValidationError(error_message)

        payload = {"httpMethod": "POST", "body": {"metadata-url": metadata_url}}

        expected_log = dumps({"error": error_message})

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
        error_message = "Some error message"
        datasets_model_mock.return_value.get.side_effect = DoesNotExist(error_message)

        payload = {"httpMethod": "POST", "body": {"metadata-url": metadata_url, "id": dataset_id}}

        expected_log = dumps({"error": error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # when
            create_dataset_version(payload)

            # then
            logger_mock.assert_any_call(expected_log)
