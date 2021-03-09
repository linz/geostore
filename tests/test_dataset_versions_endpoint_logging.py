import logging
from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist
from pytest import mark

from backend.dataset_versions.create import create_dataset_version

from .aws_utils import Dataset, any_s3_url
from .stac_generators import any_dataset_id, any_valid_dataset_type


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.dataset_versions.create")

    @mark.infrastructure
    @patch("backend.dataset_versions.create.stepfunctions_client.start_execution")
    def should_log_payload(
        self, start_execution_mock: MagicMock  # pylint:disable=unused-argument
    ) -> None:
        # given
        dataset_id = any_dataset_id()
        dataset_type = any_valid_dataset_type()
        metadata_url = any_s3_url()
        with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

            payload = {
                "httpMethod": "POST",
                "body": {"metadata-url": metadata_url, "id": dataset_id, "type": dataset_type},
            }

            expected_payload_log = dumps({"payload": payload})

            with patch.object(self.logger, "debug") as logger_mock:
                # when
                create_dataset_version(payload)

                # then
                logger_mock.assert_any_call(expected_payload_log)

    @mark.infrastructure
    @patch("backend.dataset_versions.create.stepfunctions_client.start_execution")
    def should_log_step_function_state_machine_response(
        self, start_execution_mock: MagicMock
    ) -> None:
        # given
        dataset_id = any_dataset_id()
        dataset_type = any_valid_dataset_type()
        metadata_url = any_s3_url()
        with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

            payload = {
                "httpMethod": "POST",
                "body": {"metadata-url": metadata_url, "id": dataset_id, "type": dataset_type},
            }

            start_execution_mock.return_value = step_function_response = {
                "executionArn": "Some Response"
            }

            expected_execution_log = dumps({"response": step_function_response})

            with patch.object(self.logger, "debug") as logger_mock:
                # when
                create_dataset_version(payload)

                logger_mock.assert_any_call(expected_execution_log)

    @patch("backend.dataset_versions.create.validate")
    def should_log_missing_argument_warning(self, validate_schema_mock: MagicMock) -> None:
        # given
        dataset_type = any_valid_dataset_type()
        metadata_url = any_s3_url()
        error_message = "Some error message"
        validate_schema_mock.side_effect = ValidationError(error_message)

        payload = {
            "httpMethod": "POST",
            "body": {"metadata-url": metadata_url, "type": dataset_type},
        }

        expected_log = dumps({"error": error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # when
            create_dataset_version(payload)

            # then
            logger_mock.assert_any_call(expected_log)

    @patch("backend.dataset_versions.create.DatasetModel.get")
    def should_log_warning_if_dataset_does_not_exist(
        self, validate_dataset_mock: MagicMock
    ) -> None:
        # given
        dataset_id = any_dataset_id()
        dataset_type = any_valid_dataset_type()
        metadata_url = any_s3_url()
        error_message = "Some error message"
        validate_dataset_mock.side_effect = DoesNotExist(error_message)

        payload = {
            "httpMethod": "POST",
            "body": {"metadata-url": metadata_url, "id": dataset_id, "type": dataset_type},
        }

        expected_log = dumps({"error": error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # when
            create_dataset_version(payload)

            # then
            logger_mock.assert_any_call(expected_log)
