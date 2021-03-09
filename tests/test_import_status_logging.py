import json
import logging
from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]

from backend.import_status.get import get_import_status, get_s3_batch_copy_status

from .general_generators import any_valid_arn


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.import_status.get")

    def test_should_log_payload(
        self,
    ) -> None:
        # Given
        payload = {
            "httpMethod": "GET",
            "body": {"execution_arn": any_valid_arn()},
        }

        expected_payload_log = dumps({"payload": payload})

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.import_status.get.validate"
        ) as validate_mock:
            validate_mock.side_effect = ValidationError("test")

            # When
            get_import_status(payload)

            # Then
            logger_mock.assert_any_call(expected_payload_log)

    @patch("backend.import_status.get.validate")
    def test_should_log_schema_validation_warning(self, validate_schema_mock: MagicMock) -> None:
        # Given

        error_message = "Some error message"
        validate_schema_mock.side_effect = ValidationError(error_message)
        expected_log = dumps({"error": error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # When
            get_import_status(
                {
                    "httpMethod": "GET",
                    "body": {},
                }
            )

            # Then
            logger_mock.assert_any_call(expected_log)

    @patch("backend.import_status.get.STEPFUNCTIONS_CLIENT.describe_execution")
    def test_should_log_stepfunctions_status_response(
        self,
        describe_execution_mock: MagicMock,
    ) -> None:
        # Given

        describe_execution_mock.return_value = describe_execution_response = {
            "status": "Some Response"
        }
        expected_response_log = json.dumps({"step function response": describe_execution_response})

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.import_status.get.STS_CLIENT.get_caller_identity"
        ):
            get_import_status(
                {
                    "httpMethod": "GET",
                    "body": {"execution_arn": any_valid_arn()},
                }
            )

            # Then
            logger_mock.assert_any_call(expected_response_log)

    @patch("backend.import_status.get.S3CONTROL_CLIENT.describe_job")
    def test_should_log_s3_batch_response(
        self,
        describe_s3_job_mock: MagicMock,
    ) -> None:
        # Given

        describe_s3_job_mock.return_value = s3_batch_response = {"Job": {"Status": "Some Response"}}
        expected_response_log = json.dumps({"s3 batch response": s3_batch_response})

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.import_status.get.STS_CLIENT.get_caller_identity"
        ):
            get_s3_batch_copy_status(
                "test",
                self.logger,
            )

            # Then
            logger_mock.assert_any_call(expected_response_log)
