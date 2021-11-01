from json import dumps
from logging import Logger, getLogger
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pynamodb.exceptions import DoesNotExist
from pytest import mark

from geostore.api_keys import EVENT_KEY
from geostore.aws_keys import BODY_KEY, HTTP_METHOD_KEY
from geostore.dataset_versions.create import create_dataset_version
from geostore.error_response_keys import ERROR_KEY
from geostore.step_function_keys import DATASET_ID_SHORT_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY

from .aws_utils import Dataset, any_role_arn, any_s3_url
from .general_generators import any_error_message
from .stac_generators import any_dataset_id


class TestLogging:
    logger: Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = getLogger("geostore.dataset_versions.create")

    @mark.infrastructure
    def should_log_payload(self) -> None:
        # Given
        with patch(
            "geostore.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution"
        ), Dataset() as dataset, patch.object(self.logger, "debug") as logger_mock:
            event = {
                HTTP_METHOD_KEY: "POST",
                BODY_KEY: {
                    METADATA_URL_KEY: any_s3_url(),
                    DATASET_ID_SHORT_KEY: dataset.dataset_id,
                    S3_ROLE_ARN_KEY: any_role_arn(),
                },
            }
            expected_payload_log = dumps({EVENT_KEY: event})

            # When
            create_dataset_version(event)

            # Then
            logger_mock.assert_any_call(expected_payload_log)

    @mark.infrastructure
    @patch("geostore.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution")
    def should_log_step_function_state_machine_response(
        self, start_execution_mock: MagicMock
    ) -> None:
        # Given
        start_execution_mock.return_value = step_function_response = {
            "executionArn": "Some Response"
        }

        expected_execution_log = dumps({"response": step_function_response})

        with Dataset() as dataset, patch.object(self.logger, "debug") as logger_mock:
            event = {
                METADATA_URL_KEY: any_s3_url(),
                DATASET_ID_SHORT_KEY: dataset.dataset_id,
                S3_ROLE_ARN_KEY: any_role_arn(),
            }

            # When
            create_dataset_version(event)

            # Then
            logger_mock.assert_any_call(expected_execution_log)

    @patch("geostore.dataset_versions.create.validate")
    def should_log_missing_argument_warning(self, validate_schema_mock: MagicMock) -> None:
        # given
        error_message = any_error_message()
        validate_schema_mock.side_effect = ValidationError(error_message)

        payload = {HTTP_METHOD_KEY: "POST", BODY_KEY: {}}

        expected_log = dumps({ERROR_KEY: error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # when
            create_dataset_version(payload)

            # then
            logger_mock.assert_any_call(expected_log)

    @patch("geostore.dataset_versions.create.datasets_model_with_meta")
    def should_log_warning_if_dataset_does_not_exist(self, datasets_model_mock: MagicMock) -> None:
        # given
        error_message = any_error_message()
        datasets_model_mock.return_value.get.side_effect = DoesNotExist(error_message)

        payload = {
            METADATA_URL_KEY: any_s3_url(),
            DATASET_ID_SHORT_KEY: any_dataset_id(),
            S3_ROLE_ARN_KEY: any_role_arn(),
        }

        expected_log = dumps({ERROR_KEY: error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # when
            create_dataset_version(payload)

            # then
            logger_mock.assert_any_call(expected_log)
