from json import dumps
from logging import Logger, getLogger
from os import environ
from unittest.mock import MagicMock, patch

from pytest import mark

from backend.api_keys import EVENT_KEY
from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY
from backend.aws_keys import AWS_DEFAULT_REGION_KEY
from backend.step_function_keys import DATASET_ID_SHORT_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY

from .aws_profile_utils import any_region_name
from .aws_utils import Dataset, any_role_arn, any_s3_url

with patch.dict(
    environ, {AWS_DEFAULT_REGION_KEY: environ.get(AWS_DEFAULT_REGION_KEY, any_region_name())}
):
    from backend.dataset_versions.create import create_dataset_version


class TestLogging:
    logger: Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = getLogger("backend.dataset_versions.create")

    @mark.infrastructure
    def should_log_payload(self) -> None:
        # Given
        with patch(
            "backend.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution"
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
            event = {
                METADATA_URL_KEY: any_s3_url(),
                DATASET_ID_SHORT_KEY: dataset.dataset_id,
                S3_ROLE_ARN_KEY: any_role_arn(),
            }

            # When
            create_dataset_version(event)

            # Then
            logger_mock.assert_any_call(expected_execution_log)
