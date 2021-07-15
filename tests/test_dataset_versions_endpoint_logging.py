from json import dumps
from logging import Logger, getLogger
from os import environ
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pynamodb.exceptions import DoesNotExist

from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY
from backend.aws_keys import AWS_DEFAULT_REGION_KEY
from backend.error_response_keys import ERROR_KEY
from backend.step_function_keys import DATASET_ID_SHORT_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY

from .aws_profile_utils import any_region_name
from .aws_utils import any_role_arn, any_s3_url
from .general_generators import any_error_message
from .stac_generators import any_dataset_id

with patch.dict(environ, {AWS_DEFAULT_REGION_KEY: any_region_name()}, clear=True):
    from backend.dataset_versions.create import create_dataset_version


class TestLogging:
    logger: Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = getLogger("backend.dataset_versions.create")

    @patch("backend.dataset_versions.create.validate")
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

    @patch("backend.dataset_versions.create.datasets_model_with_meta")
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
