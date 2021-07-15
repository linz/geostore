from json import dumps
from logging import Logger, getLogger
from unittest.mock import MagicMock, patch

from backend.step_function import get_s3_batch_copy_status
from backend.step_function_keys import S3_BATCH_RESPONSE_KEY

from .aws_utils import any_account_id


class TestLogging:
    logger: Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = getLogger("backend.step_function")

    @patch("backend.step_function.S3CONTROL_CLIENT.describe_job")
    def should_log_s3_batch_response(
        self,
        describe_s3_job_mock: MagicMock,
    ) -> None:
        # Given
        describe_s3_job_mock.return_value = s3_batch_response = {
            "Job": {
                "Status": "Some Response",
                "FailureReasons": [],
                "ProgressSummary": {"NumberOfTasksFailed": 0},
            }
        }
        expected_response_log = dumps({S3_BATCH_RESPONSE_KEY: s3_batch_response})

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.step_function.get_account_number"
        ) as get_account_number_mock:
            get_account_number_mock.return_value = any_account_id()

            # When
            get_s3_batch_copy_status("test")

            # Then
            logger_mock.assert_any_call(expected_response_log)
