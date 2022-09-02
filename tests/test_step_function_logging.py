from unittest.mock import MagicMock, patch

from geostore.logging_keys import GIT_COMMIT, LOG_MESSAGE_S3_BATCH_RESPONSE
from geostore.parameter_store import ParameterName, get_param
from geostore.step_function import get_s3_batch_copy_status

from .aws_utils import any_account_id


@patch("geostore.step_function.S3CONTROL_CLIENT.describe_job")
def should_log_s3_batch_response(
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

    with patch("geostore.step_function.LOGGER.debug") as logger_mock, patch(
        "geostore.step_function.get_account_number"
    ) as get_account_number_mock:
        get_account_number_mock.return_value = any_account_id()

        # When
        get_s3_batch_copy_status("test")

        # Then
        logger_mock.assert_any_call(
            LOG_MESSAGE_S3_BATCH_RESPONSE,
            extra={
                "response": s3_batch_response,
                GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
            },
        )
