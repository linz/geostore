from unittest.mock import MagicMock, patch

from geostore.api_keys import SUCCESS_KEY
from geostore.import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from geostore.logging_keys import GIT_COMMIT, LOG_MESSAGE_LAMBDA_START
from geostore.parameter_store import ParameterName, get_param
from geostore.step_function_keys import (
    DATASET_ID_KEY,
    IMPORT_DATASET_KEY,
    NEW_VERSION_ID_KEY,
    VALIDATION_KEY,
)
from geostore.upload_status.task import lambda_handler

from .aws_utils import any_job_id, any_lambda_context
from .stac_generators import any_dataset_id, any_dataset_version_id


@patch("geostore.upload_status.task.get_tasks_status")
def should_log_event(get_tasks_status_mock: MagicMock) -> None:
    # Given
    get_tasks_status_mock.return_value = {}

    event = {
        DATASET_ID_KEY: any_dataset_id(),
        NEW_VERSION_ID_KEY: any_dataset_version_id(),
        VALIDATION_KEY: {SUCCESS_KEY: True},
        IMPORT_DATASET_KEY: {
            METADATA_JOB_ID_KEY: any_job_id(),
            ASSET_JOB_ID_KEY: any_job_id(),
        },
    }

    with patch("geostore.upload_status.task.LOGGER.debug") as logger_mock:
        # When
        lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_any_call(
            LOG_MESSAGE_LAMBDA_START,
            extra={"lambda_input": event, GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
        )
