from unittest.mock import MagicMock, call, patch

from geostore.logging_keys import LOG_MESSAGE_LAMBDA_START
from geostore.step_function_keys import DATASET_ID_KEY, VERSION_ID_KEY
from geostore.validation_summary import task
from geostore.validation_summary.task import LOG_MESSAGE_VALIDATION_SUCCESS

from .aws_utils import any_lambda_context
from .stac_generators import any_dataset_id, any_dataset_version_id


def should_log_event() -> None:
    # Given
    event = {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
    expected_log_call = call(LOG_MESSAGE_LAMBDA_START, lambda_input=event)

    with patch("geostore.validation_summary.task.validation_results_model_with_meta"), patch(
        "geostore.validation_summary.task.LOGGER.debug"
    ) as logger_mock:
        # When
        task.lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_has_calls([expected_log_call])


@patch("geostore.validation_summary.task.validation_results_model_with_meta")
def should_log_failure_result(validation_results_model_mock: MagicMock) -> None:
    # Given
    event = {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
    expected_log_call = call(LOG_MESSAGE_VALIDATION_SUCCESS, details=False)
    validation_results_model_mock.return_value.validation_outcome_index.count.return_value = 1

    with patch("geostore.validation_summary.task.LOGGER.debug") as logger_mock:
        # When
        task.lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_has_calls([expected_log_call])


@patch("geostore.validation_summary.task.validation_results_model_with_meta")
def should_log_success_result(validation_results_model_mock: MagicMock) -> None:
    # Given
    event = {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
    expected_log_call = call(LOG_MESSAGE_VALIDATION_SUCCESS, details=True)
    validation_results_model_mock.return_value.validation_outcome_index.count.return_value = 0

    with patch("geostore.validation_summary.task.LOGGER.debug") as logger_mock:
        # When
        task.lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_has_calls([expected_log_call])
