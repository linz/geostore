from unittest.mock import MagicMock, patch

from geostore.logging_keys import LOG_MESSAGE_LAMBDA_START, LOG_MESSAGE_VALIDATION_COMPLETE
from geostore.step_function import Outcome
from geostore.step_function_keys import DATASET_ID_KEY, VERSION_ID_KEY
from geostore.validation_summary import task

from .aws_utils import any_lambda_context
from .stac_generators import any_dataset_id, any_dataset_version_id


def should_log_event() -> None:
    # Given
    event = {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}

    with patch("geostore.validation_summary.task.validation_results_model_with_meta"), patch(
        "geostore.validation_summary.task.LOGGER.debug"
    ) as logger_mock:
        # When
        task.lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_any_call(LOG_MESSAGE_LAMBDA_START, extra={"lambda_input": event})


@patch("geostore.validation_summary.task.validation_results_model_with_meta")
def should_log_failure_result(validation_results_model_mock: MagicMock) -> None:
    # Given
    event = {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
    validation_results_model_mock.return_value.validation_outcome_index.count.return_value = 1

    with patch("geostore.validation_summary.task.LOGGER.debug") as logger_mock:
        # When
        task.lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_any_call(
            LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.PASSED}
        )


@patch("geostore.validation_summary.task.validation_results_model_with_meta")
def should_log_success_result(validation_results_model_mock: MagicMock) -> None:
    # Given
    event = {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
    validation_results_model_mock.return_value.validation_outcome_index.count.return_value = 0

    with patch("geostore.validation_summary.task.LOGGER.debug") as logger_mock:
        # When
        task.lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_any_call(
            LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.PASSED}
        )
