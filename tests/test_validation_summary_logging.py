from json import dumps
from logging import Logger, getLogger
from unittest.mock import MagicMock, patch

from backend.validation_summary import task

from .aws_utils import any_lambda_context
from .stac_generators import any_dataset_id, any_dataset_version_id


class TestLogging:
    logger: Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = getLogger(task.__name__)

    def should_log_event(self) -> None:
        # Given
        event = {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()}
        expected_log = dumps({"event": event})

        with patch(
            f"{task.__name__}.{task.validation_results_model_with_meta.__name__}"
        ), patch.object(self.logger, "debug") as logger_mock:
            # When
            task.lambda_handler(event, any_lambda_context())

            # Then
            logger_mock.assert_any_call(expected_log)

    @patch(f"{task.__name__}.{task.validation_results_model_with_meta.__name__}")
    def should_log_failure_result(self, validation_results_model_mock: MagicMock) -> None:
        # Given
        event = {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()}
        expected_log = dumps({"success": False})
        validation_results_model_mock.return_value.validation_outcome_index.count.return_value = 1

        with patch.object(self.logger, "debug") as logger_mock:
            # When
            task.lambda_handler(event, any_lambda_context())

            # Then
            logger_mock.assert_any_call(expected_log)

    @patch(f"{task.__name__}.{task.validation_results_model_with_meta.__name__}")
    def should_log_success_result(self, validation_results_model_mock: MagicMock) -> None:
        # Given
        event = {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()}
        expected_log = dumps({"success": True})
        validation_results_model_mock.return_value.validation_outcome_index.count.return_value = 0

        with patch.object(self.logger, "debug") as logger_mock:
            # When
            task.lambda_handler(event, any_lambda_context())

            # Then
            logger_mock.assert_any_call(expected_log)
