from unittest.mock import MagicMock, patch

from backend.api_keys import SUCCESS_KEY
from backend.error_response_keys import ERROR_MESSAGE_KEY
from backend.step_function import DATASET_ID_KEY, VERSION_ID_KEY
from backend.validation_summary.task import lambda_handler

from .aws_utils import any_lambda_context
from .stac_generators import any_dataset_id, any_dataset_version_id


def should_require_dataset_id() -> None:
    response = lambda_handler({VERSION_ID_KEY: any_dataset_version_id()}, any_lambda_context())

    assert response == {ERROR_MESSAGE_KEY: "'dataset_id' is a required property"}


def should_require_dataset_version() -> None:
    response = lambda_handler({DATASET_ID_KEY: any_dataset_id()}, any_lambda_context())

    assert response == {ERROR_MESSAGE_KEY: "'version_id' is a required property"}


@patch("backend.validation_summary.task.validation_results_model_with_meta")
def should_return_success_false_if_any_validation_results_are_unsuccessful(
    validation_results_model_mock: MagicMock,
) -> None:
    # Given an unsuccessful result
    validation_results_model_mock.return_value.validation_outcome_index.count.return_value = 1

    response = lambda_handler(
        {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()},
        any_lambda_context(),
    )

    assert response == {SUCCESS_KEY: False}
