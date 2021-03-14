from unittest.mock import MagicMock, patch

from backend.validation_summary.task import lambda_handler
from tests.aws_utils import any_lambda_context
from tests.stac_generators import any_dataset_id, any_dataset_version_id


def should_require_dataset_id() -> None:
    response = lambda_handler({"version_id": any_dataset_version_id()}, any_lambda_context())

    assert response == {"error message": "'dataset_id' is a required property"}


def should_require_dataset_version() -> None:
    response = lambda_handler({"dataset_id": any_dataset_id()}, any_lambda_context())

    assert response == {"error message": "'version_id' is a required property"}


@patch("backend.validation_summary.task.ValidationResultsModel")
def should_return_success_false_if_any_validation_results_are_unsuccessful(
    validation_results_model_mock: MagicMock,
) -> None:
    # Given an unsuccessful result
    validation_results_model_mock.validation_outcome_index.count.return_value = 1

    response = lambda_handler(
        {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()},
        any_lambda_context(),
    )

    assert response == {"success": False}
