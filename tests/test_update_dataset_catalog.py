from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.error_response_keys import ERROR_MESSAGE_KEY
from backend.step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from backend.update_dataset_catalog.task import lambda_handler
from tests.aws_utils import Dataset, any_lambda_context, any_s3_url
from tests.general_generators import any_error_message
from tests.stac_generators import any_dataset_id, any_dataset_version_id


@mark.infrastructure
def should_succeed_and_trigger_sqs_update_to_catalog(subtests: SubTests) -> None:
    with Dataset() as dataset, patch(
        "backend.update_dataset_catalog.task.SQS_RESOURCE"
    ) as sqs_mock:
        response = lambda_handler(
            {
                DATASET_ID_KEY: dataset.dataset_id,
                VERSION_ID_KEY: any_dataset_version_id(),
                METADATA_URL_KEY: any_s3_url(),
            },
            any_lambda_context(),
        )

        with subtests.test(msg="success"):
            assert response == {}

        with subtests.test(msg="sqs called"):
            assert sqs_mock.get_queue_by_name.return_value.send_message.called


@mark.infrastructure
def should_return_error_if_dataset_id_does_not_exist_in_db() -> None:
    dataset_id = any_dataset_id()
    response = lambda_handler(
        {
            DATASET_ID_KEY: dataset_id,
            VERSION_ID_KEY: any_dataset_version_id(),
            METADATA_URL_KEY: any_s3_url(),
        },
        any_lambda_context(),
    )

    assert response == {ERROR_MESSAGE_KEY: f"dataset '{dataset_id}' could not be found"}


@patch("backend.update_dataset_catalog.task.validate")
def should_return_required_property_error_when_missing_mandatory_property(
    validate_url_mock: MagicMock,
) -> None:
    error_message = any_error_message()
    validate_url_mock.side_effect = ValidationError(error_message)
    with Dataset() as dataset:
        response = lambda_handler(
            {
                DATASET_ID_KEY: dataset.dataset_id,
                VERSION_ID_KEY: any_dataset_version_id(),
            },
            any_lambda_context(),
        )
        assert response == {ERROR_MESSAGE_KEY: error_message}
