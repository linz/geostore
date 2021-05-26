from copy import deepcopy
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.error_response_keys import ERROR_MESSAGE_KEY
from backend.resources import ResourceName
from backend.step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from backend.update_dataset_catalog.task import lambda_handler
from tests.aws_utils import Dataset, S3Object, any_lambda_context, any_s3_url
from tests.file_utils import json_dict_to_file_object
from tests.general_generators import any_error_message, any_safe_filename
from tests.stac_generators import any_dataset_id, any_dataset_version_id
from tests.stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT


@mark.infrastructure
def should_succeed_and_trigger_sqs_update_to_catalog(subtests: SubTests) -> None:
    dataset_version = any_dataset_version_id()
    filename = f"{any_safe_filename()}.json"

    with Dataset() as dataset, patch(
        "backend.update_dataset_catalog.task.SQS_RESOURCE"
    ) as sqs_mock, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{dataset_version}/{filename}",
    ) as dataset_version_metadata:
        response = lambda_handler(
            {
                DATASET_ID_KEY: dataset.dataset_id,
                VERSION_ID_KEY: dataset_version,
                METADATA_URL_KEY: f"{any_s3_url()}/{filename}",
            },
            any_lambda_context(),
        )

        with subtests.test(msg="success"):
            assert response == {}

        with subtests.test(msg="sqs called"):
            assert sqs_mock.get_queue_by_name.return_value.send_message.called

        with subtests.test(msg="correct url passed to sqs"):
            metadata_key = sqs_mock.get_queue_by_name.return_value.send_message.call_args[1][
                "MessageBody"
            ]
            assert metadata_key == dataset_version_metadata.key


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


@mark.infrastructure
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
