from copy import deepcopy
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pytest import mark
from pytest_subtests import SubTests

from backend.error_response_keys import ERROR_MESSAGE_KEY
from backend.resources import ResourceName
from backend.sqs_message_attributes import (
    DATA_TYPE_KEY,
    DATA_TYPE_STRING,
    MESSAGE_ATTRIBUTE_TYPE_DATASET,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
    STRING_VALUE_KEY,
)
from backend.step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)
from backend.update_dataset_catalog.task import lambda_handler
from tests.aws_utils import Dataset, S3Object, any_lambda_context, any_role_arn, any_s3_url
from tests.file_utils import json_dict_to_file_object
from tests.general_generators import any_error_message, any_safe_filename
from tests.stac_generators import any_dataset_version_id
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
                DATASET_PREFIX_KEY: dataset.dataset_prefix,
                VERSION_ID_KEY: dataset_version,
                METADATA_URL_KEY: f"{any_s3_url()}/{filename}",
                S3_ROLE_ARN_KEY: any_role_arn(),
            },
            any_lambda_context(),
        )

        expected_sqs_call = {
            "MessageBody": dataset_version_metadata.key,
            "MessageAttributes": {
                MESSAGE_ATTRIBUTE_TYPE_KEY: {
                    STRING_VALUE_KEY: MESSAGE_ATTRIBUTE_TYPE_DATASET,
                    DATA_TYPE_KEY: DATA_TYPE_STRING,
                }
            },
        }

        with subtests.test(msg="success"):
            assert response == {}

        with subtests.test(msg="sqs called"):
            assert sqs_mock.get_queue_by_name.return_value.send_message.called

        with subtests.test(msg="correct url passed to sqs"):
            assert (
                sqs_mock.get_queue_by_name.return_value.send_message.call_args[1]
                == expected_sqs_call
            )


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
