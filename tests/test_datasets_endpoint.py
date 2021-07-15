"""
Dataset endpoint Lambda function tests. Working Geostore AWS environment is
required (run '$ cdk deploy' before running tests).
"""
from http import HTTPStatus
from io import BytesIO
from json import dumps, load
from logging import INFO, basicConfig, getLogger
from unittest.mock import patch

import smart_open
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests

from backend.api_keys import MESSAGE_KEY
from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from backend.aws_message_attributes import (
    DATA_TYPE_KEY,
    DATA_TYPE_STRING,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
    MESSAGE_ATTRIBUTE_TYPE_ROOT,
    STRING_VALUE_KEY,
)
from backend.datasets.create import TITLE_PATTERN
from backend.datasets.entrypoint import lambda_handler
from backend.datasets.get import get_dataset_filter, get_dataset_single, handle_get
from backend.datasets_model import DATASET_KEY_SEPARATOR
from backend.populate_catalog.task import CATALOG_KEY
from backend.resources import ResourceName
from backend.s3 import S3_URL_PREFIX
from backend.stac_format import STAC_DESCRIPTION_KEY, STAC_TITLE_KEY
from backend.step_function_keys import DATASET_ID_SHORT_KEY, DESCRIPTION_KEY, TITLE_KEY

from .aws_utils import (
    Dataset,
    S3Object,
    any_lambda_context,
    delete_s3_key,
    delete_s3_prefix,
    get_s3_prefix_versions,
    wait_for_s3_key,
)
from .general_generators import any_dictionary_key, any_safe_filename, random_string
from .stac_generators import (
    any_dataset_description,
    any_dataset_id,
    any_dataset_title,
    any_dataset_version_id,
)

basicConfig(level=INFO)
logger = getLogger(__name__)


@mark.infrastructure
def should_create_dataset(subtests: SubTests, s3_client: S3Client) -> None:
    dataset_title = any_dataset_title()
    dataset_description = any_dataset_description()
    body = {TITLE_KEY: dataset_title, DESCRIPTION_KEY: dataset_description}

    try:

        with patch("backend.datasets.create.SQS_RESOURCE") as sqs_mock:
            response = lambda_handler(
                {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
            )

        logger.info("Response: %s", response)

        with subtests.test(msg="status code"):
            assert response[STATUS_CODE_KEY] == HTTPStatus.CREATED

        with subtests.test(msg="ID length"):
            assert len(response[BODY_KEY][DATASET_ID_SHORT_KEY]) == 26

        with subtests.test(msg="title"):
            assert response[BODY_KEY][TITLE_KEY] == dataset_title

        catalog = get_s3_prefix_versions(
            ResourceName.STORAGE_BUCKET_NAME.value, dataset_title, s3_client
        )[0]

        dataset_prefix = (
            f"{dataset_title}{DATASET_KEY_SEPARATOR}{response[BODY_KEY][DATASET_ID_SHORT_KEY]}"
        )
        expected_sqs_call = {
            "MessageBody": dataset_prefix,
            "MessageAttributes": {
                MESSAGE_ATTRIBUTE_TYPE_KEY: {
                    STRING_VALUE_KEY: MESSAGE_ATTRIBUTE_TYPE_ROOT,
                    DATA_TYPE_KEY: DATA_TYPE_STRING,
                }
            },
        }
        with smart_open.open(
            f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/{catalog['Key']}", mode="rb"
        ) as new_catalog_metadata_file:

            catalog_json = load(new_catalog_metadata_file)

            with subtests.test(msg="catalog title"):
                assert catalog_json[STAC_TITLE_KEY] == dataset_title

            with subtests.test(msg="catalog description"):
                assert catalog_json[STAC_DESCRIPTION_KEY] == dataset_description

            with subtests.test(msg="root catalog"):
                assert sqs_mock.get_queue_by_name.return_value.send_message.called

            with subtests.test(msg="correct url passed to sqs"):
                assert (
                    sqs_mock.get_queue_by_name.return_value.send_message.call_args[1]
                    == expected_sqs_call
                )

    finally:
        delete_s3_prefix(ResourceName.STORAGE_BUCKET_NAME.value, dataset_title, s3_client)


@mark.infrastructure
def should_fail_if_post_request_containing_duplicate_dataset_title() -> None:
    dataset_title = any_dataset_title()
    body = {TITLE_KEY: dataset_title, DESCRIPTION_KEY: any_dataset_description()}

    with Dataset(title=dataset_title):
        response = lambda_handler({HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.CONFLICT,
        BODY_KEY: {MESSAGE_KEY: f"Conflict: dataset '{dataset_title}' already exists"},
    }


@mark.infrastructure
def should_return_client_error_when_title_contains_unsupported_characters(
    subtests: SubTests,
) -> None:
    for character in "!@#$%^&*(){}?+| /=":
        with subtests.test(msg=character):
            response = lambda_handler(
                {
                    HTTP_METHOD_KEY: "POST",
                    BODY_KEY: {TITLE_KEY: character, DESCRIPTION_KEY: any_dataset_description()},
                },
                any_lambda_context(),
            )

            assert response == {
                STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
                BODY_KEY: {
                    MESSAGE_KEY: f"Bad Request: '{character}' does not match '{TITLE_PATTERN}'"
                },
            }


@mark.infrastructure
def should_return_single_dataset(subtests: SubTests) -> None:
    # Given a dataset instance
    with Dataset() as dataset:
        body = {DATASET_ID_SHORT_KEY: dataset.dataset_id}

        # When requesting the dataset by ID and type
        response = lambda_handler({HTTP_METHOD_KEY: "GET", BODY_KEY: body}, any_lambda_context())
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    with subtests.test(msg="status code"):
        assert response[STATUS_CODE_KEY] == HTTPStatus.OK

    with subtests.test(msg="ID"):
        assert response[BODY_KEY][DATASET_ID_SHORT_KEY] == dataset.dataset_id


@mark.infrastructure
def should_return_all_datasets(subtests: SubTests) -> None:
    # Given two datasets
    with Dataset() as first_dataset, Dataset() as second_dataset:
        # When requesting all datasets
        response = lambda_handler({HTTP_METHOD_KEY: "GET", BODY_KEY: {}}, any_lambda_context())
        logger.info("Response: %s", response)

        # Then we should get both datasets in return
        with subtests.test(msg="status code"):
            assert response[STATUS_CODE_KEY] == HTTPStatus.OK

        actual_dataset_ids = [entry[DATASET_ID_SHORT_KEY] for entry in response[BODY_KEY]]
        for dataset_id in (first_dataset.dataset_id, second_dataset.dataset_id):
            with subtests.test(msg=f"ID {dataset_id}"):
                assert dataset_id in actual_dataset_ids


@mark.infrastructure
def should_return_single_dataset_filtered_by_title(subtests: SubTests) -> None:
    # Given matching and non-matching dataset instances
    dataset_title = any_dataset_title()
    body = {TITLE_KEY: dataset_title}

    with Dataset(title=dataset_title) as matching_dataset, Dataset():
        # When requesting a specific type and title
        response = lambda_handler({HTTP_METHOD_KEY: "GET", BODY_KEY: body}, any_lambda_context())
        logger.info("Response: %s", response)

    with subtests.test(msg="ID"):
        # Then only the matching dataset should be returned
        assert response[BODY_KEY][0][DATASET_ID_SHORT_KEY] == matching_dataset.dataset_id

    with subtests.test(msg="status code"):
        assert response[STATUS_CODE_KEY] == HTTPStatus.OK

    with subtests.test(msg="body length"):
        assert len(response[BODY_KEY]) == 1


@mark.infrastructure
def should_fail_if_get_request_requests_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {DATASET_ID_SHORT_KEY: dataset_id}

    response = lambda_handler({HTTP_METHOD_KEY: "GET", BODY_KEY: body}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.NOT_FOUND,
        BODY_KEY: {MESSAGE_KEY: f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_update_dataset(subtests: SubTests) -> None:
    new_dataset_title = any_dataset_title()

    with Dataset() as dataset:
        body = {DATASET_ID_SHORT_KEY: dataset.dataset_id, TITLE_KEY: new_dataset_title}
        response = lambda_handler({HTTP_METHOD_KEY: "PATCH", BODY_KEY: body}, any_lambda_context())
    logger.info("Response: %s", response)

    with subtests.test(msg="status code"):
        assert response[STATUS_CODE_KEY] == HTTPStatus.OK

    with subtests.test(msg="title"):
        assert response[BODY_KEY][TITLE_KEY] == new_dataset_title


@mark.infrastructure
def should_fail_if_updating_with_already_existing_dataset_title() -> None:
    dataset_title = any_dataset_title()
    body = {DATASET_ID_SHORT_KEY: any_dataset_id(), TITLE_KEY: dataset_title}

    with Dataset(title=dataset_title):
        response = lambda_handler({HTTP_METHOD_KEY: "PATCH", BODY_KEY: body}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.CONFLICT,
        BODY_KEY: {MESSAGE_KEY: f"Conflict: dataset '{dataset_title}' already exists"},
    }


@mark.infrastructure
def should_fail_if_updating_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {DATASET_ID_SHORT_KEY: dataset_id, TITLE_KEY: any_dataset_title()}
    response = lambda_handler({HTTP_METHOD_KEY: "PATCH", BODY_KEY: body}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.NOT_FOUND,
        BODY_KEY: {MESSAGE_KEY: f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_delete_dataset_with_no_versions() -> None:
    with Dataset() as dataset:
        response = lambda_handler(
            {HTTP_METHOD_KEY: "DELETE", BODY_KEY: {DATASET_ID_SHORT_KEY: dataset.dataset_id}},
            any_lambda_context(),
        )

    assert response == {STATUS_CODE_KEY: HTTPStatus.NO_CONTENT, BODY_KEY: {}}


@mark.infrastructure
def should_return_error_when_trying_to_delete_dataset_with_versions() -> None:
    with Dataset() as dataset, S3Object(
        file_object=BytesIO(),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_id}/{any_dataset_version_id()}/{any_safe_filename()}",
    ):
        response = lambda_handler(
            {HTTP_METHOD_KEY: "DELETE", BODY_KEY: {DATASET_ID_SHORT_KEY: dataset.dataset_id}},
            any_lambda_context(),
        )

    expected_message = (
        f"Conflict: Can’t delete dataset “{dataset.dataset_id}”: dataset versions still exist"
    )
    assert response == {
        STATUS_CODE_KEY: HTTPStatus.CONFLICT,
        BODY_KEY: {MESSAGE_KEY: expected_message},
    }


def should_return_error_when_trying_to_handle_get_dataset_with_wrong_property() -> None:
    response = handle_get({any_dictionary_key(): random_string(1)})

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: "Bad Request: Unhandled request"},
    }


def should_return_error_when_trying_to_get_single_dataset_with_missing_property() -> None:
    response = get_dataset_single({})

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{DATASET_ID_SHORT_KEY}' is a required property"},
    }


def should_return_error_when_trying_to_get_datasets_with_missing_property() -> None:
    response = get_dataset_filter({})

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{TITLE_KEY}' is a required property"},
    }


def should_return_error_when_trying_to_update_dataset_with_missing_property() -> None:
    response = lambda_handler({HTTP_METHOD_KEY: "PATCH", BODY_KEY: {}}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{DATASET_ID_SHORT_KEY}' is a required property"},
    }


def should_return_error_when_trying_to_delete_dataset_with_missing_id() -> None:
    response = lambda_handler({HTTP_METHOD_KEY: "DELETE", BODY_KEY: {}}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{DATASET_ID_SHORT_KEY}' is a required property"},
    }


@mark.infrastructure
def should_fail_if_deleting_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {DATASET_ID_SHORT_KEY: dataset_id, TITLE_KEY: any_dataset_title()}

    response = lambda_handler({HTTP_METHOD_KEY: "DELETE", BODY_KEY: body}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.NOT_FOUND,
        BODY_KEY: {MESSAGE_KEY: f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_launch_datasets_endpoint_lambda_function(
    lambda_client: LambdaClient, s3_client: S3Client
) -> None:
    """
    Test if datasets endpoint lambda can be successfully launched and has required permission to
    create dataset in DB.
    """
    title = any_dataset_title()

    try:
        body = {TITLE_KEY: title, DESCRIPTION_KEY: any_dataset_description()}

        resp = lambda_client.invoke(
            FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
            Payload=dumps({HTTP_METHOD_KEY: "POST", BODY_KEY: body}).encode(),
        )
        json_resp = load(resp["Payload"])

        assert json_resp.get(STATUS_CODE_KEY) == HTTPStatus.CREATED, json_resp

    finally:
        wait_for_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, CATALOG_KEY, s3_client)
        delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, CATALOG_KEY, s3_client)
        delete_s3_prefix(ResourceName.STORAGE_BUCKET_NAME.value, title, s3_client)
