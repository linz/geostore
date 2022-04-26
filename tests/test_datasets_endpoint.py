"""
Dataset endpoint Lambda function tests. Working Geostore AWS environment is
required (run '$ cdk deploy' before running tests).
"""
from http import HTTPStatus
from io import BytesIO
from json import dumps, load
from logging import INFO, basicConfig

from mypy_boto3_lambda import LambdaClient
from pytest import mark
from pytest_subtests import SubTests

from geostore.api_keys import MESSAGE_KEY
from geostore.aws_keys import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from geostore.dataset_properties import TITLE_PATTERN
from geostore.datasets.entrypoint import lambda_handler
from geostore.datasets.get import get_dataset_filter, get_dataset_single, handle_get
from geostore.resources import Resource
from geostore.step_function_keys import DATASET_ID_SHORT_KEY, DESCRIPTION_KEY, TITLE_KEY

from .aws_utils import Dataset, S3Object, any_lambda_context
from .general_generators import any_dictionary_key, any_safe_filename, random_string
from .stac_generators import (
    any_dataset_description,
    any_dataset_id,
    any_dataset_title,
    any_dataset_version_id,
)

basicConfig(level=INFO)


@mark.infrastructure
def should_create_dataset(subtests: SubTests) -> None:
    dataset_title = any_dataset_title()
    dataset_description = any_dataset_description()
    body = {TITLE_KEY: dataset_title, DESCRIPTION_KEY: dataset_description}

    response = lambda_handler({HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context())

    with subtests.test(msg="status code"):
        assert response[STATUS_CODE_KEY] == HTTPStatus.CREATED

    with subtests.test(msg="ID length"):
        assert len(response[BODY_KEY][DATASET_ID_SHORT_KEY]) == 26

    with subtests.test(msg="title"):
        assert response[BODY_KEY][TITLE_KEY] == dataset_title


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
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
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
def should_launch_datasets_endpoint_lambda_function(lambda_client: LambdaClient) -> None:
    """
    Test if datasets endpoint lambda can be successfully launched and has required permission to
    create dataset in DB.
    """
    title = any_dataset_title()

    body = {TITLE_KEY: title, DESCRIPTION_KEY: any_dataset_description()}

    resp = lambda_client.invoke(
        FunctionName=Resource.DATASETS_ENDPOINT_FUNCTION_NAME.resource_name,
        Payload=dumps({HTTP_METHOD_KEY: "POST", BODY_KEY: body}).encode(),
    )
    json_resp = load(resp["Payload"])

    assert json_resp.get(STATUS_CODE_KEY) == HTTPStatus.CREATED, json_resp
