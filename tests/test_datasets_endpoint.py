"""
Dataset endpoint Lambda function tests. Working Data Lake AWS environment is
required (run '$ cdk deploy' before running tests).
"""
import json
import logging
from http import HTTPStatus
from io import BytesIO

from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.datasets import entrypoint
from backend.datasets.create import TITLE_PATTERN
from backend.resources import ResourceName

from .aws_utils import Dataset, S3Object, any_lambda_context, delete_s3_prefix
from .general_generators import any_safe_filename
from .stac_generators import (
    any_dataset_description,
    any_dataset_id,
    any_dataset_title,
    any_dataset_version_id,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@mark.infrastructure
def should_create_dataset(subtests: SubTests, s3_client: S3Client) -> None:
    dataset_title = any_dataset_title()
    body = {"title": dataset_title, "description": any_dataset_description()}

    try:

        response = entrypoint.lambda_handler(
            {"http_method": "POST", "body": body}, any_lambda_context()
        )
        logger.info("Response: %s", response)

        with subtests.test(msg="status code"):
            assert response["status_code"] == HTTPStatus.CREATED

            with subtests.test(msg="ID length"):
                assert len(response["body"]["id"]) == 41

            with subtests.test(msg="title"):
                assert response["body"]["title"] == dataset_title

    finally:
        delete_s3_prefix(ResourceName.STORAGE_BUCKET_NAME.value, dataset_title, s3_client)


@mark.infrastructure
def should_fail_if_post_request_containing_duplicate_dataset_title() -> None:
    dataset_title = any_dataset_title()
    body = {"title": dataset_title, "description": any_dataset_description()}

    with Dataset(title=dataset_title):
        response = entrypoint.lambda_handler(
            {"http_method": "POST", "body": body}, any_lambda_context()
        )

    assert response == {
        "status_code": HTTPStatus.CONFLICT,
        "body": {"message": f"Conflict: dataset '{dataset_title}' already exists"},
    }


@mark.infrastructure
def should_return_client_error_when_title_contains_unsupported_characters(
    subtests: SubTests,
) -> None:
    for character in "!@#$%^&*(){}?+| /=":
        with subtests.test(msg=character):
            response = entrypoint.lambda_handler(
                {
                    "http_method": "POST",
                    "body": {"title": character, "description": any_dataset_description()},
                },
                any_lambda_context(),
            )

            assert response == {
                "status_code": HTTPStatus.BAD_REQUEST,
                "body": {"message": f"Bad Request: '{character}' does not match '{TITLE_PATTERN}'"},
            }


@mark.infrastructure
def should_return_single_dataset(subtests: SubTests) -> None:
    # Given a dataset instance
    with Dataset() as dataset:
        body = {"id": dataset.dataset_id}

        # When requesting the dataset by ID and type
        response = entrypoint.lambda_handler(
            {"http_method": "GET", "body": body}, any_lambda_context()
        )
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    with subtests.test(msg="status code"):
        assert response["status_code"] == HTTPStatus.OK

    with subtests.test(msg="ID"):
        assert response["body"]["id"] == dataset.dataset_id


@mark.infrastructure
def should_return_all_datasets(subtests: SubTests) -> None:
    # Given two datasets
    with Dataset() as first_dataset, Dataset() as second_dataset:
        # When requesting all datasets
        response = entrypoint.lambda_handler(
            {"http_method": "GET", "body": {}}, any_lambda_context()
        )
        logger.info("Response: %s", response)

        # Then we should get both datasets in return
        with subtests.test(msg="status code"):
            assert response["status_code"] == HTTPStatus.OK

        actual_dataset_ids = [entry["id"] for entry in response["body"]]
        for dataset_id in (first_dataset.dataset_id, second_dataset.dataset_id):
            with subtests.test(msg=f"ID {dataset_id}"):
                assert dataset_id in actual_dataset_ids


@mark.infrastructure
def should_return_single_dataset_filtered_by_title(subtests: SubTests) -> None:
    # Given matching and non-matching dataset instances
    dataset_title = any_dataset_title()
    body = {"title": dataset_title}

    with Dataset(title=dataset_title) as matching_dataset, Dataset():
        # When requesting a specific type and title
        response = entrypoint.lambda_handler(
            {"http_method": "GET", "body": body}, any_lambda_context()
        )
        logger.info("Response: %s", response)

    with subtests.test(msg="ID"):
        # Then only the matching dataset should be returned
        assert response["body"][0]["id"] == matching_dataset.dataset_id

    with subtests.test(msg="status code"):
        assert response["status_code"] == HTTPStatus.OK

    with subtests.test(msg="body length"):
        assert len(response["body"]) == 1


@mark.infrastructure
def should_fail_if_get_request_requests_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {"id": dataset_id}

    response = entrypoint.lambda_handler({"http_method": "GET", "body": body}, any_lambda_context())

    assert response == {
        "status_code": HTTPStatus.NOT_FOUND,
        "body": {"message": f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_update_dataset(subtests: SubTests) -> None:
    new_dataset_title = any_dataset_title()

    with Dataset() as dataset:
        body = {"id": dataset.dataset_id, "title": new_dataset_title}
        response = entrypoint.lambda_handler(
            {
                "http_method": "PATCH",
                "body": body,
            },
            any_lambda_context(),
        )
    logger.info("Response: %s", response)

    with subtests.test(msg="status code"):
        assert response["status_code"] == HTTPStatus.OK

    with subtests.test(msg="title"):
        assert response["body"]["title"] == new_dataset_title


@mark.infrastructure
def should_fail_if_updating_with_already_existing_dataset_title() -> None:
    dataset_title = any_dataset_title()
    body = {"id": any_dataset_id(), "title": dataset_title}

    with Dataset(title=dataset_title):
        response = entrypoint.lambda_handler(
            {"http_method": "PATCH", "body": body}, any_lambda_context()
        )

    assert response == {
        "status_code": HTTPStatus.CONFLICT,
        "body": {"message": f"Conflict: dataset '{dataset_title}' already exists"},
    }


@mark.infrastructure
def should_fail_if_updating_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {"id": dataset_id, "title": any_dataset_title()}
    response = entrypoint.lambda_handler(
        {"http_method": "PATCH", "body": body}, any_lambda_context()
    )

    assert response == {
        "status_code": HTTPStatus.NOT_FOUND,
        "body": {"message": f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_delete_dataset_with_no_versions(lambda_client: LambdaClient) -> None:
    with Dataset() as dataset:
        body = {"id": dataset.dataset_id}
        raw_response = lambda_client.invoke(
            FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
            Payload=json.dumps({"http_method": "DELETE", "body": body}).encode(),
        )
        response_payload = json.load(raw_response["Payload"])

    assert response_payload == {"status_code": HTTPStatus.NO_CONTENT, "body": {}}


@mark.infrastructure
def should_return_error_when_trying_to_delete_dataset_with_versions() -> None:
    with Dataset() as dataset, S3Object(
        file_object=BytesIO(),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_id}/{any_dataset_version_id()}/{any_safe_filename()}",
    ):
        response = entrypoint.lambda_handler(
            {"http_method": "DELETE", "body": {"id": dataset.dataset_id}}, any_lambda_context()
        )

    expected_message = (
        f"Conflict: Can’t delete dataset “{dataset.dataset_id}”: dataset versions still exist"
    )
    assert response == {
        "status_code": HTTPStatus.CONFLICT,
        "body": {"message": expected_message},
    }


@mark.infrastructure
def should_fail_if_deleting_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {"id": dataset_id, "title": any_dataset_title()}

    response = entrypoint.lambda_handler(
        {"http_method": "DELETE", "body": body}, any_lambda_context()
    )

    assert response == {
        "status_code": HTTPStatus.NOT_FOUND,
        "body": {"message": f"Not Found: dataset '{dataset_id}' does not exist"},
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
        body = {"title": title, "description": any_dataset_description()}

        resp = lambda_client.invoke(
            FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
            Payload=json.dumps({"http_method": "POST", "body": body}).encode(),
        )
        json_resp = json.load(resp["Payload"])

        assert json_resp.get("status_code") == HTTPStatus.CREATED, json_resp

    finally:
        delete_s3_prefix(ResourceName.STORAGE_BUCKET_NAME.value, title, s3_client)
