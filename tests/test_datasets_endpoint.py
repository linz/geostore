"""
Dataset endpoint Lambda function tests. Working Data Lake AWS environment is
required (run '$ cdk deploy' before running tests).
"""
import json
import logging
from http import HTTPStatus
from io import BytesIO

from mypy_boto3_lambda import LambdaClient
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.datasets import entrypoint
from backend.parameter_store import ParameterName, get_param
from backend.resources import ResourceName

from .aws_utils import Dataset, S3Object, any_lambda_context
from .general_generators import any_safe_filename
from .stac_generators import any_dataset_id, any_dataset_title, any_dataset_version_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@mark.infrastructure
def should_create_dataset(subtests: SubTests) -> None:
    dataset_title = any_dataset_title()

    body = {"title": dataset_title}

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())
    logger.info("Response: %s", response)

    with subtests.test(msg="status code"):
        assert response["statusCode"] == 201

    with subtests.test(msg="ID length"):
        assert len(response["body"]["id"]) == 26  # ULID

    with subtests.test(msg="title"):
        assert response["body"]["title"] == dataset_title


@mark.infrastructure
def should_fail_if_post_request_containing_duplicate_dataset_title() -> None:
    dataset_title = "Dataset ABC"
    body = {"title": dataset_title}

    with Dataset(title=dataset_title):
        response = entrypoint.lambda_handler(
            {"httpMethod": "POST", "body": body}, any_lambda_context()
        )

    assert response == {
        "statusCode": HTTPStatus.CONFLICT,
        "body": {"message": f"Conflict: dataset '{dataset_title}' already exists"},
    }


@mark.infrastructure
def should_return_single_dataset(subtests: SubTests) -> None:
    # Given a dataset instance
    dataset_id = any_dataset_id()
    body = {"id": dataset_id}

    with Dataset(dataset_id=dataset_id):
        # When requesting the dataset by ID and type
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": body}, any_lambda_context()
        )
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    with subtests.test(msg="status code"):
        assert response["statusCode"] == 200

    with subtests.test(msg="ID"):
        assert response["body"]["id"] == dataset_id


@mark.infrastructure
def should_return_all_datasets(subtests: SubTests) -> None:
    # Given two datasets
    with Dataset() as first_dataset, Dataset() as second_dataset:
        # When requesting all datasets
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": {}}, any_lambda_context()
        )
        logger.info("Response: %s", response)

        # Then we should get both datasets in return
        with subtests.test(msg="status code"):
            assert response["statusCode"] == 200

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
            {"httpMethod": "GET", "body": body}, any_lambda_context()
        )
        logger.info("Response: %s", response)

    with subtests.test(msg="ID"):
        # Then only the matching dataset should be returned
        assert response["body"][0]["id"] == matching_dataset.dataset_id

    with subtests.test(msg="status code"):
        assert response["statusCode"] == 200

    with subtests.test(msg="body length"):
        assert len(response["body"]) == 1


@mark.infrastructure
def should_fail_if_get_request_requests_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {"id": dataset_id}

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, any_lambda_context())

    assert response == {
        "statusCode": HTTPStatus.NOT_FOUND,
        "body": {"message": f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_update_dataset(subtests: SubTests) -> None:
    dataset_id = any_dataset_id()
    new_dataset_title = any_dataset_title()
    body = {"id": dataset_id, "title": new_dataset_title}

    with Dataset(dataset_id=dataset_id):
        response = entrypoint.lambda_handler(
            {
                "httpMethod": "PATCH",
                "body": body,
            },
            any_lambda_context(),
        )
    logger.info("Response: %s", response)

    with subtests.test(msg="status code"):
        assert response["statusCode"] == 200

    with subtests.test(msg="title"):
        assert response["body"]["title"] == new_dataset_title


@mark.infrastructure
def should_fail_if_updating_with_already_existing_dataset_title() -> None:
    dataset_title = any_dataset_title()
    body = {"id": any_dataset_id(), "title": dataset_title}

    with Dataset(title=dataset_title):
        response = entrypoint.lambda_handler(
            {"httpMethod": "PATCH", "body": body}, any_lambda_context()
        )

    assert response == {
        "statusCode": HTTPStatus.CONFLICT,
        "body": {"message": f"Conflict: dataset '{dataset_title}' already exists"},
    }


@mark.infrastructure
def should_fail_if_updating_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {"id": dataset_id, "title": any_dataset_title()}
    response = entrypoint.lambda_handler(
        {"httpMethod": "PATCH", "body": body}, any_lambda_context()
    )

    assert response == {
        "statusCode": HTTPStatus.NOT_FOUND,
        "body": {"message": f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_delete_dataset_with_no_versions(lambda_client: LambdaClient) -> None:
    dataset_id = any_dataset_id()
    body = {"id": dataset_id}

    with Dataset(dataset_id=dataset_id):
        raw_response = lambda_client.invoke(
            FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
            Payload=json.dumps({"httpMethod": "DELETE", "body": body}).encode(),
        )
        response_payload = json.load(raw_response["Payload"])

    assert response_payload == {"statusCode": HTTPStatus.NO_CONTENT, "body": {}}


@mark.infrastructure
def should_return_error_when_trying_to_delete_dataset_with_versions() -> None:
    storage_bucket_name = get_param(ParameterName.STORAGE_BUCKET_NAME)
    with Dataset() as dataset, S3Object(
        file_object=BytesIO(),
        bucket_name=storage_bucket_name,
        key=f"{dataset.dataset_id}/{any_dataset_version_id()}/{any_safe_filename()}",
    ):
        response = entrypoint.lambda_handler(
            {"httpMethod": "DELETE", "body": {"id": dataset.dataset_id}}, any_lambda_context()
        )

    expected_message = (
        f"Conflict: Can’t delete dataset “{dataset.dataset_id}”: dataset versions still exist"
    )
    assert response == {
        "statusCode": HTTPStatus.CONFLICT,
        "body": {"message": expected_message},
    }


@mark.infrastructure
def should_fail_if_deleting_not_existing_dataset() -> None:
    dataset_id = any_dataset_id()

    body = {"id": dataset_id, "title": any_dataset_title()}

    response = entrypoint.lambda_handler(
        {"httpMethod": "DELETE", "body": body}, any_lambda_context()
    )

    assert response == {
        "statusCode": HTTPStatus.NOT_FOUND,
        "body": {"message": f"Not Found: dataset '{dataset_id}' does not exist"},
    }


@mark.infrastructure
def should_launch_datasets_endpoint_lambda_function(lambda_client: LambdaClient) -> None:
    """
    Test if datasets endpoint lambda can be successfully launched and has required permission to
    create dataset in DB.
    """

    method = "POST"
    body = {"title": any_dataset_title()}

    resp = lambda_client.invoke(
        FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
        Payload=json.dumps({"httpMethod": method, "body": body}).encode(),
    )
    json_resp = json.load(resp["Payload"])

    assert json_resp.get("statusCode") == 201, json_resp
