"""
Dataset endpoint Lambda function tests. Working Data Lake AWS environment is
required (run '$ cdk deploy' before running tests).
"""

import logging
import re

from pytest import mark

from ..endpoints.datasets import entrypoint

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_should_fail_if_request_not_containing_method():
    response = entrypoint.lambda_handler({"body": {}}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert response["body"]["message"] == "Bad Request: 'httpMethod' is a required property"


def test_should_fail_if_request_not_containing_body():
    response = entrypoint.lambda_handler({"httpMethod": "POST"}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert response["body"]["message"] == "Bad Request: 'body' is a required property"


@mark.infrastructure
def test_should_create_dataset(db_prepare):  # pylint:disable=unused-argument
    dataset_type = "RASTER"
    dataset_title = "Dataset 123"
    dataset_owning_group = "A_ABC_XYZ"

    body = {}
    body["type"] = dataset_type
    body["title"] = dataset_title
    body["owning_group"] = dataset_owning_group

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 201
    assert len(response["body"]["id"]) == 32  # 32 characters long UUID
    assert response["body"]["type"] == dataset_type
    assert response["body"]["title"] == dataset_title
    assert response["body"]["owning_group"] == dataset_owning_group


def test_should_fail_if_post_request_not_containing_mandatory_attribute():
    body = {}
    # body["type"] = "RASTER"  # type attribute is missing
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert response["body"]["message"] == "Bad Request: 'type' is a required property"


def test_should_fail_if_post_request_containing_incorrect_dataset_type():
    body = {}
    body["type"] = "INCORRECT_TYPE"
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert re.search("^Bad Request: 'INCORRECT_TYPE' is not one of .*", response["body"]["message"])


@mark.infrastructure
def test_should_fail_if_post_request_containing_duplicate_dataset_title(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 409
    assert (
        response["body"]["message"]
        == "Conflict: dataset 'Dataset ABC' of type 'RASTER' already exists"
    )


@mark.infrastructure
def test_should_return_single_dataset(db_prepare):  # pylint:disable=unused-argument
    dataset_id = "111abc"
    dataset_type = "RASTER"

    body = {}
    body["id"] = dataset_id
    body["type"] = dataset_type

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 200
    assert response["body"]["id"] == dataset_id
    assert response["body"]["type"] == dataset_type
    assert response["body"]["title"] == "Dataset ABC"


@mark.infrastructure
def test_should_return_all_datasets(db_prepare):  # pylint:disable=unused-argument
    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": {}}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 200
    assert len(response["body"]) == 2
    assert response["body"][0]["id"] in ("111abc", "222xyz")
    assert response["body"][0]["type"] == "RASTER"
    assert response["body"][0]["title"] in ("Dataset ABC", "Dataset XYZ")


@mark.infrastructure
def test_should_return_single_dataset_filtered_by_type_and_title(
    db_prepare,
):  # pylint:disable=unused-argument
    dataset_type = "RASTER"
    dataset_title = "Dataset ABC"

    body = {}
    body["type"] = dataset_type
    body["title"] = dataset_title

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 200
    assert len(response["body"]) == 1
    assert response["body"][0]["id"] == "111abc"
    assert response["body"][0]["type"] == dataset_type
    assert response["body"][0]["title"] == dataset_title


@mark.infrastructure
def test_should_return_multiple_datasets_filtered_by_type_and_owning_group(
    db_prepare,
):  # pylint:disable=unused-argument
    dataset_type = "RASTER"
    dataset_owning_group = "A_ABC_XYZ"

    body = {}
    body["type"] = dataset_type
    body["owning_group"] = dataset_owning_group

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 200
    assert len(response["body"]) == 2
    assert response["body"][0]["id"] == "111abc"
    assert response["body"][0]["type"] == dataset_type
    assert response["body"][0]["owning_group"] == dataset_owning_group


@mark.infrastructure
def test_should_fail_if_get_request_containing_tile_and_owning_group_filter(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert re.search("^Bad Request: .* has too many properties", response["body"]["message"])


@mark.infrastructure
def test_should_fail_if_get_request_requests_not_existing_dataset():
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 404
    assert (
        response["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist"
    )


@mark.infrastructure
def test_should_update_dataset(db_prepare):  # pylint:disable=unused-argument
    new_dataset_title = "New Dataset ABC"

    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = new_dataset_title
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 200
    assert response["body"]["title"] == new_dataset_title


@mark.infrastructure
def test_should_fail_if_updating_with_already_existing_dataset_title(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = "Dataset XYZ"
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 409
    assert (
        response["body"]["message"]
        == "Conflict: dataset 'Dataset XYZ' of type 'RASTER' already exists"
    )


@mark.infrastructure
def test_should_fail_if_updating_not_existing_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "New Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 404
    assert (
        response["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist"
    )


@mark.infrastructure
def test_should_delete_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"

    response = entrypoint.lambda_handler({"httpMethod": "DELETE", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 204
    assert response["body"] == {}


@mark.infrastructure
def test_should_fail_if_deleting_not_existing_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    response = entrypoint.lambda_handler({"httpMethod": "DELETE", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 404
    assert (
        response["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist"
    )
