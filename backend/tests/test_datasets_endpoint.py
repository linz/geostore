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
    resp = entrypoint.lambda_handler({"body": {}}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'httpMethod' is a required property"


def test_should_fail_if_request_not_containing_body():
    resp = entrypoint.lambda_handler({"httpMethod": "POST"}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'body' is a required property"


@mark.infrastructure
def test_should_create_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 201
    assert len(resp["body"]["id"]) == 32  # 32 characters long UUID
    assert resp["body"]["type"] == body["type"]
    assert resp["body"]["title"] == body["title"]
    assert resp["body"]["owning_group"] == body["owning_group"]


def test_should_fail_if_post_request_not_containing_mandatory_attribute():
    body = {}
    # body["type"] = "RASTER"  # type attribute is missing
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'type' is a required property"


def test_should_fail_if_post_request_containing_incorrect_dataset_type():
    body = {}
    body["type"] = "INCORRECT_TYPE"
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert re.search("^Bad Request: 'INCORRECT_TYPE' is not one of .*", resp["body"]["message"])


@mark.infrastructure
def test_should_fail_if_post_request_containing_duplicate_dataset_title(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"] == "Conflict: dataset 'Dataset ABC' of type 'RASTER' already exists"
    )


@mark.infrastructure
def test_should_return_single_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"

    resp = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["id"] == "111abc"
    assert resp["body"]["type"] == "RASTER"
    assert resp["body"]["title"] == "Dataset ABC"


@mark.infrastructure
def test_should_return_all_datasets(db_prepare):  # pylint:disable=unused-argument
    resp = entrypoint.lambda_handler({"httpMethod": "GET", "body": {}}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 2
    assert resp["body"][0]["id"] in ("111abc", "222xyz")
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["title"] in ("Dataset ABC", "Dataset XYZ")


@mark.infrastructure
def test_should_return_single_dataset_filtered_by_type_and_title(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"

    resp = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 1
    assert resp["body"][0]["id"] == "111abc"
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["title"] == "Dataset ABC"


@mark.infrastructure
def test_should_return_multiple_datasets_filtered_by_type_and_owning_group(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["type"] = "RASTER"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 2
    assert resp["body"][0]["id"] == "111abc"
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["owning_group"] == "A_ABC_XYZ"


@mark.infrastructure
def test_should_fail_if_get_request_containing_tile_and_owning_group_filter(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert re.search("^Bad Request: .* has too many properties", resp["body"]["message"])


@mark.infrastructure
def test_should_fail_if_get_request_requests_not_existing_dataset():
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"

    resp = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist"
    )


@mark.infrastructure
def test_should_update_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = "New Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["title"] == "New Dataset ABC"


@mark.infrastructure
def test_should_fail_if_updating_with_already_existing_dataset_title(
    db_prepare,
):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = "Dataset XYZ"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"] == "Conflict: dataset 'Dataset XYZ' of type 'RASTER' already exists"
    )


@mark.infrastructure
def test_should_fail_if_updating_not_existing_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "New Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist"
    )


@mark.infrastructure
def test_should_delete_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"

    resp = entrypoint.lambda_handler({"httpMethod": "DELETE", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 204
    assert resp["body"] == {}


@mark.infrastructure
def test_should_fail_if_deleting_not_existing_dataset(db_prepare):  # pylint:disable=unused-argument
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = entrypoint.lambda_handler({"httpMethod": "DELETE", "body": body}, "context")
    logger.info("Response: %s", resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist"
    )
