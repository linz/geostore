"""
Basic Lambda function tests. Working Data Lake AWS environment is required
(run '$ cdk deploy' before running tests).
"""

import logging
import re
import uuid

import function  # pylint:disable=import-error
import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_request_no_method():
    """Try request not containing method."""

    body = {}

    resp = function.lambda_handler({"body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'httpMethod' is a required property."


def test_request_no_body():
    """Try request not containing body."""

    method = "POST"

    resp = function.lambda_handler({"httpMethod": method}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'body' is a required property."


def test_post_method(db_truncate):  # pylint:disable=unused-argument
    """Test Dataset creation using POST method."""

    method = "POST"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset {}".format(uuid.uuid4().hex[:8])
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 201
    assert resp["body"]["type"] == body["type"]
    assert resp["body"]["title"] == body["title"]
    assert resp["body"]["owning_group"] == body["owning_group"]

    pytest.dataset_id = resp["body"]["id"]
    pytest.dataset_type = resp["body"]["type"]
    pytest.dataset_title = resp["body"]["title"]


def test_post_method_missing_attr():
    """Try to create Dataset with missing attribute."""

    method = "POST"
    body = {}
    # body["type"] = "RASTER"  # type attribute is missing
    body["title"] = "Dataset {}".format(uuid.uuid4().hex[:8])
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'type' is a required property."


def test_post_method_incorrect_attr_value():
    """Try to create Dataset with incorrect dataset type value."""

    method = "POST"
    body = {}
    body["type"] = "INCORRECT_TYPE"
    body["title"] = "Dataset {}".format(uuid.uuid4().hex[:8])
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 400
    assert re.search("^Bad Request: 'INCORRECT_TYPE' is not one of .*", resp["body"]["message"])


def test_post_method_already_existing_title():
    """Try to create Dataset with already existing title."""

    method = "POST"
    body = {}
    body["type"] = "RASTER"
    body["title"] = pytest.dataset_title
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"]
        == f"Conflict: dataset '{pytest.dataset_title}' of type 'RASTER' already exists."
    )


def test_get_method_single():
    """Test retrieving single Dataset using GET method."""

    method = "GET"
    body = {}
    body["id"] = pytest.dataset_id
    body["type"] = pytest.dataset_type

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["id"] == pytest.dataset_id
    assert resp["body"]["type"] == pytest.dataset_type
    assert resp["body"]["title"] == pytest.dataset_title


def test_get_method_all():
    """Test retrieving all Datasets using GET method."""

    method = "GET"
    body = {}

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 1
    assert resp["body"][0]["id"] == pytest.dataset_id
    assert resp["body"][0]["type"] == pytest.dataset_type
    assert resp["body"][0]["title"] == pytest.dataset_title


def test_get_method_not_existing():
    """Try to retrieve not existing Dataset."""

    method = "GET"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )


def test_patch_method():
    """Test Dataset update using PATCH method."""

    method = "PATCH"
    body = {}
    body["id"] = pytest.dataset_id
    body["type"] = "RASTER"
    body["title"] = "New {}".format(pytest.dataset_title)
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["title"] == "New {}".format(pytest.dataset_title)


def test_patch_method_already_existing_title():
    """Try to update Dataset with already existing title."""

    method = "PATCH"
    body = {}
    body["id"] = pytest.dataset_id
    body["type"] = "RASTER"
    body["title"] = "New {}".format(pytest.dataset_title)
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"]
        == f"Conflict: dataset '{body['title']}' of type 'RASTER' already exists."
    )


def test_patch_method_not_existing():
    """Try to update not existing Dataset."""

    method = "PATCH"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "Updated {}".format(pytest.dataset_title)
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )


def test_delete_method():
    """Test Dataset deletion using DELETE method."""

    method = "DELETE"
    body = {}
    body["id"] = pytest.dataset_id
    body["type"] = "RASTER"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 204
    assert resp["body"] == {}


def test_delete_method_not_existing():
    """Try to delete not existing Dataset."""

    method = "DELETE"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "New Dataset title"
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )
