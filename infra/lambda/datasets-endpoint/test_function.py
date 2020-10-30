"""
Basic Lambda function tests. Working Data Lake AWS environment is required
(run '$ cdk deploy' before running tests).
"""

import logging
import re
import uuid

import function  # pylint:disable=import-error

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


def test_post_method(db_prepare):  # pylint:disable=unused-argument
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


def test_post_method_already_existing_title(db_prepare):  # pylint:disable=unused-argument
    """Try to create Dataset with already existing title."""

    method = "POST"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"]
        == "Conflict: dataset 'Dataset ABC' of type 'RASTER' already exists."
    )


def test_get_method_single(db_prepare):  # pylint:disable=unused-argument
    """Test retrieving single Dataset using GET method."""

    method = "GET"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["id"] == "111abc"
    assert resp["body"]["type"] == "RASTER"
    assert resp["body"]["title"] == "Dataset ABC"


def test_get_method_all(db_prepare):  # pylint:disable=unused-argument
    """Test retrieving all Datasets using GET method."""

    method = "GET"
    body = {}

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 2
    assert resp["body"][0]["id"] in ("111abc", "222xyz")
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["title"] in ("Dataset ABC", "Dataset XYZ")


def test_get_method_filter_title(db_prepare):  # pylint:disable=unused-argument
    """Test filtering Datasets by title."""

    method = "GET"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 1
    assert resp["body"][0]["id"] == "111abc"
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["title"] == "Dataset ABC"


def test_get_method_filter_owning_group(db_prepare):  # pylint:disable=unused-argument
    """Test filtering Datasets by owning_group."""

    method = "GET"
    body = {}
    body["type"] = "RASTER"
    body["owning_group"] = "A_ABC_ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 1
    assert resp["body"][0]["id"] == "111abc"
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["owning_group"] == "A_ABC_ABC"


def test_get_method_multiple_filters(db_prepare):  # pylint:disable=unused-argument
    """Test filtering Datasets by by both title and owning_group."""

    method = "GET"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 400
    assert re.search("^Bad Request: .* has too many properties", resp["body"]["message"])


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


def test_patch_method(db_prepare):  # pylint:disable=unused-argument
    """Test Dataset update using PATCH method."""

    method = "PATCH"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = "New Dataset ABC"
    body["owning_group"] = "A_ABC_ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["title"] == "New Dataset ABC"


def test_patch_method_already_existing_title(db_prepare):  # pylint:disable=unused-argument
    """Try to update Dataset with already existing title."""

    method = "PATCH"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = "Dataset XYZ"
    body["owning_group"] = "A_XYZ_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"]
        == "Conflict: dataset 'Dataset XYZ' of type 'RASTER' already exists."
    )


def test_patch_method_not_existing(db_prepare):  # pylint:disable=unused-argument
    """Try to update not existing Dataset."""

    method = "PATCH"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "New Dataset ABC"
    body["owning_group"] = "A_ABC_ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )


def test_delete_method(db_prepare):  # pylint:disable=unused-argument
    """Test Dataset deletion using DELETE method."""

    method = "DELETE"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 204
    assert resp["body"] == {}


def test_delete_method_not_existing(db_prepare):  # pylint:disable=unused-argument
    """Try to delete not existing Dataset."""

    method = "DELETE"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.info(resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )
