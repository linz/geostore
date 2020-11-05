"""
Dataset endpoint Lambda function tests. Working Data Lake AWS environment is
required (run '$ cdk deploy' before running tests).
"""

import logging
import re

import function  # pylint:disable=import-error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_should_fail_if_request_not_containing_method():
    """Test if request fails correctly if not containing method attribute."""

    body = {}

    resp = function.lambda_handler({"body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'httpMethod' is a required property."


def test_should_fail_if_request_not_containing_body():
    """Test if request fails correctly if not containing body."""

    method = "POST"

    resp = function.lambda_handler({"httpMethod": method}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'body' is a required property."


def test_should_create_dataset(db_prepare):  # pylint:disable=unused-argument
    """Test Dataset creation using POST method."""

    method = "POST"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 201
    assert resp["body"]["type"] == body["type"]
    assert resp["body"]["title"] == body["title"]
    assert resp["body"]["owning_group"] == body["owning_group"]


def test_should_fail_if_post_request_not_containing_mandatory_attribute():
    """Test if POST request fails correctly if not containing mandatory attribute."""

    method = "POST"
    body = {}
    # body["type"] = "RASTER"  # type attribute is missing
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert resp["body"]["message"] == "Bad Request: 'type' is a required property."


def test_should_fail_if_post_request_containing_incorrect_dataset_type():
    """Test if POST request fails correctly if containing incorrect dataset type."""

    method = "POST"
    body = {}
    body["type"] = "INCORRECT_TYPE"
    body["title"] = "Dataset 123"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert re.search("^Bad Request: 'INCORRECT_TYPE' is not one of .*", resp["body"]["message"])


def test_shoud_fail_if_post_request_containing_duplicate_dataset_title(
    db_prepare,
):  # pylint:disable=unused-argument
    """
    Test if POST request fails correctly if containing duplicate dataset
    title.
    """

    method = "POST"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"]
        == "Conflict: dataset 'Dataset ABC' of type 'RASTER' already exists."
    )


def test_should_return_single_dateset(db_prepare):  # pylint:disable=unused-argument
    """Test retrieving single Dataset using GET method."""

    method = "GET"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["id"] == "111abc"
    assert resp["body"]["type"] == "RASTER"
    assert resp["body"]["title"] == "Dataset ABC"


def test_should_return_all_datesets(db_prepare):  # pylint:disable=unused-argument
    """Test retrieving all Datasets using GET method."""

    method = "GET"
    body = {}

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 2
    assert resp["body"][0]["id"] in ("111abc", "222xyz")
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["title"] in ("Dataset ABC", "Dataset XYZ")


def test_should_return_single_dataset_filtered_by_type_and_title(
    db_prepare,
):  # pylint:disable=unused-argument
    """
    Test filtering Datasets by type and title. Must return single dataset,
    because type/title combination must be unique.
    """

    method = "GET"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 1
    assert resp["body"][0]["id"] == "111abc"
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["title"] == "Dataset ABC"


def test_should_return_multiple_datasets_filtered_by_type_and_owning_group(
    db_prepare,
):  # pylint:disable=unused-argument
    """
    Test filtering Datasets by type and title.
    """

    method = "GET"
    body = {}
    body["type"] = "RASTER"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert len(resp["body"]) == 2
    assert resp["body"][0]["id"] == "111abc"
    assert resp["body"][0]["type"] == "RASTER"
    assert resp["body"][0]["owning_group"] == "A_ABC_XYZ"


def test_should_fail_if_get_request_containing_tile_and_owning_group_filter(
    db_prepare,
):  # pylint:disable=unused-argument
    """
    Test if GET request fails correctly if filter contains both tile and
    owning_group attributes.
    """

    method = "GET"
    body = {}
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 400
    assert re.search("^Bad Request: .* has too many properties", resp["body"]["message"])


def test_should_fail_if_get_request_requests_not_existing_dataset():
    """
    Test if GET request fails correctly if not existing dataset ID is specified.
    """
    method = "GET"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )


def test_should_update_dataset(db_prepare):  # pylint:disable=unused-argument
    """Test Dataset update using PATCH method."""

    method = "PATCH"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = "New Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 200
    assert resp["body"]["title"] == "New Dataset ABC"


def test_should_fail_if_updating_with_already_existing_dataset_title(
    db_prepare,
):  # pylint:disable=unused-argument
    """
    Test if PATCH request fails correctly if trying to update dataset with
    already existing dataset title.
    """

    method = "PATCH"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"
    body["title"] = "Dataset XYZ"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 409
    assert (
        resp["body"]["message"]
        == "Conflict: dataset 'Dataset XYZ' of type 'RASTER' already exists."
    )


def test_should_fail_if_updating_not_existing_dataset(db_prepare):  # pylint:disable=unused-argument
    """
    Test if PATCH request fails correctly if trying to update not existing
    dataset.
    """
    method = "PATCH"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "New Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )


def test_should_delete_dataset(db_prepare):  # pylint:disable=unused-argument
    """Test Dataset deletion using DELETE method."""

    method = "DELETE"
    body = {}
    body["id"] = "111abc"
    body["type"] = "RASTER"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 204
    assert resp["body"] == {}


def test_should_fail_if_deleting_not_existing_dataset(db_prepare):  # pylint:disable=unused-argument
    """
    Test if DELETE request fails correctly if trying to update not existing
    dataset.
    """

    method = "DELETE"
    body = {}
    body["id"] = "NOT_EXISTING_ID"
    body["type"] = "RASTER"
    body["title"] = "Dataset ABC"
    body["owning_group"] = "A_ABC_XYZ"

    resp = function.lambda_handler({"httpMethod": method, "body": body}, "context")
    logger.debug("Response: %s", resp)

    assert resp["statusCode"] == 404
    assert (
        resp["body"]["message"]
        == "Not Found: dataset 'NOT_EXISTING_ID' of type 'RASTER' does not exist."
    )
