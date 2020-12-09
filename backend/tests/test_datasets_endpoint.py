"""
Dataset endpoint Lambda function tests. Working Data Lake AWS environment is
required (run '$ cdk deploy' before running tests).
"""

import logging
import re

from pytest import mark

from ..endpoints.datasets import entrypoint
from ..endpoints.datasets.common import DATASET_TYPES
from .utils import (
    Dataset,
    any_dataset_id,
    any_dataset_owning_group,
    any_dataset_title,
    any_valid_dataset_type,
)

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
def test_should_create_dataset(db_teardown):  # pylint:disable=unused-argument
    dataset_type = any_valid_dataset_type()
    dataset_title = any_dataset_title()
    dataset_owning_group = any_dataset_owning_group()

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
    body["title"] = any_dataset_title()
    body["owning_group"] = any_dataset_owning_group()

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert response["body"]["message"] == "Bad Request: 'type' is a required property"


def test_should_fail_if_post_request_containing_incorrect_dataset_type():
    dataset_type = f"{''.join(DATASET_TYPES)}x"  # Guaranteed not in `DATASET_TYPES`
    body = {}
    body["type"] = dataset_type
    body["title"] = any_dataset_title()
    body["owning_group"] = any_dataset_owning_group()

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert re.search(
        f"^Bad Request: '{dataset_type}' is not one of .*", response["body"]["message"]
    )


@mark.infrastructure
def test_should_fail_if_post_request_containing_duplicate_dataset_title():
    dataset_type = any_valid_dataset_type()
    dataset_title = "Dataset ABC"

    body = {}
    body["type"] = dataset_type
    body["title"] = dataset_title
    body["owning_group"] = any_dataset_owning_group()

    with Dataset(dataset_type=dataset_type, title=dataset_title):
        response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 409
    assert (
        response["body"]["message"]
        == f"Conflict: dataset '{dataset_title}' of type '{dataset_type}' already exists"
    )


@mark.infrastructure
def test_should_return_single_dataset(db_teardown):  # pylint:disable=unused-argument
    # Given a dataset instance
    dataset_id = "111abc"
    dataset_type = any_valid_dataset_type()
    dataset_title = "Dataset ABC"

    body = {}
    body["id"] = dataset_id
    body["type"] = dataset_type
    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type, title=dataset_title):
        # When requesting the dataset by ID and type
        response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    assert response["statusCode"] == 200
    assert response["body"]["id"] == dataset_id
    assert response["body"]["type"] == dataset_type
    assert response["body"]["title"] == dataset_title


@mark.infrastructure
def test_should_return_all_datasets(db_teardown):  # pylint:disable=unused-argument
    # Given two datasets
    with Dataset() as first_dataset, Dataset() as second_dataset:
        # When requesting all datasets
        response = entrypoint.lambda_handler({"httpMethod": "GET", "body": {}}, "context")
        logger.info("Response: %s", response)

        # Then we should get both datasets in return
        assert response["statusCode"] == 200
        assert len(response["body"]) == 2
        assert response["body"][0]["id"] in (first_dataset.dataset_id, second_dataset.dataset_id)
        assert response["body"][0]["type"] in (
            first_dataset.dataset_type,
            second_dataset.dataset_type,
        )
        assert response["body"][0]["title"] in (first_dataset.title, second_dataset.title)


@mark.infrastructure
def test_should_return_single_dataset_filtered_by_type_and_title(
    db_teardown,
):  # pylint:disable=unused-argument
    # Given matching and non-matching dataset instances
    dataset_type = "IMAGE"
    dataset_title = "Dataset ABC"

    body = {}
    body["type"] = dataset_type
    body["title"] = dataset_title

    with Dataset(dataset_type=dataset_type, title=dataset_title) as matching_dataset, Dataset(
        dataset_type="RASTER", title=dataset_title
    ), Dataset(dataset_type=dataset_type):
        # When requesting a specific type and title
        response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
        logger.info("Response: %s", response)

        # Then only the matching dataset should be returned
        assert response["body"][0]["id"] == matching_dataset.dataset_id

    assert response["statusCode"] == 200
    assert len(response["body"]) == 1
    assert response["body"][0]["type"] == dataset_type
    assert response["body"][0]["title"] == dataset_title


@mark.infrastructure
def test_should_return_multiple_datasets_filtered_by_type_and_owning_group(
    db_teardown,
):  # pylint:disable=unused-argument
    # Given matching and non-matching dataset instances
    dataset_type = "RASTER"
    dataset_owning_group = "A_ABC_XYZ"

    body = {}
    body["type"] = dataset_type
    body["owning_group"] = dataset_owning_group

    with Dataset(
        dataset_type=dataset_type, owning_group=dataset_owning_group
    ) as first_match, Dataset(
        dataset_type=dataset_type, owning_group=dataset_owning_group
    ) as second_match, Dataset(
        dataset_type=dataset_type
    ), Dataset(
        dataset_type="IMAGE", owning_group=dataset_owning_group
    ):
        # When requesting a specific type and owning group
        response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
        logger.info("Response: %s", response)

        # Then only the matching instances should be returned
        assert response["body"][0]["id"] in (first_match.dataset_id, second_match.dataset_id)

    assert response["statusCode"] == 200
    assert len(response["body"]) == 2
    assert response["body"][0]["type"] == dataset_type
    assert response["body"][0]["owning_group"] == dataset_owning_group


@mark.infrastructure
def test_should_fail_if_get_request_containing_tile_and_owning_group_filter():
    body = {}
    body["type"] = any_valid_dataset_type()
    body["title"] = any_dataset_title()
    body["owning_group"] = any_dataset_owning_group()

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert re.search("^Bad Request: .* has too many properties", response["body"]["message"])


@mark.infrastructure
def test_should_fail_if_get_request_requests_not_existing_dataset(
    db_teardown,
):  # pylint:disable=unused-argument
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {}
    body["id"] = dataset_id
    body["type"] = dataset_type

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 404
    assert (
        response["body"]["message"]
        == f"Not Found: dataset '{dataset_id}' of type '{dataset_type}' does not exist"
    )


@mark.infrastructure
def test_should_update_dataset(db_teardown):  # pylint:disable=unused-argument
    dataset_id = "111abc"
    dataset_type = any_valid_dataset_type()
    new_dataset_title = "New Dataset ABC"

    body = {}
    body["id"] = dataset_id
    body["type"] = dataset_type
    body["title"] = new_dataset_title
    body["owning_group"] = any_dataset_owning_group()

    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):
        response = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 200
    assert response["body"]["title"] == new_dataset_title


@mark.infrastructure
def test_should_fail_if_updating_with_already_existing_dataset_title(
    db_teardown,
):  # pylint:disable=unused-argument
    dataset_type = any_valid_dataset_type()
    dataset_title = "Dataset XYZ"

    body = {}
    body["id"] = "111abc"
    body["type"] = dataset_type
    body["title"] = dataset_title
    body["owning_group"] = any_dataset_owning_group()

    with Dataset(dataset_type=dataset_type, title=dataset_title):
        response = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 409
    assert (
        response["body"]["message"]
        == f"Conflict: dataset '{dataset_title}' of type '{dataset_type}' already exists"
    )


@mark.infrastructure
def test_should_fail_if_updating_not_existing_dataset(
    db_teardown,
):  # pylint:disable=unused-argument
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {}
    body["id"] = dataset_id
    body["type"] = dataset_type
    body["title"] = "New Dataset ABC"
    body["owning_group"] = any_dataset_owning_group()

    response = entrypoint.lambda_handler({"httpMethod": "PATCH", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 404
    assert (
        response["body"]["message"]
        == f"Not Found: dataset '{dataset_id}' of type '{dataset_type}' does not exist"
    )


@mark.infrastructure
def test_should_delete_dataset(db_teardown):  # pylint:disable=unused-argument
    dataset_id = "111abc"
    dataset_type = any_valid_dataset_type()

    body = {}
    body["id"] = dataset_id
    body["type"] = dataset_type

    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):
        response = entrypoint.lambda_handler({"httpMethod": "DELETE", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 204
    assert response["body"] == {}


@mark.infrastructure
def test_should_fail_if_deleting_not_existing_dataset(
    db_teardown,
):  # pylint:disable=unused-argument
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {}
    body["id"] = dataset_id
    body["type"] = dataset_type
    body["title"] = "Dataset ABC"
    body["owning_group"] = any_dataset_owning_group()

    response = entrypoint.lambda_handler({"httpMethod": "DELETE", "body": body}, "context")
    logger.info("Response: %s", response)

    assert response["statusCode"] == 404
    assert (
        response["body"]["message"]
        == f"Not Found: dataset '{dataset_id}' of type '{dataset_type}' does not exist"
    )
