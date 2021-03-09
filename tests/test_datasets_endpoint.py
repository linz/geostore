"""
Dataset endpoint Lambda function tests. Working Data Lake AWS environment is
required (run '$ cdk deploy' before running tests).
"""
import json
import logging
import re

import _pytest
from mypy_boto3_lambda import LambdaClient
from pytest import mark

from backend.dataset import DATASET_TYPES
from backend.datasets import entrypoint
from backend.resources import ResourceName

from .aws_utils import Dataset, any_lambda_context
from .stac_generators import (
    any_dataset_id,
    any_dataset_owning_group,
    any_dataset_title,
    any_valid_dataset_type,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def should_fail_if_request_not_containing_method() -> None:
    response = entrypoint.lambda_handler({"body": {}}, any_lambda_context())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'httpMethod' is a required property"},
    }


def should_fail_if_request_not_containing_body() -> None:
    response = entrypoint.lambda_handler({"httpMethod": "POST"}, any_lambda_context())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'body' is a required property"},
    }


@mark.infrastructure
def should_create_dataset(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    dataset_type = any_valid_dataset_type()
    dataset_title = any_dataset_title()
    dataset_owning_group = any_dataset_owning_group()

    body = {"type": dataset_type, "title": dataset_title, "owning_group": dataset_owning_group}

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())
    logger.info("Response: %s", response)

    assert response["statusCode"] == 201
    assert len(response["body"]["id"]) == 32  # 32 characters long UUID
    assert response["body"]["type"] == dataset_type
    assert response["body"]["title"] == dataset_title
    assert response["body"]["owning_group"] == dataset_owning_group


def should_fail_if_post_request_not_containing_mandatory_attribute() -> None:
    # Given a missing "type" attribute in the body
    body = {"title": any_dataset_title(), "owning_group": any_dataset_owning_group()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'type' is a required property"},
    }


def should_fail_if_post_request_containing_incorrect_dataset_type() -> None:
    dataset_type = f"{''.join(DATASET_TYPES)}x"  # Guaranteed not in `DATASET_TYPES`
    body = {
        "type": dataset_type,
        "title": any_dataset_title(),
        "owning_group": any_dataset_owning_group(),
    }

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert re.search(
        f"^Bad Request: '{dataset_type}' is not one of .*", response["body"]["message"]
    )


@mark.infrastructure
def should_fail_if_post_request_containing_duplicate_dataset_title() -> None:
    dataset_type = any_valid_dataset_type()
    dataset_title = "Dataset ABC"

    body = {
        "type": dataset_type,
        "title": dataset_title,
        "owning_group": any_dataset_owning_group(),
    }
    with Dataset(dataset_type=dataset_type, title=dataset_title):
        response = entrypoint.lambda_handler(
            {"httpMethod": "POST", "body": body}, any_lambda_context()
        )

    assert response == {
        "statusCode": 409,
        "body": {
            "message": (
                f"Conflict: dataset '{dataset_title}' of type '{dataset_type}' already exists"
            )
        },
    }


@mark.infrastructure
def should_return_single_dataset(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    # Given a dataset instance
    dataset_id = "111abc"
    dataset_type = any_valid_dataset_type()

    body = {"id": dataset_id, "type": dataset_type}
    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):
        # When requesting the dataset by ID and type
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": body}, any_lambda_context()
        )
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    assert response["statusCode"] == 200
    assert response["body"]["id"] == dataset_id


@mark.infrastructure
def should_return_all_datasets(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    # Given two datasets
    with Dataset() as first_dataset, Dataset() as second_dataset:
        # When requesting all datasets
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": {}}, any_lambda_context()
        )
        logger.info("Response: %s", response)

        # Then we should get both datasets in return
        assert response["statusCode"] == 200
        assert len(response["body"]) == 2
        assert response["body"][0]["id"] in (first_dataset.dataset_id, second_dataset.dataset_id)


@mark.infrastructure
def should_return_single_dataset_filtered_by_type_and_title(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    # Given matching and non-matching dataset instances
    dataset_type = "IMAGE"
    dataset_title = "Dataset ABC"

    body = {"type": dataset_type, "title": dataset_title}

    with Dataset(dataset_type=dataset_type, title=dataset_title) as matching_dataset, Dataset(
        dataset_type="RASTER", title=dataset_title
    ), Dataset(dataset_type=dataset_type):
        # When requesting a specific type and title
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": body}, any_lambda_context()
        )
        logger.info("Response: %s", response)

        # Then only the matching dataset should be returned
        assert response["body"][0]["id"] == matching_dataset.dataset_id

    assert response["statusCode"] == 200
    assert len(response["body"]) == 1


@mark.infrastructure
def should_return_multiple_datasets_filtered_by_type_and_owning_group(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    # Given matching and non-matching dataset instances
    dataset_type = "RASTER"
    dataset_owning_group = "A_ABC_XYZ"

    body = {"type": dataset_type, "owning_group": dataset_owning_group}

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
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": body}, any_lambda_context()
        )
        logger.info("Response: %s", response)

        # Then only the matching instances should be returned
        assert response["body"][0]["id"] in (first_match.dataset_id, second_match.dataset_id)

    assert response["statusCode"] == 200
    assert len(response["body"]) == 2
    assert response["body"][0]["type"] == dataset_type
    assert response["body"][0]["owning_group"] == dataset_owning_group


@mark.infrastructure
def should_fail_if_get_request_containing_tile_and_owning_group_filter() -> None:
    body = {
        "type": any_valid_dataset_type(),
        "title": any_dataset_title(),
        "owning_group": any_dataset_owning_group(),
    }

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, any_lambda_context())
    logger.info("Response: %s", response)

    assert response["statusCode"] == 400
    assert re.search("^Bad Request: .* has too many properties", response["body"]["message"])


@mark.infrastructure
def should_fail_if_get_request_requests_not_existing_dataset(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {"id": dataset_id, "type": dataset_type}

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, any_lambda_context())

    assert response == {
        "statusCode": 404,
        "body": {
            "message": f"Not Found: dataset '{dataset_id}' of type '{dataset_type}' does not exist"
        },
    }


@mark.infrastructure
def should_update_dataset(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    dataset_id = "111abc"
    dataset_type = any_valid_dataset_type()
    new_dataset_title = "New Dataset ABC"

    body = {
        "id": dataset_id,
        "type": dataset_type,
        "title": new_dataset_title,
        "owning_group": any_dataset_owning_group(),
    }

    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):
        response = entrypoint.lambda_handler(
            {"httpMethod": "PATCH", "body": body}, any_lambda_context()
        )
    logger.info("Response: %s", response)

    assert response["statusCode"] == 200
    assert response["body"]["title"] == new_dataset_title


@mark.infrastructure
def should_fail_if_updating_with_already_existing_dataset_title(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    dataset_type = any_valid_dataset_type()
    dataset_title = "Dataset XYZ"

    body = {
        "id": any_dataset_id(),
        "type": dataset_type,
        "title": dataset_title,
        "owning_group": any_dataset_owning_group(),
    }

    with Dataset(dataset_type=dataset_type, title=dataset_title):
        response = entrypoint.lambda_handler(
            {"httpMethod": "PATCH", "body": body}, any_lambda_context()
        )

    assert response == {
        "statusCode": 409,
        "body": {
            "message": (
                f"Conflict: dataset '{dataset_title}' of type '{dataset_type}' already exists"
            )
        },
    }


@mark.infrastructure
def should_fail_if_updating_not_existing_dataset(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {
        "id": dataset_id,
        "type": dataset_type,
        "title": any_dataset_title(),
        "owning_group": any_dataset_owning_group(),
    }
    response = entrypoint.lambda_handler(
        {"httpMethod": "PATCH", "body": body}, any_lambda_context()
    )

    assert response == {
        "statusCode": 404,
        "body": {
            "message": f"Not Found: dataset '{dataset_id}' of type '{dataset_type}' does not exist"
        },
    }


@mark.infrastructure
def should_delete_dataset(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {"id": dataset_id, "type": dataset_type}

    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):
        response = entrypoint.lambda_handler(
            {"httpMethod": "DELETE", "body": body}, any_lambda_context()
        )

    assert response == {"statusCode": 204, "body": {}}


@mark.infrastructure
def should_fail_if_deleting_not_existing_dataset(
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {
        "id": dataset_id,
        "type": dataset_type,
        "title": any_dataset_title(),
        "owning_group": any_dataset_owning_group(),
    }

    response = entrypoint.lambda_handler(
        {"httpMethod": "DELETE", "body": body}, any_lambda_context()
    )

    assert response == {
        "statusCode": 404,
        "body": {
            "message": f"Not Found: dataset '{dataset_id}' of type '{dataset_type}' does not exist"
        },
    }


@mark.infrastructure
def should_launch_datasets_endpoint_lambda_function(
    lambda_client: LambdaClient,
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    """
    Test if datasets endpoint lambda can be successfully launched and has required permission to
    create dataset in DB.
    """

    method = "POST"
    body = {
        "type": any_valid_dataset_type(),
        "title": any_dataset_title(),
        "owning_group": any_dataset_owning_group(),
    }

    resp = lambda_client.invoke(
        FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
        Payload=json.dumps({"httpMethod": method, "body": body}).encode(),
        InvocationType="RequestResponse",
    )
    json_resp = json.load(resp["Payload"])

    assert json_resp.get("statusCode") == 201, json_resp
