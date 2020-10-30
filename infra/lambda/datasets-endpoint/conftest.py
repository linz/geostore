"""
Pytest configuration file.
"""

import logging

import boto3
import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture()
def db_prepare(table_name="datasets"):
    """
    Prepare DB with some dataset records and clean it up after test is
    finished.
    """

    logger.info("Running DB Setup")

    DYNAMODB = boto3.client("dynamodb")
    DYNAMODB.batch_write_item(
        RequestItems={
            table_name: [
                {
                    "PutRequest": {
                        "Item": {
                            "pk": {
                                "S": "DATASET#111abc",
                            },
                            "sk": {
                                "S": "TYPE#RASTER",
                            },
                            "title": {
                                "S": "Dataset ABC",
                            },
                            "owning_group": {
                                "S": "A_ABC_ABC",
                            },
                            "created_at": {
                                "S": "2020-01-01 01:01:01.000000+00:00",
                            },
                        },
                    },
                },
                {
                    "PutRequest": {
                        "Item": {
                            "pk": {
                                "S": "DATASET#222xyz",
                            },
                            "sk": {
                                "S": "TYPE#RASTER",
                            },
                            "title": {
                                "S": "Dataset XYZ",
                            },
                            "owning_group": {
                                "S": "A_XYZ_XYZ",
                            },
                            "created_at": {
                                "S": "2020-02-01 01:01:01.000000+00:00",
                            },
                        },
                    },
                },
            ],
        },
    )

    yield  # teardown

    logger.info("Running DB Teardown")

    DYNAMODB = boto3.resource("dynamodb")
    table = DYNAMODB.Table(table_name)

    # get the table keys
    key_names = [key["AttributeName"] for key in table.key_schema]

    # only retrieve the keys for each item in the table (minimize data transfer)
    projection_expression = ", ".join("#" + key for key in key_names)
    expression_attribute_names = {"#" + key: key for key in key_names}

    counter = 0
    page = table.scan(
        ProjectionExpression=projection_expression,
        ExpressionAttributeNames=expression_attribute_names,
    )

    with table.batch_writer() as batch:
        while page["Count"] > 0:
            counter += page["Count"]

            # delete items in batches
            for itemKeys in page["Items"]:
                batch.delete_item(Key=itemKeys)

            # fetch the next page
            if "LastEvaluatedKey" in page:
                page = table.scan(
                    ProjectionExpression=projection_expression,
                    ExpressionAttributeNames=expression_attribute_names,
                    ExclusiveStartKey=page["LastEvaluatedKey"],
                )
            else:
                break

    return True
