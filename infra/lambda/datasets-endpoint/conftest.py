"""
Pytest configuration file.
"""

import logging

import boto3
import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DYNAMODB = boto3.resource("dynamodb")


@pytest.fixture()
def db_prepare(table_name="datasets"):
    """
    Prepare DB with some dataset records and clean it up after test is
    finished.
    """

    logger.debug("Running DB Setup")

    DYNAMODB.batch_write_item(
        RequestItems={
            table_name: [
                {
                    "PutRequest": {
                        "Item": {
                            "pk": "DATASET#111abc",
                            "sk": "TYPE#RASTER",
                            "title": "Dataset ABC",
                            "owning_group": "A_ABC_XYZ",
                            "created_at": "2020-01-01 01:01:01.000000+00:00",
                            "updated_at": "2020-01-01 02:01:01.000000+00:00",
                        },
                    },
                },
                {
                    "PutRequest": {
                        "Item": {
                            "pk": "DATASET#222xyz",
                            "sk": "TYPE#RASTER",
                            "title": "Dataset XYZ",
                            "owning_group": "A_ABC_XYZ",
                            "created_at": "2020-02-01 01:01:01.000000+00:00",
                            "updated_at": "2020-02-01 02:01:01.000000+00:00",
                        },
                    },
                },
            ],
        },
    )

    yield  # teardown

    logger.debug("Running DB Teardown")

    table = DYNAMODB.Table(table_name)

    # get the table keys
    key_names = [key["AttributeName"] for key in table.key_schema]

    # only retrieve the keys for each item in the table (minimize data transfer)
    projection_expression = ", ".join(f"#{key}" for key in key_names)
    expression_attribute_names = {f"#{key}": key for key in key_names}

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
