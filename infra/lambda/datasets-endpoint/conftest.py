"""
Pytest configuration file.
"""

import boto3
import pytest

DYNAMODB = boto3.resource("dynamodb")


def pytest_configure():
    """Share Dataset ID and title values between tests."""
    return {"dataset_id": None, "dataset_type": None, "dataset_title": None}


@pytest.fixture()
def db_truncate(table_name="datasets"):
    """Truncate DynamoDB table."""

    table = DYNAMODB.Table(table_name)

    # get the table keys
    key_names = [key.get("AttributeName") for key in table.key_schema]

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
