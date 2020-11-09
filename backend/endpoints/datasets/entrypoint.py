"""
Dataset endpoint Lambda function.
"""

from endpoints.datasets.create import create_dataset
from endpoints.datasets.delete import delete_dataset
from endpoints.datasets.get import get_dataset_filter, get_dataset_single
from endpoints.datasets.list import list_datasets
from endpoints.datasets.update import update_dataset
from endpoints.utils import error_response
from jsonschema import ValidationError, validate

REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "httpMethod": {"type": "string", "enum": ["GET", "POST", "PATCH", "DELETE"]},
        "body": {"type": "object"},
    },
    "required": ["httpMethod", "body"],
}

# TODO: implement GET response paging
# TODO: allow Dataset delete only if no Dataset Version exists


def lambda_handler(  # pylint:disable=inconsistent-return-statements,too-many-return-statements
    event, _context
):
    """Main Lambda entry point."""

    # request validation
    try:
        validate(event, REQUEST_SCHEMA)
        method = event["httpMethod"]
    except ValidationError as err:
        return error_response(400, err.message)

    if method == "POST":
        return create_dataset(event)

    if method == "GET":
        if "id" in event["body"] and "type" in event["body"]:
            return get_dataset_single(event)

        if "title" in event["body"] or "owning_group" in event["body"]:
            return get_dataset_filter(event)

        if event["body"] == {}:
            return list_datasets()

    if method == "PATCH":
        return update_dataset(event)

    if method == "DELETE":
        return delete_dataset(event)
