"""
Dataset endpoint Lambda function.
"""

from jsonschema import ValidationError, validate

from ..utils import error_response
from .create import create_dataset
from .delete import delete_dataset
from .get import get_dataset_filter, get_dataset_single
from .list import list_datasets
from .update import update_dataset

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
        return handle_get(event)

    if method == "PATCH":
        return update_dataset(event)

    if method == "DELETE":
        return delete_dataset(event)


def handle_get(event):
    if "id" in event["body"] and "type" in event["body"]:
        return get_dataset_single(event)

    if "title" in event["body"] or "owning_group" in event["body"]:
        return get_dataset_filter(event)

    if event["body"] == {}:
        return list_datasets()

    return error_response(400, "Unhandled request")
