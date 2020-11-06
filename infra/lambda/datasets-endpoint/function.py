"""
Dataset endpoint Lambda function.
"""

from http.client import responses as http_responses

from datasets_model import DatasetModel
from jsonschema import ValidationError, validate
from pynamodb.exceptions import DoesNotExist

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


def error_response(code, message):
    """Return error response content as string."""

    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}."}}


def success_response(code, body):
    """Return success response content as string."""

    return {"statusCode": code, "body": body}


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
            return get_dataset_all()

    if method == "PATCH":
        return update_dataset(event)

    if method == "DELETE":
        return delete_dataset(event)


def create_dataset(payload):
    """POST: Create Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
            },
            "title": {"type": "string"},
            "owning_group": {"type": "string"},
        },
        "required": ["type", "title", "owning_group"],
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # check for duplicate type/title
    if DatasetModel.datasets_tile_idx.count(
        hash_key=f"TYPE#{req_body['type']}",
        range_key_condition=(DatasetModel.title == f"{req_body['title']}"),
    ):
        return error_response(
            409, f"dataset '{req_body['title']}' of type '{req_body['type']}' already exists"
        )

    # create dataset
    dataset = DatasetModel(
        type=f"TYPE#{req_body['type']}",
        title=req_body["title"],
        owning_group=req_body["owning_group"],
    )
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = {}
    resp_body = dict(dataset)

    resp_body["id"] = dataset.dataset_id
    resp_body["type"] = dataset.dataset_type

    return success_response(201, resp_body)


def get_dataset_single(payload):
    """GET: Get single Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
            },
        },
        "required": ["id", "type"],
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # get dataset
    try:
        dataset = DatasetModel.get(
            hash_key=f"DATASET#{req_body['id']}",
            range_key=f"TYPE#{req_body['type']}",
            consistent_read=True,
        )
    except DoesNotExist:
        return error_response(
            404, f"dataset '{req_body['id']}' of type '{req_body['type']}' does not exist"
        )

    # return response
    resp_body = {}
    resp_body = dict(dataset)

    resp_body["id"] = dataset.dataset_id
    resp_body["type"] = dataset.dataset_type

    return success_response(200, resp_body)


def get_dataset_filter(payload):
    """GET: Get Datasets by filter."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
            },
            "title": {"type": "string"},
            "owning_group": {"type": "string"},
        },
        "required": ["type"],
        "minProperties": 2,
        "maxProperties": 2,
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # dataset query by filter
    if "title" in req_body:
        datasets = DatasetModel.datasets_tile_idx.query(
            hash_key=f"TYPE#{req_body['type']}",
            range_key_condition=DatasetModel.title == f"{req_body['title']}",
        )

    if "owning_group" in req_body:
        datasets = DatasetModel.datasets_owning_group_idx.query(
            hash_key=f"TYPE#{req_body['type']}",
            range_key_condition=DatasetModel.owning_group == f"{req_body['owning_group']}",
        )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dict(dataset)
        resp_item["id"] = dataset.dataset_id
        resp_item["type"] = dataset.dataset_type
        resp_body.append(resp_item)

    return success_response(200, resp_body)


def get_dataset_all():
    """GET: Get all Datasets."""

    # get all datasets
    datasets = DatasetModel.scan(
        filter_condition=DatasetModel.id.startswith("DATASET#")
        & DatasetModel.type.startswith("TYPE#")
    )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dict(dataset)
        resp_item["id"] = dataset.dataset_id
        resp_item["type"] = dataset.dataset_type
        resp_body.append(resp_item)

    return success_response(200, resp_body)


def update_dataset(payload):
    """PATCH: Update Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
            },
            "title": {"type": "string"},
            "owning_group": {"type": "string"},
        },
        "required": [
            "id",
            "type",
        ],
        "minProperties": 3,
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # check for duplicate type/title
    if DatasetModel.datasets_tile_idx.count(
        hash_key=f"TYPE#{req_body['type']}",
        range_key_condition=(DatasetModel.title == f"{req_body['title']}"),
    ):
        return error_response(
            409, f"dataset '{req_body['title']}' of type '{req_body['type']}' already exists"
        )

    # get dataset to update
    try:
        dataset = DatasetModel.get(
            hash_key=f"DATASET#{req_body['id']}",
            range_key=f"TYPE#{req_body['type']}",
            consistent_read=True,
        )
    except DoesNotExist:
        return error_response(
            404, f"dataset '{req_body['id']}' of type '{req_body['type']}' does not exist"
        )

    # update dataset
    for attr in DatasetModel.get_attributes():
        if attr not in ("id", "type"):
            if attr in req_body:
                setattr(dataset, attr, req_body[attr])

    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = {}
    resp_body = dict(dataset)

    resp_body["id"] = dataset.dataset_id
    resp_body["type"] = dataset.dataset_type

    return success_response(200, resp_body)


def delete_dataset(payload):
    """DELETE: Delete Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
            },
        },
        "required": ["id", "type"],
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # get dataset to delete
    try:
        dataset = DatasetModel.get(
            hash_key=f"DATASET#{req_body['id']}",
            range_key=f"TYPE#{req_body['type']}",
            consistent_read=True,
        )
    except DoesNotExist:
        return error_response(
            404, f"dataset '{req_body['id']}' of type '{req_body['type']}' does not exist"
        )

    # delete dataset
    dataset.delete()

    resp_body = {}
    return success_response(204, resp_body)
