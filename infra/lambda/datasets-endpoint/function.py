"""
Dataset Lambda handler function.
"""

import uuid
from datetime import datetime, timezone
from http.client import responses as http_responses

import boto3
from jsonschema import ValidationError, validate

TABLE_NAME = "datasets"
DS_PRIMARY_KEYS = ("id", "type")
DS_ATTRIBUTES = ("title", "owning_group")
DS_ATTRIBUTES_EXT = ("created_at",)

DYNAMODB = boto3.resource("dynamodb")

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

    # get PKs
    pk = {}
    pk["id"] = uuid.uuid1().hex
    pk["type"] = payload["body"]["type"]

    # get attributes
    attr = {a: payload["body"][a] for a in DS_ATTRIBUTES}
    attr["created_at"] = str(datetime.now(timezone.utc))

    table = DYNAMODB.Table(TABLE_NAME)

    # make sure that requested type/title doesn't already exist in DB
    db_resp = table.query(
        IndexName="datasets_title",
        ExpressionAttributeNames={
            "#type": "sk",
            "#title": "title",
        },
        ExpressionAttributeValues={
            ":type": f"TYPE#{pk['type']}",
            ":title": f"{attr['title']}",
        },
        KeyConditionExpression="#type = :type AND #title = :title",
        Select="COUNT",
        Limit=1,
    )
    if int(db_resp["Count"]) > 0:
        return error_response(
            409, f"dataset '{attr['title']}' of type '{pk['type']}' already exists"
        )

    # create Dataset record in DB
    item_attr = {a: attr[a] for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT}

    db_resp = table.put_item(
        Item={
            "pk": f"DATASET#{pk['id']}",
            "sk": f"TYPE#{pk['type']}",
            **item_attr,
        },
    )
    # TODO: check if DB request was successful

    resp_body = {}
    resp_body["id"] = pk["id"]
    resp_body["type"] = pk["type"]

    for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
        resp_body[a] = attr[a]

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

    # get PKs
    pk = {key: payload["body"][key] for key in DS_PRIMARY_KEYS if key in payload["body"]}

    table = DYNAMODB.Table(TABLE_NAME)

    # single dataset query (if id and type specified)
    db_resp = table.query(
        ExpressionAttributeNames={
            "#id": "pk",
            "#type": "sk",
        },
        ExpressionAttributeValues={
            ":id": f"DATASET#{pk['id']}",
            ":type": f"TYPE#{pk['type']}",
        },
        KeyConditionExpression="#id = :id AND #type = :type",
        Select="ALL_ATTRIBUTES",
        ConsistentRead=True,
    )

    if db_resp["Items"]:  # pylint:disable=no-else-return
        itemdict = db_resp["Items"][0]

        resp_body = {}
        resp_body["id"] = itemdict["pk"].split("#")[1]
        resp_body["type"] = itemdict["sk"].split("#")[1]

        for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
            resp_body[a] = itemdict[a]

        return success_response(200, resp_body)
    else:
        return error_response(404, f"dataset '{pk['id']}' of type '{pk['type']}' does not exist")


def get_dataset_filter(payload):  # pylint:disable=too-many-locals
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

    # get PKs
    pk = {key: payload["body"][key] for key in DS_PRIMARY_KEYS if key in payload["body"]}

    # get attributes
    attr = {a: payload["body"][a] for a in DS_ATTRIBUTES if a in payload["body"]}

    # dataset query by filter
    if "title" in attr:
        index_name = "datasets_title"

        expression_attribute_names = {"#title": "title"}
        expression_attribute_values = {":title": attr["title"]}
        key_condition_expression = "#type = :type AND #title = :title"

    if "owning_group" in attr:
        index_name = "datasets_owning_group"

        expression_attribute_names = {"#owning_group": "owning_group"}
        expression_attribute_values = {":owning_group": attr["owning_group"]}
        key_condition_expression = "#type = :type AND #owning_group = :owning_group"

    table = DYNAMODB.Table(TABLE_NAME)

    db_resp = table.query(
        IndexName=index_name,
        ExpressionAttributeNames={"#type": "sk", **expression_attribute_names},
        ExpressionAttributeValues={
            ":type": f"TYPE#{pk['type']}",
            **expression_attribute_values,
        },
        KeyConditionExpression=key_condition_expression,
        Select="ALL_ATTRIBUTES",
        ConsistentRead=False,
    )

    resp_body = []
    for item in db_resp["Items"]:

        resp_item = {}
        resp_item["id"] = item["pk"].split("#")[1]
        resp_item["type"] = item["sk"].split("#")[1]

        for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
            resp_item[a] = item[a]

        resp_body.append(resp_item)

    return success_response(200, resp_body)


def get_dataset_all():
    """GET: Get all Datasets."""

    table = DYNAMODB.Table(TABLE_NAME)

    # multiple datasets query
    db_resp = table.scan(
        ExpressionAttributeNames={
            "#id": "pk",
            "#type": "sk",
        },
        ExpressionAttributeValues={
            ":id": "DATASET#",
            ":type": "TYPE#",
        },
        FilterExpression="begins_with(#id, :id) and begins_with(#type, :type)",
        Select="ALL_ATTRIBUTES",
    )

    resp_body = []
    for item in db_resp["Items"]:

        resp_item = {}
        resp_item["id"] = item["pk"].split("#")[1]
        resp_item["type"] = item["sk"].split("#")[1]

        for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
            resp_item[a] = item[a]

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

    # get PKs
    pk = {}
    pk["id"] = payload["body"]["id"]
    pk["type"] = payload["body"]["type"]

    # get attributes
    attr = {a: payload["body"][a] for a in DS_ATTRIBUTES if a in payload["body"]}

    table = DYNAMODB.Table(TABLE_NAME)

    # make sure that requested type/title doesn't already exist in DB
    db_resp = table.query(
        IndexName="datasets_title",
        ExpressionAttributeNames={
            "#type": "sk",
            "#title": "title",
        },
        ExpressionAttributeValues={
            ":type": f"TYPE#{pk['type']}",
            ":title": f"{attr['title']}",
        },
        KeyConditionExpression="#title = :title AND #type = :type",
        Select="COUNT",
        ConsistentRead=False,
    )
    if int(db_resp["Count"]) > 0:
        return error_response(
            409, f"dataset '{attr['title']}' of type '{pk['type']}' already exists"
        )

    # update Dataset record in DB
    expression_attribute_names = {}
    for a in attr:
        expression_attribute_names[f"#{a}"] = a

    expression_attribute_values = {}
    for a in attr:
        expression_attribute_values[f":{a}"] = attr[a]

    update_expression = "SET "
    update_expression += ", ".join([f"#{a} = :{a}" for a in attr])

    try:
        db_resp = table.update_item(
            Key={
                "pk": f"DATASET#{pk['id']}",
                "sk": f"TYPE#{pk['type']}",
            },
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            UpdateExpression=update_expression,
            ConditionExpression="attribute_exists(pk)",
            ReturnValues="ALL_NEW",
        )
        # TODO: check if DB request was successful
    except DYNAMODB.meta.client.exceptions.ConditionalCheckFailedException:
        return error_response(404, f"dataset '{pk['id']}' of type '{pk['type']}' does not exist")

    resp_body = {}
    resp_body["id"] = pk["id"]
    resp_body["type"] = pk["type"]

    for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
        resp_body[a] = db_resp["Attributes"][a]

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

    # get PKs
    pk = {}
    pk["id"] = payload["body"]["id"]
    pk["type"] = payload["body"]["type"]

    table = DYNAMODB.Table(TABLE_NAME)

    # delete Dataset record in DB
    try:
        db_resp = table.delete_item(  # pylint:disable=unused-variable
            Key={
                "pk": f"DATASET#{pk['id']}",
                "sk": f"TYPE#{pk['type']}",
            },
            ConditionExpression="attribute_exists(pk)",
            ReturnValues="NONE",
        )
        # TODO: check if DB request was successful
    except DYNAMODB.meta.client.exceptions.ConditionalCheckFailedException:
        return error_response(404, f"dataset '{pk['id']}' of type '{pk['type']}' does not exist")

    resp_body = {}
    return success_response(204, resp_body)
