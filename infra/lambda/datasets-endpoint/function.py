"""
Dataset Lambda handler function.
"""

import uuid
from datetime import datetime

import boto3
from dateutil.tz import gettz
from jsonschema import ValidationError, validate

DYNAMODB = boto3.client("dynamodb")

DS_PRIMARY_KEYS = ("id", "type")
DS_ATTRIBUTES = ("title", "owning_group")
DS_ATTRIBUTES_EXT = ("created_at",)

REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "httpMethod": {"type": "string", "enum": ["GET", "POST", "PATCH", "DELETE"]},
        "body": {"type": "object"},
    },
    "required": ["httpMethod", "body"],
}

# TODO: implement GET response paging
# TODO: implement GET request filtering by title and owning_group
# TODO: allow Dataset delete only if no Dataset Version exists
# TODO: don't assume that all Dataset attributes are strings ("S")


def lambda_handler(event, context):  # pylint:disable=unused-argument,inconsistent-return-statements
    """Main Lambda entry point."""

    # request validation
    try:
        validate(event, REQUEST_SCHEMA)
        method = event["httpMethod"]
    except ValidationError as err:
        return {"statusCode": 400, "body": {"message": f"Bad Request: {err.message}."}}

    if method == "POST":
        return post_method(event)

    if method == "GET" and "id" in event["body"] and "type" in event["body"]:
        return get_method_single(event)

    if method == "GET" and event["body"] == {}:
        return get_method_all()

    if method == "PATCH":
        return patch_method(event)

    if method == "DELETE":
        return delete_method(event)


def post_method(payload):
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
    try:
        req_body = payload["body"]
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return {"statusCode": 400, "body": {"message": f"Bad Request: {err.message}."}}

    # get PKs
    pk = {}
    pk["id"] = uuid.uuid4().hex[:6]
    pk["type"] = payload["body"]["type"]

    # get attributes
    attr = {}
    for a in DS_ATTRIBUTES:
        attr[a] = payload["body"][a]

    curr_time = datetime.utcnow()
    attr["created_at"] = str(curr_time.replace(tzinfo=gettz("UTC")))

    # make sure that requested type/title doesn't already exist in DB
    db_resp = DYNAMODB.query(
        TableName="datasets",
        IndexName="datasets_title",
        ExpressionAttributeNames={
            "#type": "sk",
            "#title": "title",
        },
        ExpressionAttributeValues={
            ":type": {"S": f"TYPE#{pk['type']}"},
            ":title": {"S": f"{attr['title']}"},
        },
        KeyConditionExpression="#title = :title AND #type = :type",
        Select="COUNT",
    )
    if int(db_resp["Count"]) > 0:
        return {
            "statusCode": 409,
            "body": {
                "message": f"Conflict: dataset '{attr['title']}' of type '{pk['type']}' already exists."
            },
        }

    # create Dataset record in DB
    while True:
        try:
            item_attr = {}
            for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
                item_attr[a] = {"S": attr[a]}

            db_resp = DYNAMODB.put_item(
                TableName="datasets",
                Item={
                    "pk": {"S": f"DATASET#{pk['id']}"},
                    "sk": {"S": f"TYPE#{pk['type']}"},
                    **item_attr,
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            # TODO: check if DB request was successful
            break
        except DYNAMODB.exceptions.ConditionalCheckFailedException:
            pass  # try once again with different generated id

    resp_body = {}
    resp_body["id"] = pk["id"]
    resp_body["type"] = pk["type"]

    for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
        resp_body[a] = attr[a]

    return {"statusCode": 201, "body": resp_body}


def get_method_single(payload):
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
    try:
        req_body = payload["body"]
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return {"statusCode": 400, "body": {"message": f"Bad Request: {err.message}."}}

    # get PKs
    pk = {}
    for k in DS_PRIMARY_KEYS:
        if k in payload["body"]:
            pk[k] = payload["body"][k]

    # single dataset query (if id and type specified)
    db_resp = DYNAMODB.query(
        TableName="datasets",
        ExpressionAttributeNames={
            "#id": "pk",
            "#type": "sk",
        },
        ExpressionAttributeValues={
            ":id": {"S": f"DATASET#{pk['id']}"},
            ":type": {"S": f"TYPE#{pk['type']}"},
        },
        KeyConditionExpression="#id = :id AND #type = :type",
        Select="ALL_ATTRIBUTES",
        ConsistentRead=True,
    )

    if len(db_resp["Items"]) > 0:  # pylint:disable=no-else-return
        resp_body = {}
        resp_body["id"] = pk["id"]
        resp_body["type"] = pk["type"]

        for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
            resp_body[a] = list(db_resp["Items"][0][a].values())[0]

        return {"statusCode": 200, "body": resp_body}
    else:
        return {
            "statusCode": 404,
            "body": {
                "message": f"Not Found: dataset '{pk['id']}' of type '{pk['type']}' does not exist."
            },
        }


def get_method_all():
    """GET: Get all Datasets."""

    # multiple datasets query
    db_resp = DYNAMODB.scan(
        TableName="datasets",
        ExpressionAttributeNames={
            "#id": "pk",
            "#type": "sk",
        },
        ExpressionAttributeValues={
            ":id": {"S": "DATASET#"},
            ":type": {"S": "TYPE#"},
        },
        FilterExpression="begins_with(#id, :id) and begins_with(#type, :type)",
        Select="ALL_ATTRIBUTES",
    )

    resp_body = []
    for i in db_resp["Items"]:
        item = {}
        item["id"] = list(i["pk"].values())[0].split("#")[1]
        item["type"] = list(i["sk"].values())[0].split("#")[1]

        for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
            item[a] = list(i[a].values())[0]

        resp_body.append(item)

    return {"statusCode": 200, "body": resp_body}


def patch_method(payload):
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
    try:
        req_body = payload["body"]
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return {"statusCode": 400, "body": {"message": f"Bad Request: {err.message}."}}

    # get PKs
    pk = {}
    pk["id"] = payload["body"]["id"]
    pk["type"] = payload["body"]["type"]

    # get attributes
    attr = {}
    for a in DS_ATTRIBUTES:
        if a in payload["body"].keys():
            attr[a] = payload["body"][a]

    # make sure that requested type/title doesn't already exist in DB
    db_resp = DYNAMODB.query(
        TableName="datasets",
        IndexName="datasets_title",
        ExpressionAttributeNames={
            "#type": "sk",
            "#title": "title",
        },
        ExpressionAttributeValues={
            ":type": {"S": f"TYPE#{pk['type']}"},
            ":title": {"S": f"{attr['title']}"},
        },
        KeyConditionExpression="#title = :title AND #type = :type",
        Select="COUNT",
    )
    if int(db_resp["Count"]) > 0:
        return {
            "statusCode": 409,
            "body": {
                "message": f"Conflict: dataset '{attr['title']}' of type '{pk['type']}' already exists."
            },
        }

    # update Dataset record in DB
    expression_attribute_names = {}
    for a in attr:
        expression_attribute_names[f"#{a}"] = a

    expression_attribute_values = {}
    for a in attr:
        expression_attribute_values[f":{a}"] = {"S": attr[a]}

    update_expression = "SET "
    update_expression += ", ".join([f"#{a} = :{a}" for a in attr])

    try:
        db_resp = DYNAMODB.update_item(
            TableName="datasets",
            Key={
                "pk": {"S": f"DATASET#{pk['id']}"},
                "sk": {"S": f"TYPE#{pk['type']}"},
            },
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            UpdateExpression=update_expression,
            ConditionExpression="attribute_exists(pk)",
            ReturnValues="ALL_NEW",
        )
        # TODO: check if DB request was successful
    except DYNAMODB.exceptions.ConditionalCheckFailedException:
        return {
            "statusCode": 404,
            "body": {
                "message": f"Not Found: dataset '{pk['id']}' of type '{pk['type']}' does not exist."
            },
        }

    resp_body = {}
    resp_body["id"] = pk["id"]
    resp_body["type"] = pk["type"]

    for a in DS_ATTRIBUTES + DS_ATTRIBUTES_EXT:
        resp_body[a] = list(db_resp["Attributes"][a].values())[0]

    return {"statusCode": 200, "body": resp_body}


def delete_method(payload):
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
    try:
        req_body = payload["body"]
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return {"statusCode": 400, "body": {"message": f"Bad Request: {err.message}."}}

    # get PKs
    pk = {}
    pk["id"] = payload["body"]["id"]
    pk["type"] = payload["body"]["type"]

    # delete Dataset record in DB
    try:
        db_resp = DYNAMODB.delete_item(  # pylint:disable=unused-variable
            TableName="datasets",
            Key={
                "pk": {"S": f"DATASET#{pk['id']}"},
                "sk": {"S": f"TYPE#{pk['type']}"},
            },
            ConditionExpression="attribute_exists(pk)",
            ReturnValues="NONE",
        )
        # TODO: check if DB request was successful
    except DYNAMODB.exceptions.ConditionalCheckFailedException:
        return {
            "statusCode": 404,
            "body": {
                "message": f"Not Found: dataset '{pk['id']}' of type '{pk['type']}' does not exist."
            },
        }

    resp_body = {}

    return {"statusCode": 204, "body": resp_body}
