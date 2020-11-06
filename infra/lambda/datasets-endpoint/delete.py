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
