from http.client import responses as http_responses
from typing import Any, List, MutableMapping, Union

JsonList = List[Any]
JsonObject = MutableMapping[str, Any]


def error_response(code: int, message: str) -> JsonObject:
    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}"}}


def success_response(code: int, body: Union[JsonList, JsonObject]) -> JsonObject:
    return {"statusCode": code, "body": body}
