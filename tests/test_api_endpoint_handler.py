from unittest.mock import MagicMock

from backend.api_responses import handle_request


def should_return_required_property_error_when_missing_http_method() -> None:

    response = handle_request({"body": {}}, MagicMock())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'httpMethod' is a required property"},
    }


def should_return_required_property_error_when_missing_body() -> None:
    response = handle_request({"httpMethod": "GET"}, MagicMock())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'body' is a required property"},
    }
