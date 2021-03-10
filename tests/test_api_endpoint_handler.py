from typing import Callable, MutableMapping
from unittest.mock import MagicMock

from pytest_subtests import SubTests  # type: ignore[import]

from backend.api_responses import JsonObject, handle_request


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


def should_call_relevant_http_method(subtests: SubTests) -> None:
    post_mock = MagicMock()

    get_mock = MagicMock()
    get_mock.return_value = expected_response = "Some Response"

    request_handlers: MutableMapping[str, Callable[[JsonObject], JsonObject]] = {
        "POST": post_mock,
        "GET": get_mock,
    }

    response = handle_request({"httpMethod": "GET", "body": {}}, request_handlers)

    with subtests.test("Should return response"):
        assert response == expected_response
    with subtests.test("Should call GET method"):
        assert get_mock.called
    with subtests.test("Should not call POST method"):
        assert not post_mock.called
