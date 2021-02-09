from copy import deepcopy
from typing import Any, Dict

from jsonschema import ValidationError  # type: ignore[import]
from pytest import raises

from ..processing.content_iterator.task import lambda_handler
from .utils import any_dataset_id, any_dictionary_key, any_lambda_context

VALID_EVENT: Dict[str, Any] = {
    "content": {
        "dataset_id": any_dataset_id(),
    }
}


def test_should_raise_exception_if_event_is_missing_content() -> None:
    event = deepcopy(VALID_EVENT)
    del event["content"]
    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_event_has_unknown_top_level_property() -> None:
    event = deepcopy(VALID_EVENT)
    event[any_dictionary_key()] = 1
    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_event_is_missing_dataset_id() -> None:
    event = deepcopy(VALID_EVENT)
    del event["content"]["dataset_id"]
    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())
