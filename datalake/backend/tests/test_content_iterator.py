from copy import deepcopy
from typing import Any, Dict

from jsonschema import ValidationError  # type: ignore[import]
from pytest import raises
from pytest_subtests import SubTests  # type: ignore[import]

from ..processing.content_iterator.task import lambda_handler
from .utils import (
    any_dataset_id,
    any_dataset_version_id,
    any_dictionary_key,
    any_lambda_context,
    any_next_item,
)

VALID_EVENT: Dict[str, Any] = {
    "content": {
        "dataset_id": any_dataset_id(),
        "version_id": any_dataset_version_id(),
        "next_item": any_next_item(),
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


def test_should_raise_exception_if_event_is_missing_dataset_id(subtests: SubTests) -> None:
    for property_name in ["dataset_id", "version_id", "next_item"]:
        event = deepcopy(VALID_EVENT)
        del event["content"][property_name]
        expected_message = f"'{property_name}' is a required property"
        with subtests.test(msg=property_name), raises(ValidationError, match=expected_message):
            lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_next_item_is_negative() -> None:
    event = deepcopy(VALID_EVENT)
    event["content"]["next_item"] = -any_next_item()
    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())
