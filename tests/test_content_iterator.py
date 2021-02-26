from copy import deepcopy
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.processing.content_iterator.task import MAX_ITERATION_SIZE, lambda_handler

from .utils import (
    any_dataset_id,
    any_dataset_version_id,
    any_dictionary_key,
    any_item_count,
    any_item_index,
    any_lambda_context,
    any_s3_url,
    any_valid_dataset_type,
)

INITIAL_EVENT: Dict[str, Any] = {
    "dataset_id": any_dataset_id(),
    "metadata_url": any_s3_url(),
    "type": any_valid_dataset_type(),
    "version_id": any_dataset_version_id(),
}

SUBSEQUENT_EVENT: Dict[str, Any] = {
    "content": {
        "first_item": str(any_item_index()),
        "iteration_size": MAX_ITERATION_SIZE,
        "next_item": any_item_index(),
    },
    "dataset_id": any_dataset_id(),
    "metadata_url": any_s3_url(),
    "type": any_valid_dataset_type(),
    "version_id": any_dataset_version_id(),
}


def test_should_raise_exception_if_event_is_missing_state_machine_properties(
    subtests: SubTests,
) -> None:
    for property_name in ["dataset_id", "metadata_url", "type", "version_id"]:
        event = deepcopy(INITIAL_EVENT)
        del event[property_name]
        expected_message = f"'{property_name}' is a required property"

        with subtests.test(msg=property_name), raises(ValidationError, match=expected_message):
            lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_event_has_unknown_top_level_property() -> None:
    event = deepcopy(INITIAL_EVENT)
    event[any_dictionary_key()] = 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_content_is_missing_any_property(subtests: SubTests) -> None:
    for property_name in ["first_item", "iteration_size", "next_item"]:
        event = deepcopy(SUBSEQUENT_EVENT)
        del event["content"][property_name]
        expected_message = f"'{property_name}' is a required property"

        with subtests.test(msg=property_name), raises(ValidationError, match=expected_message):
            lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_content_has_unknown_property() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"][any_dictionary_key()] = 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_next_item_is_negative() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = -1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_next_item_is_zero() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = 0

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_iteration_size_is_not_positive() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["iteration_size"] = 0

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_iteration_size_is_more_than_production_iteration_size() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["iteration_size"] = MAX_ITERATION_SIZE + 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_first_item_is_negative() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["first_item"] = "-1"

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def test_should_raise_exception_if_first_item_is_not_a_multiple_of_iteration_size() -> None:
    """Assumes iteration size is not 1"""
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["first_item"] = str(MAX_ITERATION_SIZE - 1)

    with raises(AssertionError):
        lambda_handler(event, any_lambda_context())


@patch("backend.processing.content_iterator.task.ProcessingAssetsModel")
def test_should_return_zero_as_first_item_if_no_content(
    processing_assets_model_mock: MagicMock,
) -> None:
    event = deepcopy(INITIAL_EVENT)
    processing_assets_model_mock.count.return_value = any_item_index()

    response = lambda_handler(event, any_lambda_context())

    assert response["first_item"] == "0", response


@patch("backend.processing.content_iterator.task.ProcessingAssetsModel")
def test_should_return_next_item_as_first_item(processing_assets_model_mock: MagicMock) -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    next_item_index = any_item_index()
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.count.return_value = any_item_count()

    response = lambda_handler(event, any_lambda_context())

    assert response["first_item"] == str(next_item_index), response


@patch("backend.processing.content_iterator.task.ProcessingAssetsModel")
def test_should_return_minus_one_next_item_if_remaining_item_count_is_less_than_iteration_size(
    processing_assets_model_mock: MagicMock,
) -> None:
    remaining_item_count = MAX_ITERATION_SIZE - 1
    next_item_index = any_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.count.return_value = next_item_index + remaining_item_count
    expected_response = {
        "first_item": str(next_item_index),
        "iteration_size": remaining_item_count,
        "next_item": -1,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@patch("backend.processing.content_iterator.task.ProcessingAssetsModel")
def test_should_return_minus_one_next_item_if_remaining_item_count_matches_iteration_size(
    processing_assets_model_mock: MagicMock,
) -> None:
    remaining_item_count = MAX_ITERATION_SIZE
    next_item_index = any_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.count.return_value = next_item_index + remaining_item_count
    expected_response = {
        "first_item": str(next_item_index),
        "iteration_size": MAX_ITERATION_SIZE,
        "next_item": -1,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@patch("backend.processing.content_iterator.task.ProcessingAssetsModel")
def test_should_return_content_when_remaining_item_count_is_more_than_iteration_size(
    processing_assets_model_mock: MagicMock,
) -> None:
    remaining_item_count = MAX_ITERATION_SIZE + 1
    next_item_index = any_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.count.return_value = next_item_index + remaining_item_count
    expected_response = {
        "first_item": str(next_item_index),
        "iteration_size": MAX_ITERATION_SIZE,
        "next_item": next_item_index + MAX_ITERATION_SIZE,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response
