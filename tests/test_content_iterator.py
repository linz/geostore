from copy import deepcopy
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark, raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.content_iterator.task import MAX_ITERATION_SIZE, lambda_handler
from backend.processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta

from .aws_utils import any_item_count, any_lambda_context, any_next_item_index, any_s3_url
from .general_generators import any_dictionary_key
from .stac_generators import any_dataset_id, any_dataset_version_id, any_hex_multihash

INITIAL_EVENT: Dict[str, Any] = {
    "dataset_id": any_dataset_id(),
    "metadata_url": any_s3_url(),
    "version_id": any_dataset_version_id(),
}

SUBSEQUENT_EVENT: Dict[str, Any] = {
    "content": {
        "first_item": str(any_next_item_index()),
        "iteration_size": MAX_ITERATION_SIZE,
        "next_item": any_next_item_index(),
    },
    "dataset_id": any_dataset_id(),
    "metadata_url": any_s3_url(),
    "version_id": any_dataset_version_id(),
}


def should_raise_exception_if_event_is_missing_state_machine_properties(
    subtests: SubTests,
) -> None:
    for property_name in ["dataset_id", "metadata_url", "version_id"]:
        event = deepcopy(INITIAL_EVENT)
        del event[property_name]
        expected_message = f"'{property_name}' is a required property"

        with subtests.test(msg=property_name), raises(ValidationError, match=expected_message):
            lambda_handler(event, any_lambda_context())


def should_raise_exception_if_event_has_unknown_top_level_property() -> None:
    event = deepcopy(INITIAL_EVENT)
    event[any_dictionary_key()] = 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_content_is_missing_any_property(subtests: SubTests) -> None:
    for property_name in ["first_item", "iteration_size", "next_item"]:
        event = deepcopy(SUBSEQUENT_EVENT)
        del event["content"][property_name]
        expected_message = f"'{property_name}' is a required property"

        with subtests.test(msg=property_name), raises(ValidationError, match=expected_message):
            lambda_handler(event, any_lambda_context())


def should_raise_exception_if_content_has_unknown_property() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"][any_dictionary_key()] = 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_next_item_is_negative() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = -1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_next_item_is_zero() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = 0

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_iteration_size_is_not_positive() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["iteration_size"] = 0

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_iteration_size_is_more_than_production_iteration_size() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["iteration_size"] = MAX_ITERATION_SIZE + 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_first_item_is_negative() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["first_item"] = "-1"

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_first_item_is_not_a_multiple_of_iteration_size() -> None:
    """Assumes iteration size is not 1"""
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["first_item"] = str(MAX_ITERATION_SIZE - 1)

    with raises(AssertionError):
        lambda_handler(event, any_lambda_context())


@patch("backend.content_iterator.task.processing_assets_model_with_meta")
def should_return_zero_as_first_item_if_no_content(
    processing_assets_model_mock: MagicMock,
) -> None:
    event = deepcopy(INITIAL_EVENT)
    processing_assets_model_mock.return_value.count.return_value = any_item_count()

    response = lambda_handler(event, any_lambda_context())

    assert response["first_item"] == "0", response


@patch("backend.content_iterator.task.processing_assets_model_with_meta")
def should_return_next_item_as_first_item(processing_assets_model_mock: MagicMock) -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    next_item_index = any_next_item_index()
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = any_item_count()

    response = lambda_handler(event, any_lambda_context())

    assert response["first_item"] == str(next_item_index), response


@patch("backend.content_iterator.task.processing_assets_model_with_meta")
def should_return_minus_one_next_item_if_remaining_item_count_is_less_than_iteration_size(
    processing_assets_model_mock: MagicMock,
) -> None:
    remaining_item_count = MAX_ITERATION_SIZE - 1
    next_item_index = any_next_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = (
        next_item_index + remaining_item_count
    )
    expected_response = {
        "first_item": str(next_item_index),
        "iteration_size": remaining_item_count,
        "next_item": -1,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@patch("backend.content_iterator.task.processing_assets_model_with_meta")
def should_return_minus_one_next_item_if_remaining_item_count_matches_iteration_size(
    processing_assets_model_mock: MagicMock,
) -> None:
    remaining_item_count = MAX_ITERATION_SIZE
    next_item_index = any_next_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = (
        next_item_index + remaining_item_count
    )
    expected_response = {
        "first_item": str(next_item_index),
        "iteration_size": MAX_ITERATION_SIZE,
        "next_item": -1,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@patch("backend.content_iterator.task.processing_assets_model_with_meta")
def should_return_content_when_remaining_item_count_is_more_than_iteration_size(
    processing_assets_model_mock: MagicMock,
) -> None:
    remaining_item_count = MAX_ITERATION_SIZE + 1
    next_item_index = any_next_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event["content"]["next_item"] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = (
        next_item_index + remaining_item_count
    )
    expected_response = {
        "first_item": str(next_item_index),
        "iteration_size": MAX_ITERATION_SIZE,
        "next_item": next_item_index + MAX_ITERATION_SIZE,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@mark.infrastructure
def should_count_only_asset_files() -> None:
    # Given a single metadata and asset entry in the database
    event = deepcopy(INITIAL_EVENT)
    hash_key = f"DATASET#{event['dataset_id']}#VERSION#{event['version_id']}"
    processing_assets_model = processing_assets_model_with_meta()
    processing_assets_model(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.METADATA.value}#0",
        url=any_s3_url(),
    ).save()
    processing_assets_model(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.DATA.value}#0",
        url=any_s3_url(),
        multihash=any_hex_multihash(),
    ).save()

    # When running the Lambda handler
    response = lambda_handler(event, any_lambda_context())

    # Then the iteration size should be one
    assert response["iteration_size"] == 1
