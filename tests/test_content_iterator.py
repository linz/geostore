from copy import deepcopy
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pytest import mark, raises
from pytest_subtests import SubTests

from geostore.content_iterator.task import (
    ASSETS_TABLE_NAME_KEY,
    CONTENT_KEY,
    FIRST_ITEM_KEY,
    ITERATION_SIZE_KEY,
    MAX_ITERATION_SIZE,
    NEXT_ITEM_KEY,
    RESULTS_TABLE_NAME_KEY,
    lambda_handler,
)
from geostore.models import DB_KEY_SEPARATOR
from geostore.processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from geostore.step_function import get_hash_key
from geostore.step_function_keys import (
    DATASET_ID_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    S3_ROLE_ARN_KEY,
)

from .aws_utils import (
    any_item_count,
    any_lambda_context,
    any_next_item_index,
    any_role_arn,
    any_s3_url,
    any_table_name,
)
from .general_generators import any_dictionary_key, any_safe_filename
from .stac_generators import any_dataset_id, any_dataset_version_id, any_hex_multihash

INITIAL_EVENT: Dict[str, Any] = {
    DATASET_ID_KEY: any_dataset_id(),
    METADATA_URL_KEY: any_s3_url(),
    S3_ROLE_ARN_KEY: any_role_arn(),
    NEW_VERSION_ID_KEY: any_dataset_version_id(),
}

SUBSEQUENT_EVENT: Dict[str, Any] = {
    CONTENT_KEY: {
        FIRST_ITEM_KEY: str(any_next_item_index()),
        ITERATION_SIZE_KEY: MAX_ITERATION_SIZE,
        NEXT_ITEM_KEY: any_next_item_index(),
    },
    DATASET_ID_KEY: any_dataset_id(),
    METADATA_URL_KEY: any_s3_url(),
    S3_ROLE_ARN_KEY: any_role_arn(),
    NEW_VERSION_ID_KEY: any_dataset_version_id(),
}


def should_raise_exception_if_event_is_missing_required_property(
    subtests: SubTests,
) -> None:
    for property_name in [DATASET_ID_KEY, METADATA_URL_KEY, NEW_VERSION_ID_KEY]:
        event = deepcopy(INITIAL_EVENT)
        del event[property_name]
        expected_message = f"'{property_name}' is a required property"

        with subtests.test(msg=property_name), raises(ValidationError, match=expected_message):
            lambda_handler(event, any_lambda_context())


def should_raise_exception_if_content_is_missing_any_property(subtests: SubTests) -> None:
    for property_name in [FIRST_ITEM_KEY, ITERATION_SIZE_KEY, NEXT_ITEM_KEY]:
        event = deepcopy(SUBSEQUENT_EVENT)
        del event[CONTENT_KEY][property_name]
        expected_message = f"'{property_name}' is a required property"

        with subtests.test(msg=property_name), raises(ValidationError, match=expected_message):
            lambda_handler(event, any_lambda_context())


def should_raise_exception_if_content_has_unknown_property() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][any_dictionary_key()] = 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_next_item_is_negative() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][NEXT_ITEM_KEY] = -1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_next_item_is_zero() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][NEXT_ITEM_KEY] = 0

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_iteration_size_is_not_positive() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][ITERATION_SIZE_KEY] = 0

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_iteration_size_is_more_than_production_iteration_size() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][ITERATION_SIZE_KEY] = MAX_ITERATION_SIZE + 1

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_first_item_is_negative() -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][FIRST_ITEM_KEY] = "-1"

    with raises(ValidationError):
        lambda_handler(event, any_lambda_context())


def should_raise_exception_if_first_item_is_not_a_multiple_of_iteration_size() -> None:
    """Assumes iteration size is not 1"""
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][FIRST_ITEM_KEY] = str(MAX_ITERATION_SIZE - 1)

    with raises(AssertionError):
        lambda_handler(event, any_lambda_context())


@patch("geostore.content_iterator.task.processing_assets_model_with_meta")
def should_return_zero_as_first_item_if_no_content(
    processing_assets_model_mock: MagicMock,
) -> None:
    event = deepcopy(INITIAL_EVENT)
    processing_assets_model_mock.return_value.count.return_value = any_item_count()

    response = lambda_handler(event, any_lambda_context())

    assert response[FIRST_ITEM_KEY] == "0", response


@patch("geostore.content_iterator.task.processing_assets_model_with_meta")
def should_return_next_item_as_first_item(processing_assets_model_mock: MagicMock) -> None:
    event = deepcopy(SUBSEQUENT_EVENT)
    next_item_index = any_next_item_index()
    event[CONTENT_KEY][NEXT_ITEM_KEY] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = any_item_count()

    response = lambda_handler(event, any_lambda_context())

    assert response[FIRST_ITEM_KEY] == str(next_item_index), response


@patch("geostore.content_iterator.task.processing_assets_model_with_meta")
@patch("geostore.content_iterator.task.get_param")
def should_return_minus_one_next_item_if_remaining_item_count_is_less_than_iteration_size(
    get_param_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
) -> None:
    assets_table_name = any_table_name()
    results_table_name = any_table_name()
    get_param_mock.side_effect = [assets_table_name, results_table_name]

    remaining_item_count = MAX_ITERATION_SIZE - 1
    next_item_index = any_next_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][NEXT_ITEM_KEY] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = (
        next_item_index + remaining_item_count
    )
    expected_response = {
        FIRST_ITEM_KEY: str(next_item_index),
        ITERATION_SIZE_KEY: remaining_item_count,
        NEXT_ITEM_KEY: -1,
        ASSETS_TABLE_NAME_KEY: assets_table_name,
        RESULTS_TABLE_NAME_KEY: results_table_name,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@patch("geostore.content_iterator.task.processing_assets_model_with_meta")
@patch("geostore.content_iterator.task.get_param")
def should_return_minus_one_next_item_if_remaining_item_count_matches_iteration_size(
    get_param_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
) -> None:
    assets_table_name = any_table_name()
    results_table_name = any_table_name()
    get_param_mock.side_effect = [assets_table_name, results_table_name]

    remaining_item_count = MAX_ITERATION_SIZE
    next_item_index = any_next_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][NEXT_ITEM_KEY] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = (
        next_item_index + remaining_item_count
    )
    expected_response = {
        FIRST_ITEM_KEY: str(next_item_index),
        ITERATION_SIZE_KEY: MAX_ITERATION_SIZE,
        NEXT_ITEM_KEY: -1,
        ASSETS_TABLE_NAME_KEY: assets_table_name,
        RESULTS_TABLE_NAME_KEY: results_table_name,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@patch("geostore.content_iterator.task.processing_assets_model_with_meta")
@patch("geostore.content_iterator.task.get_param")
def should_return_content_when_remaining_item_count_is_more_than_iteration_size(
    get_param_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
) -> None:
    assets_table_name = any_table_name()
    results_table_name = any_table_name()
    get_param_mock.side_effect = [assets_table_name, results_table_name]

    remaining_item_count = MAX_ITERATION_SIZE + 1
    next_item_index = any_next_item_index()
    event = deepcopy(SUBSEQUENT_EVENT)
    event[CONTENT_KEY][NEXT_ITEM_KEY] = next_item_index
    processing_assets_model_mock.return_value.count.return_value = (
        next_item_index + remaining_item_count
    )
    expected_response = {
        FIRST_ITEM_KEY: str(next_item_index),
        ITERATION_SIZE_KEY: MAX_ITERATION_SIZE,
        NEXT_ITEM_KEY: next_item_index + MAX_ITERATION_SIZE,
        ASSETS_TABLE_NAME_KEY: assets_table_name,
        RESULTS_TABLE_NAME_KEY: results_table_name,
    }

    response = lambda_handler(event, any_lambda_context())

    assert response == expected_response, response


@mark.infrastructure
def should_count_only_asset_files() -> None:
    # Given a single metadata and asset entry in the database
    event = deepcopy(INITIAL_EVENT)
    hash_key = get_hash_key(event[DATASET_ID_KEY], event[NEW_VERSION_ID_KEY])
    processing_assets_model = processing_assets_model_with_meta()
    processing_assets_model(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}0",
        url=any_s3_url(),
        filename=any_safe_filename(),
    ).save()
    processing_assets_model(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
        url=any_s3_url(),
        filename=any_safe_filename(),
        multihash=any_hex_multihash(),
    ).save()

    # When running the Lambda handler
    response = lambda_handler(event, any_lambda_context())

    # Then the iteration size should be one
    assert response[ITERATION_SIZE_KEY] == 1
