"""
Dataset endpoint Lambda function tests. Working Geostore AWS environment is
required (run '$ cdk deploy' before running tests).
"""
from http import HTTPStatus
from logging import INFO, basicConfig, getLogger
from os import environ
from unittest.mock import patch

from backend.aws_keys import AWS_DEFAULT_REGION_KEY

from .aws_profile_utils import any_region_name

with patch.dict(environ, {AWS_DEFAULT_REGION_KEY: any_region_name()}, clear=True):
    from backend.api_keys import MESSAGE_KEY
    from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
    from backend.datasets.entrypoint import lambda_handler
    from backend.datasets.get import get_dataset_filter, get_dataset_single, handle_get
    from backend.step_function_keys import DATASET_ID_SHORT_KEY, TITLE_KEY

    from .aws_utils import any_lambda_context
    from .general_generators import any_dictionary_key, random_string

basicConfig(level=INFO)
logger = getLogger(__name__)


def should_return_error_when_trying_to_handle_get_dataset_with_wrong_property() -> None:
    response = handle_get({any_dictionary_key(): random_string(1)})

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: "Bad Request: Unhandled request"},
    }


def should_return_error_when_trying_to_get_single_dataset_with_missing_property() -> None:
    response = get_dataset_single({})

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{DATASET_ID_SHORT_KEY}' is a required property"},
    }


def should_return_error_when_trying_to_get_datasets_with_missing_property() -> None:
    response = get_dataset_filter({})

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{TITLE_KEY}' is a required property"},
    }


def should_return_error_when_trying_to_update_dataset_with_missing_property() -> None:
    response = lambda_handler({HTTP_METHOD_KEY: "PATCH", BODY_KEY: {}}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{DATASET_ID_SHORT_KEY}' is a required property"},
    }


def should_return_error_when_trying_to_delete_dataset_with_missing_id() -> None:
    response = lambda_handler({HTTP_METHOD_KEY: "DELETE", BODY_KEY: {}}, any_lambda_context())

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{DATASET_ID_SHORT_KEY}' is a required property"},
    }
