"""
Dataset Versions endpoint Lambda function tests.
"""
from http import HTTPStatus
from json import dumps
from os import environ
from unittest.mock import MagicMock, patch

from pytest import mark

from backend.api_keys import STATUS_KEY, SUCCESS_KEY
from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from backend.aws_keys import AWS_DEFAULT_REGION_KEY
from backend.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from backend.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    ERRORS_KEY,
    ERROR_CHECK_KEY,
    ERROR_DETAILS_KEY,
    ERROR_RESULT_KEY,
    ERROR_URL_KEY,
    EXECUTION_ARN_KEY,
    METADATA_UPLOAD_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)
from backend.validation_results_model import ValidationResult

from .aws_profile_utils import any_region_name
from .aws_utils import ValidationItem, any_arn_formatted_string, any_lambda_context, any_s3_url
from .stac_generators import any_dataset_id, any_dataset_version_id

with patch.dict(
    environ, {AWS_DEFAULT_REGION_KEY: environ.get(AWS_DEFAULT_REGION_KEY, any_region_name())}
):
    from backend.import_status import entrypoint
    from backend.step_function import Outcome


@mark.infrastructure
@patch("backend.step_function.STEP_FUNCTIONS_CLIENT.describe_execution")
def should_retrieve_validation_failures(describe_step_function_mock: MagicMock) -> None:
    # Given

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    describe_step_function_mock.return_value = {
        "status": "SUCCEEDED",
        "input": dumps({DATASET_ID_KEY: dataset_id, VERSION_ID_KEY: version_id}),
        "output": dumps({VALIDATION_KEY: {SUCCESS_KEY: False}}),
    }

    url = any_s3_url()
    error_details = {"error_message": "test"}
    check = "example"

    expected_response = {
        STATUS_CODE_KEY: HTTPStatus.OK,
        BODY_KEY: {
            STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
            VALIDATION_KEY: {
                STATUS_KEY: Outcome.FAILED.value,
                ERRORS_KEY: [
                    {
                        ERROR_CHECK_KEY: check,
                        ERROR_DETAILS_KEY: error_details,
                        ERROR_RESULT_KEY: ValidationResult.FAILED.value,
                        ERROR_URL_KEY: url,
                    }
                ],
            },
            METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
            ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        },
    }
    with ValidationItem(
        asset_id=(
            f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
        ),
        result=ValidationResult.FAILED,
        details=error_details,
        url=url,
        check=check,
    ):
        # When
        response = entrypoint.lambda_handler(
            {HTTP_METHOD_KEY: "GET", BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()}},
            any_lambda_context(),
        )

        # Then
        assert response == expected_response
