"""
Dataset Versions endpoint Lambda function tests.
"""

from copy import deepcopy
from http import HTTPStatus
from logging import INFO, basicConfig, getLogger
from os import environ
from unittest.mock import patch

from pytest_subtests import SubTests

from backend.aws_keys import AWS_DEFAULT_REGION_KEY

from .aws_profile_utils import any_region_name

with patch.dict(environ, {AWS_DEFAULT_REGION_KEY: any_region_name()}, clear=True):
    from backend.api_keys import MESSAGE_KEY
    from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
    from backend.dataset_versions import entrypoint
    from backend.step_function_keys import DATASET_ID_SHORT_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY

    from .aws_utils import any_lambda_context, any_role_arn, any_s3_url
    from .stac_generators import any_dataset_id

basicConfig(level=INFO)
logger = getLogger(__name__)


def should_return_error_when_missing_required_property(subtests: SubTests) -> None:
    minimal_body = {
        DATASET_ID_SHORT_KEY: any_dataset_id(),
        METADATA_URL_KEY: any_s3_url(),
        S3_ROLE_ARN_KEY: any_role_arn(),
    }

    for key in minimal_body:
        with subtests.test(msg=key):
            # Given a missing property in the body
            body = deepcopy(minimal_body)
            body.pop(key)

            # When attempting to create the instance
            response = entrypoint.lambda_handler(
                {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
            )

            # Then the API should return an error message
            assert response == {
                STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
                BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{key}' is a required property"},
            }
