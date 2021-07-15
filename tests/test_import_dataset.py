from copy import deepcopy
from os import environ
from unittest.mock import patch

from pytest_subtests import SubTests

from backend.aws_keys import AWS_DEFAULT_REGION_KEY
from backend.error_response_keys import ERROR_MESSAGE_KEY
from backend.step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)

from .aws_profile_utils import any_region_name
from .aws_utils import any_lambda_context, any_role_arn, any_s3_url
from .stac_generators import any_dataset_id, any_dataset_prefix, any_dataset_version_id

with patch.dict(environ, {AWS_DEFAULT_REGION_KEY: any_region_name()}, clear=True):
    from backend.import_dataset.task import lambda_handler


def should_return_error_when_missing_required_property(subtests: SubTests) -> None:
    # Given
    minimal_body = {
        DATASET_ID_KEY: any_dataset_id(),
        DATASET_PREFIX_KEY: any_dataset_prefix(),
        METADATA_URL_KEY: any_s3_url(),
        S3_ROLE_ARN_KEY: any_role_arn(),
        VERSION_ID_KEY: any_dataset_version_id(),
    }

    # When
    for key in minimal_body:
        with subtests.test(msg=key):
            # Given a missing property in the body
            body = deepcopy(minimal_body)
            body.pop(key)

            response = lambda_handler(body, any_lambda_context())

            assert response == {ERROR_MESSAGE_KEY: f"'{key}' is a required property"}
