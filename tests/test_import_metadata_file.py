from io import StringIO
from json import dumps
from unittest.mock import MagicMock, patch
from urllib.parse import quote

from geostore.import_dataset_file import (
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    RESULTS_KEY,
    RESULT_CODE_KEY,
    RESULT_CODE_PERMANENT_FAILURE,
    RESULT_CODE_SUCCEEDED,
    RESULT_STRING_KEY,
    S3_BUCKET_ARN_KEY,
    S3_KEY_KEY,
    TASKS_KEY,
    TASK_ID_KEY,
    TREAT_MISSING_KEYS_AS_KEY,
)
from geostore.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY
from geostore.import_metadata_file.task import S3_BODY_KEY, lambda_handler
from geostore.stac_format import STAC_ASSETS_KEY, STAC_HREF_KEY
from geostore.step_function_keys import S3_ROLE_ARN_KEY

from .aws_utils import (
    any_invocation_id,
    any_invocation_schema_version,
    any_lambda_context,
    any_role_arn,
    any_s3_bucket_arn,
    any_s3_bucket_name,
    any_s3_url,
    any_task_id,
)
from .general_generators import any_safe_file_path
from .stac_generators import any_asset_name


@patch("geostore.import_dataset_file.get_s3_client_for_role")
@patch("geostore.import_metadata_file.task.TARGET_S3_CLIENT")
def should_return_success_response(
    target_s3_client_mock: MagicMock, get_s3_client_for_role_mock: MagicMock
) -> None:
    # Given
    task_id = any_task_id()
    invocation_id = any_invocation_id()
    invocation_schema_version = any_invocation_schema_version()
    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            NEW_KEY_KEY: any_safe_file_path(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            S3_ROLE_ARN_KEY: any_role_arn(),
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                        }
                    )
                ),
                TASK_ID_KEY: task_id,
            }
        ],
        INVOCATION_ID_KEY: invocation_id,
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
    }
    stac_object = {STAC_ASSETS_KEY: {any_asset_name(): {STAC_HREF_KEY: any_s3_url()}}}
    get_s3_client_for_role_mock.return_value.get_object.return_value = {
        S3_BODY_KEY: StringIO(initial_value=dumps(stac_object))
    }
    return_value = "any return value"
    target_s3_client_mock.put_object.return_value = return_value

    # When
    response = lambda_handler(event, any_lambda_context())

    # Then
    assert response == {
        INVOCATION_ID_KEY: invocation_id,
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
        RESULTS_KEY: [
            {
                TASK_ID_KEY: task_id,
                RESULT_CODE_KEY: RESULT_CODE_SUCCEEDED,
                RESULT_STRING_KEY: return_value,
            }
        ],
        TREAT_MISSING_KEYS_AS_KEY: RESULT_CODE_PERMANENT_FAILURE,
    }
