from copy import deepcopy
from hashlib import sha256
from http import HTTPStatus
from io import BytesIO
from json import loads
from os import environ
from re import MULTILINE, match
from unittest.mock import ANY, MagicMock, patch

from botocore.exceptions import NoCredentialsError, NoRegionError
from mypy_boto3_lambda.type_defs import InvocationResponseTypeDef
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests
from typer.testing import CliRunner

from geostore.aws_keys import AWS_DEFAULT_REGION_KEY, BODY_KEY, STATUS_CODE_KEY
from geostore.cli import app
from geostore.dataset_properties import DATASET_KEY_SEPARATOR
from geostore.environment import ENV_NAME_VARIABLE_NAME
from geostore.populate_catalog.task import CATALOG_FILENAME
from geostore.resources import Resource
from geostore.stac_format import STAC_ASSETS_KEY, STAC_FILE_CHECKSUM_KEY, STAC_HREF_KEY
from geostore.step_function import Outcome
from geostore.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_SHORT_KEY,
    ERRORS_KEY,
    ERROR_CHECK_KEY,
    ERROR_DETAILS_KEY,
    ERROR_RESULT_KEY,
    ERROR_URL_KEY,
    METADATA_UPLOAD_KEY,
    STATUS_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
)
from geostore.sts import get_account_number
from geostore.types import JsonObject
from geostore.validation_results_model import ValidationResult

from .aws_utils import (
    LAMBDA_EXECUTED_VERSION,
    Dataset,
    S3Object,
    any_arn_formatted_string,
    any_role_arn,
    any_s3_url,
    delete_s3_key,
    wait_for_s3_key,
)
from .file_utils import json_dict_to_file_object
from .general_generators import any_dictionary_key, any_file_contents, any_name, any_safe_filename
from .stac_generators import (
    any_asset_name,
    any_dataset_description,
    any_dataset_id,
    any_dataset_title,
    sha256_hex_digest_to_multihash,
)
from .stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

ACCOUNT_NUMBER = get_account_number()

DATASET_VERSION_ID_REGEX = (
    r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z_[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{16}"
)
AWS_REGION = environ[AWS_DEFAULT_REGION_KEY]

CLI_RUNNER = CliRunner(mix_stderr=False)


@mark.infrastructure
def should_create_dataset(s3_client: S3Client) -> None:
    # When
    dataset_title = any_dataset_title()
    result = CLI_RUNNER.invoke(
        app,
        [
            "dataset",
            "create",
            f"--title={dataset_title}",
            f"--description={any_dataset_description()}",
        ],
    )

    # Then
    assert result.exit_code == 0, result

    # Cleanup
    dataset_id = result.stdout.rstrip()
    wait_for_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, CATALOG_FILENAME, s3_client)
    delete_s3_key(
        Resource.STORAGE_BUCKET_NAME.resource_name,
        f"{dataset_title}{DATASET_KEY_SEPARATOR}{dataset_id}/{CATALOG_FILENAME}",
        s3_client,
    )
    delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, CATALOG_FILENAME, s3_client)


@mark.infrastructure
def should_report_duplicate_dataset_title(s3_client: S3Client, subtests: SubTests) -> None:
    # When
    dataset_title = any_dataset_title()
    first_result = CLI_RUNNER.invoke(
        app,
        [
            "dataset",
            "create",
            f"--title={dataset_title}",
            f"--description={any_dataset_description()}",
        ],
    )
    assert first_result.exit_code == 0, first_result
    dataset_id = first_result.stdout.rstrip()

    duplicate_result = CLI_RUNNER.invoke(
        app,
        [
            "dataset",
            "create",
            f"--title={dataset_title}",
            f"--description={any_dataset_description()}",
        ],
    )

    with subtests.test(msg="should print nothing to standard output"):
        assert duplicate_result.stdout == ""

    with subtests.test(msg="should print error message to standard error"):
        assert duplicate_result.stderr == f"Conflict: dataset '{dataset_title}' already exists\n"

    with subtests.test(msg="should indicate failure via exit code"):
        assert duplicate_result.exit_code == 3, duplicate_result

    # Cleanup
    wait_for_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, CATALOG_FILENAME, s3_client)
    delete_s3_key(
        Resource.STORAGE_BUCKET_NAME.resource_name,
        f"{dataset_title}{DATASET_KEY_SEPARATOR}{dataset_id}/{CATALOG_FILENAME}",
        s3_client,
    )
    delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, CATALOG_FILENAME, s3_client)


@patch("boto3.client")
def should_report_dataset_creation_success(
    boto3_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    dataset_id = any_dataset_id()
    response_payload_object = get_response_object(
        HTTPStatus.CREATED, {DATASET_ID_SHORT_KEY: dataset_id}
    )
    response_payload = json_dict_to_file_object(response_payload_object)
    boto3_client_mock.return_value.invoke.return_value = InvocationResponseTypeDef(
        StatusCode=HTTPStatus.OK,
        FunctionError="",
        LogResult="",
        Payload=response_payload,
        ExecutedVersion=LAMBDA_EXECUTED_VERSION,
    )

    # When
    result = CLI_RUNNER.invoke(
        app,
        [
            "dataset",
            "create",
            f"--title={any_dataset_title()}",
            f"--description={any_dataset_description()}",
        ],
    )

    # Then
    with subtests.test(msg="should print dataset ID to standard output"):
        assert result.stdout == f"{dataset_id}\n"

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""

    with subtests.test(msg="should indicate success via exit code"):
        assert result.exit_code == 0, result


@patch("boto3.client")
def should_print_error_message_when_authentication_missing(
    boto3_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    boto3_client_mock.return_value.invoke.side_effect = NoCredentialsError()

    # When
    result = CLI_RUNNER.invoke(app, ["dataset", "list"])

    # Then
    with subtests.test(msg="should print nothing to standard output"):
        assert result.stdout == ""

    with subtests.test(msg="should print error message to standard error"):
        assert result.stderr == "Unable to locate credentials. Make sure to log in to AWS first.\n"

    with subtests.test(msg="should indicate failure via exit code"):
        assert result.exit_code == 4, result


@patch("boto3.client")
def should_print_error_message_when_region_missing(
    boto3_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    boto3_client_mock.side_effect = NoRegionError()

    # When
    result = CLI_RUNNER.invoke(app, ["dataset", "list"])

    # Then
    with subtests.test(msg="should print nothing to standard output"):
        assert result.stdout == ""

    with subtests.test(msg="should print error message to standard error"):
        assert (
            result.stderr == "Unable to locate region settings. Make sure to log in to AWS first.\n"
        )

    with subtests.test(msg="should indicate failure via exit code"):
        assert result.exit_code == 5, result


@patch("boto3.client")
def should_report_arbitrary_dataset_creation_failure(
    boto3_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given an arbitrary error
    response_body = {any_dictionary_key(): any_name()}
    response_object = get_response_object(HTTPStatus.TOO_MANY_REQUESTS, response_body)
    boto3_client_mock.return_value.invoke.return_value = InvocationResponseTypeDef(
        StatusCode=HTTPStatus.BAD_REQUEST,
        FunctionError="",
        LogResult="",
        Payload=json_dict_to_file_object(response_object),
        ExecutedVersion=LAMBDA_EXECUTED_VERSION,
    )

    # When
    result = CLI_RUNNER.invoke(app, ["dataset", "list"])

    # Then
    with subtests.test(msg="should print nothing to standard output"):
        assert result.stdout == ""

    with subtests.test(msg="should print response body to standard error"):
        assert loads(result.stderr) == response_body

    with subtests.test(msg="should indicate failure via exit code"):
        assert result.exit_code == 1


@mark.infrastructure
def should_list_datasets(subtests: SubTests) -> None:
    # Given two datasets
    with Dataset() as first_dataset, Dataset() as second_dataset:
        # When
        result = CLI_RUNNER.invoke(app, ["dataset", "list"])

    # Then
    with subtests.test(msg="should print datasets to standard output"):
        assert (
            f"{first_dataset.title}{DATASET_KEY_SEPARATOR}{first_dataset.dataset_id}\n"
            in result.stdout
        )
        assert (
            f"{second_dataset.title}{DATASET_KEY_SEPARATOR}{second_dataset.dataset_id}\n"
            in result.stdout
        )

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""

    with subtests.test(msg="should indicate success via exit code"):
        assert result.exit_code == 0


@mark.infrastructure
def should_filter_datasets_listing(subtests: SubTests) -> None:
    # Given two datasets
    with Dataset() as first_dataset, Dataset():
        # When
        result = CLI_RUNNER.invoke(app, ["dataset", "list", f"--id={first_dataset.dataset_id}"])

    # Then
    with subtests.test(msg="should print dataset to standard output"):
        assert (
            result.stdout
            == f"{first_dataset.title}{DATASET_KEY_SEPARATOR}{first_dataset.dataset_id}\n"
        )

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""

    with subtests.test(msg="should indicate success via exit code"):
        assert result.exit_code == 0


@mark.infrastructure
def should_delete_dataset(subtests: SubTests) -> None:
    # Given
    with Dataset() as dataset:
        # When
        result = CLI_RUNNER.invoke(app, ["dataset", "delete", f"--id={dataset.dataset_id}"])

    # Then
    with subtests.test(msg="should print nothing to standard output"):
        assert result.stdout == ""

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""

    with subtests.test(msg="should indicate success via exit code"):
        assert result.exit_code == 0


@mark.infrastructure
def should_create_dataset_version(subtests: SubTests) -> None:
    # Given
    with Dataset() as dataset:
        result = CLI_RUNNER.invoke(
            app,
            [
                "version",
                "create",
                f"--dataset-id={dataset.dataset_id}",
                f"--metadata-url={any_s3_url()}",
                f"--s3-role-arn={any_role_arn()}",
            ],
        )

    # Then
    with subtests.test(msg="should print dataset version ID and execution ARN to standard output"):
        assert match(
            f"^({DATASET_VERSION_ID_REGEX})\tarn:aws:states:{AWS_REGION}:\\d+:execution:.*:\\1\n$",
            result.stdout,
            flags=MULTILINE,
        )

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""

    with subtests.test(msg="should indicate success via exit code"):
        assert result.exit_code == 0, result


@patch("boto3.client")
def should_print_version_import_status_verbatim(
    boto3_client_mock: MagicMock, subtests: SubTests
) -> None:
    response_body = {
        STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
        VALIDATION_KEY: {
            STATUS_KEY: Outcome.FAILED.value,
            ERRORS_KEY: [
                {
                    ERROR_CHECK_KEY: "any check name",
                    ERROR_DETAILS_KEY: {"message": "value"},
                    ERROR_RESULT_KEY: ValidationResult.FAILED.value,
                    ERROR_URL_KEY: any_s3_url(),
                }
            ],
        },
        METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
    }
    response_object = get_response_object(HTTPStatus.OK, response_body)
    boto3_client_mock.return_value.invoke.return_value = InvocationResponseTypeDef(
        StatusCode=HTTPStatus.OK,
        FunctionError="",
        LogResult="",
        Payload=json_dict_to_file_object(response_object),
        ExecutedVersion=LAMBDA_EXECUTED_VERSION,
    )

    result = CLI_RUNNER.invoke(
        app, ["version", "status", f"--execution-arn={any_arn_formatted_string()}"]
    )

    with subtests.test(msg="should print JSON response body to standard output"):
        assert loads(result.stdout) == response_body

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""

    with subtests.test(msg="should indicate success via exit code"):
        assert result.exit_code == 0, result


@mark.infrastructure
def should_get_version_import_status(subtests: SubTests) -> None:
    asset_filename = any_safe_filename()
    asset_content = any_file_contents()
    asset_multihash = sha256_hex_digest_to_multihash(sha256(asset_content).hexdigest())
    with Dataset() as dataset, S3Object(
        file_object=BytesIO(),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=asset_filename,
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ASSETS_KEY: {
                    any_asset_name(): {
                        STAC_HREF_KEY: f"./{asset_filename}",
                        STAC_FILE_CHECKSUM_KEY: asset_multihash,
                    }
                },
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=any_safe_filename(),
    ) as collection_object:
        version_create_result = CLI_RUNNER.invoke(
            app,
            [
                "version",
                "create",
                f"--dataset-id={dataset.dataset_id}",
                f"--metadata-url={collection_object.url}",
                "--s3-role-arn="
                f"arn:aws:iam::{ACCOUNT_NUMBER}:role/{Resource.API_USERS_ROLE_NAME.resource_name}",
            ],
        )
        execution_arn = version_create_result.stdout.split("\t", maxsplit=1)[1].rstrip()

        status_result = CLI_RUNNER.invoke(
            app, ["version", "status", f"--execution-arn={execution_arn}"]
        )

    with subtests.test(msg="should print JSON response body to standard output"):
        assert loads(status_result.stdout)

    with subtests.test(msg="should print nothing to standard error"):
        assert status_result.stderr == ""

    with subtests.test(msg="should indicate success via exit code"):
        assert status_result.exit_code == 0, status_result


def get_response_object(status_code: int, body: JsonObject) -> JsonObject:
    return {STATUS_CODE_KEY: status_code, BODY_KEY: body}


@patch("geostore.cli.handle_api_request")
def should_call_given_environment_function(
    handle_api_request_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    environment_name = "any environment name"

    with patch.dict(environ, {ENV_NAME_VARIABLE_NAME: environment_name}):
        # When
        result = CLI_RUNNER.invoke(
            app, [f"--environment-name={environment_name}", "dataset", "list"]
        )

    # Then
    with subtests.test(msg="should call the function in the given environment"):
        handle_api_request_mock.assert_called_once_with(
            f"{environment_name}-{Resource.DATASETS_ENDPOINT_FUNCTION_NAME.value}", ANY, ANY
        )

    with subtests.test(msg="should print nothing to standard output"):
        assert result.stdout == ""

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""


@patch("geostore.cli.handle_api_request")
def should_default_to_production_environment(
    handle_api_request_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    with patch.dict(environ):
        del environ[ENV_NAME_VARIABLE_NAME]
        # When
        result = CLI_RUNNER.invoke(app, ["dataset", "list"])

    # Then
    with subtests.test(msg="should call the function in the given environment"):
        handle_api_request_mock.assert_called_once_with(
            Resource.DATASETS_ENDPOINT_FUNCTION_NAME.value, ANY, ANY
        )

    with subtests.test(msg="should print nothing to standard output"):
        assert result.stdout == ""

    with subtests.test(msg="should print nothing to standard error"):
        assert result.stderr == ""
