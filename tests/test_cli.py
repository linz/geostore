from http import HTTPStatus
from json import loads
from os import environ
from re import MULTILINE, match
from unittest.mock import MagicMock, patch

from botocore.exceptions import NoCredentialsError, NoRegionError
from mypy_boto3_lambda.type_defs import InvocationResponseTypeDef
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests
from typer.testing import CliRunner

from geostore.aws_keys import AWS_DEFAULT_REGION_KEY, BODY_KEY, STATUS_CODE_KEY
from geostore.cli import app
from geostore.dataset_keys import DATASET_KEY_SEPARATOR
from geostore.populate_catalog.task import CATALOG_FILENAME
from geostore.resources import Resource
from geostore.step_function_keys import DATASET_ID_SHORT_KEY
from geostore.types import JsonObject

from .aws_utils import (
    LAMBDA_EXECUTED_VERSION,
    Dataset,
    any_role_arn,
    any_s3_url,
    delete_s3_key,
    wait_for_s3_key,
)
from .file_utils import json_dict_to_file_object
from .general_generators import any_dictionary_key, any_name
from .stac_generators import any_dataset_description, any_dataset_id, any_dataset_title

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


def get_response_object(status_code: int, body: JsonObject) -> JsonObject:
    return {STATUS_CODE_KEY: status_code, BODY_KEY: body}
