from http import HTTPStatus
from io import BytesIO
from json import dumps, loads
from unittest.mock import MagicMock, patch

from botocore.exceptions import NoCredentialsError, NoRegionError
from mypy_boto3_lambda.type_defs import InvocationResponseTypeDef
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests
from typer.testing import CliRunner

from geostore.aws_keys import BODY_KEY, STATUS_CODE_KEY
from geostore.cli import app
from geostore.dataset_keys import DATASET_KEY_SEPARATOR
from geostore.populate_catalog.task import CATALOG_FILENAME
from geostore.resources import ResourceName
from geostore.step_function_keys import DATASET_ID_SHORT_KEY
from geostore.types import JsonObject

from .aws_utils import LAMBDA_EXECUTED_VERSION, delete_s3_key, wait_for_s3_key
from .general_generators import any_dictionary_key, any_name
from .stac_generators import any_dataset_description, any_dataset_id, any_dataset_title

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
    wait_for_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, CATALOG_FILENAME, s3_client)
    delete_s3_key(
        ResourceName.STORAGE_BUCKET_NAME.value,
        f"{dataset_title}{DATASET_KEY_SEPARATOR}{dataset_id}/{CATALOG_FILENAME}",
        s3_client,
    )
    delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, CATALOG_FILENAME, s3_client)


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
    wait_for_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, CATALOG_FILENAME, s3_client)
    delete_s3_key(
        ResourceName.STORAGE_BUCKET_NAME.value,
        f"{dataset_title}{DATASET_KEY_SEPARATOR}{dataset_id}/{CATALOG_FILENAME}",
        s3_client,
    )
    delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, CATALOG_FILENAME, s3_client)


@patch("boto3.client")
def should_report_dataset_creation_success(
    boto3_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    dataset_id = any_dataset_id()
    response_payload_object = get_response_object(
        HTTPStatus.CREATED, {DATASET_ID_SHORT_KEY: dataset_id}
    )
    response_payload = BytesIO(initial_bytes=dumps(response_payload_object).encode())
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
    response_payload = dumps(response_object).encode()
    boto3_client_mock.return_value.invoke.return_value = InvocationResponseTypeDef(
        StatusCode=HTTPStatus.BAD_REQUEST,
        FunctionError="",
        LogResult="",
        Payload=BytesIO(initial_bytes=response_payload),
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
    with subtests.test(msg="should print nothing to standard output"):
        assert result.stdout == ""

    with subtests.test(msg="should print response body to standard error"):
        assert loads(result.stderr) == response_body

    with subtests.test(msg="should indicate failure via exit code"):
        assert result.exit_code == 1


def get_response_object(status_code: int, body: JsonObject) -> JsonObject:
    return {STATUS_CODE_KEY: status_code, BODY_KEY: body}
