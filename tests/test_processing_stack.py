from copy import deepcopy
from hashlib import sha256
from http import HTTPStatus
from io import BytesIO
from json import dumps, load, loads
from logging import INFO, basicConfig
from time import sleep

import smart_open
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from pytest import mark, raises
from pytest_subtests import SubTests

from geostore.api_keys import STATUS_KEY
from geostore.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from geostore.datasets_model import DATASET_KEY_SEPARATOR
from geostore.parameter_store import ParameterName
from geostore.resources import ResourceName
from geostore.s3 import S3_URL_PREFIX
from geostore.stac_format import (
    STAC_ASSETS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_LINKS_KEY,
    STAC_REL_CHILD,
    STAC_REL_KEY,
    STAC_REL_ROOT,
    STAC_REL_SELF,
)
from geostore.step_function import Outcome
from geostore.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_SHORT_KEY,
    ERRORS_KEY,
    EXECUTION_ARN_KEY,
    FAILED_TASKS_KEY,
    FAILURE_REASONS_KEY,
    IMPORT_DATASET_KEY,
    METADATA_UPLOAD_KEY,
    METADATA_URL_KEY,
    S3_ROLE_ARN_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)
from geostore.sts import get_account_number

from .aws_utils import (
    S3_BATCH_JOB_COMPLETED_STATE,
    Dataset,
    S3Object,
    delete_copy_job_files,
    delete_s3_key,
    get_s3_role_arn,
    wait_for_copy_jobs,
)
from .file_utils import json_dict_to_file_object
from .general_generators import any_file_contents, any_safe_file_path, any_safe_filename
from .stac_generators import any_asset_name, any_hex_multihash, sha256_hex_digest_to_multihash
from .stac_objects import (
    MINIMAL_VALID_STAC_CATALOG_OBJECT,
    MINIMAL_VALID_STAC_COLLECTION_OBJECT,
    MINIMAL_VALID_STAC_ITEM_OBJECT,
)

basicConfig(level=INFO)


@mark.infrastructure
def should_check_state_machine_arn_parameter_exists(ssm_client: SSMClient) -> None:
    """Test if Geostore State Machine ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(
        Name=ParameterName.PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN.value
    )

    parameter = parameter_response["Parameter"]
    assert (
        parameter["Name"]
        == ParameterName.PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN.value
    )

    parameter_value = parameter["Value"]
    assert "arn" in parameter_value
    assert "stateMachine" in parameter_value


@mark.infrastructure
def should_check_s3_batch_copy_role_arn_parameter_exists(ssm_client: SSMClient) -> None:
    """Test if Geostore S3 Batch Copy Role ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(
        Name=ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN.value
    )

    parameter = parameter_response["Parameter"]
    assert parameter["Name"] == ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN.value

    parameter_value = parameter["Value"]
    assert "arn" in parameter_value
    assert "iam" in parameter_value


@mark.timeout(1200)
@mark.infrastructure
def should_successfully_run_dataset_version_creation_process_with_multiple_assets(  # pylint:disable=too-many-statements
    # pylint:disable=too-many-arguments
    step_functions_client: SFNClient,
    lambda_client: LambdaClient,
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    subtests: SubTests,
) -> None:
    # pylint: disable=too-many-locals
    key_prefix = any_safe_file_path()

    collection_metadata_filename = any_safe_filename()
    catalog_metadata_filename = any_safe_filename()
    item_metadata_filename = any_safe_filename()

    metadata_url_prefix = f"{S3_URL_PREFIX}{ResourceName.STAGING_BUCKET_NAME.value}/{key_prefix}"
    collection_metadata_url = f"{metadata_url_prefix}/{collection_metadata_filename}"
    catalog_metadata_url = f"{metadata_url_prefix}/{catalog_metadata_filename}"
    item_metadata_url = f"{metadata_url_prefix}/{item_metadata_filename}"

    first_asset_contents = any_file_contents()
    first_asset_filename = any_safe_filename()
    first_asset_name = any_asset_name()
    first_asset_hex_digest = sha256_hex_digest_to_multihash(
        sha256(first_asset_contents).hexdigest()
    )
    second_asset_contents = any_file_contents()
    second_asset_filename = any_safe_filename()
    second_asset_name = any_asset_name()
    second_asset_hex_digest = sha256_hex_digest_to_multihash(
        sha256(second_asset_contents).hexdigest()
    )

    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_contents),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{first_asset_filename}",
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_contents),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{second_asset_filename}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: collection_metadata_url, STAC_REL_KEY: STAC_REL_CHILD},
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                ],
            }
        ),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{catalog_metadata_filename}",
    ) as catalog_metadata_file, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ASSETS_KEY: {
                    second_asset_name: {
                        STAC_HREF_KEY: f"./{second_asset_filename}",
                        STAC_FILE_CHECKSUM_KEY: second_asset_hex_digest,
                    },
                },
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: item_metadata_url, STAC_REL_KEY: STAC_REL_CHILD},
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                    {STAC_HREF_KEY: collection_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                ],
            }
        ),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{collection_metadata_filename}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                STAC_ASSETS_KEY: {
                    first_asset_name: {
                        STAC_HREF_KEY: first_asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
                    },
                },
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                    {STAC_HREF_KEY: item_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                ],
            }
        ),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{item_metadata_filename}",
    ), Dataset() as dataset:

        # When
        try:
            dataset_versions_response = lambda_client.invoke(
                FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                Payload=dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            DATASET_ID_SHORT_KEY: dataset.dataset_id,
                            METADATA_URL_KEY: catalog_metadata_file.url,
                            S3_ROLE_ARN_KEY: get_s3_role_arn(),
                        },
                    }
                ).encode(),
            )
            dataset_versions_payload = load(dataset_versions_response["Payload"])

            with subtests.test(msg="Dataset Versions endpoint returns success"):
                assert (
                    dataset_versions_payload.get(STATUS_CODE_KEY) == HTTPStatus.CREATED
                ), dataset_versions_payload

            dataset_versions_body = dataset_versions_payload[BODY_KEY]
            with subtests.test(msg="Should complete Step Function successfully"):

                # Then poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=dataset_versions_body[EXECUTION_ARN_KEY]
                    )
                )["status"] == "RUNNING":
                    sleep(5)  # pragma: no cover

                assert execution["status"] == "SUCCEEDED", execution

            assert (execution_output := execution.get("output")), execution

            account_id = get_account_number()

            import_dataset_response = loads(execution_output)[IMPORT_DATASET_KEY]
            metadata_copy_job_result, asset_copy_job_result = wait_for_copy_jobs(
                import_dataset_response, account_id, s3_control_client, subtests
            )

            dataset_version_prefix = (
                f"{dataset.title}{DATASET_KEY_SEPARATOR}{dataset.dataset_id}"
                f"/{dataset_versions_body[VERSION_ID_KEY]}/"
            )
            storage_bucket_prefix = f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/"

            # Catalog contents
            imported_catalog_key = f"{dataset_version_prefix}{catalog_metadata_filename}"
            with subtests.test(msg="Imported catalog has relative keys"), smart_open.open(
                f"{storage_bucket_prefix}{imported_catalog_key}", mode="rb"
            ) as imported_catalog_file:
                assert {
                    **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                    STAC_LINKS_KEY: [
                        {
                            STAC_HREF_KEY: collection_metadata_filename,
                            STAC_REL_KEY: STAC_REL_CHILD,
                        },
                        {
                            STAC_HREF_KEY: catalog_metadata_filename,
                            STAC_REL_KEY: STAC_REL_ROOT,
                        },
                        {
                            STAC_HREF_KEY: catalog_metadata_filename,
                            STAC_REL_KEY: STAC_REL_SELF,
                        },
                    ],
                } == load(imported_catalog_file)

            # Collection contents
            imported_collection_key = f"{dataset_version_prefix}{collection_metadata_filename}"
            with subtests.test(msg="Imported collection has relative keys"), smart_open.open(
                f"{storage_bucket_prefix}{imported_collection_key}", mode="rb"
            ) as imported_collection_file:
                assert {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    STAC_ASSETS_KEY: {
                        second_asset_name: {
                            STAC_HREF_KEY: second_asset_filename,
                            STAC_FILE_CHECKSUM_KEY: second_asset_hex_digest,
                        },
                    },
                    STAC_LINKS_KEY: [
                        {
                            STAC_HREF_KEY: item_metadata_filename,
                            STAC_REL_KEY: STAC_REL_CHILD,
                        },
                        {
                            STAC_HREF_KEY: catalog_metadata_filename,
                            STAC_REL_KEY: STAC_REL_ROOT,
                        },
                        {
                            STAC_HREF_KEY: collection_metadata_filename,
                            STAC_REL_KEY: STAC_REL_SELF,
                        },
                    ],
                } == load(imported_collection_file)

            # Item contents
            imported_item_key = f"{dataset_version_prefix}{item_metadata_filename}"
            with subtests.test(msg="Imported item has relative keys"), smart_open.open(
                f"{storage_bucket_prefix}{imported_item_key}", mode="rb"
            ) as imported_item_file:
                assert {
                    **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                    STAC_ASSETS_KEY: {
                        first_asset_name: {
                            STAC_HREF_KEY: first_asset_filename,
                            STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
                        },
                    },
                    STAC_LINKS_KEY: [
                        {STAC_HREF_KEY: catalog_metadata_filename, STAC_REL_KEY: STAC_REL_ROOT},
                        {STAC_HREF_KEY: item_metadata_filename, STAC_REL_KEY: STAC_REL_SELF},
                    ],
                } == load(imported_item_file)

            # First asset contents
            imported_first_asset_key = f"{dataset_version_prefix}{first_asset_filename}"
            with subtests.test(msg="Verify first asset contents"), smart_open.open(
                f"{storage_bucket_prefix}{imported_first_asset_key}", mode="rb"
            ) as imported_first_asset_file:
                assert first_asset_contents == imported_first_asset_file.read()

            # Second asset contents
            imported_second_asset_key = f"{dataset_version_prefix}{second_asset_filename}"
            with subtests.test(msg="Verify second asset contents"), smart_open.open(
                f"{storage_bucket_prefix}{imported_second_asset_key}", mode="rb"
            ) as imported_second_asset_file:
                assert second_asset_contents == imported_second_asset_file.read()
        finally:
            # Cleanup
            for key in [
                imported_catalog_key,
                imported_collection_key,
                imported_item_key,
                imported_first_asset_key,
                imported_second_asset_key,
            ]:
                with subtests.test(msg=f"Delete {key}"):
                    delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, key, s3_client)

            with subtests.test(msg="Delete copy job files"):
                delete_copy_job_files(
                    metadata_copy_job_result,
                    asset_copy_job_result,
                    ResourceName.STORAGE_BUCKET_NAME.value,
                    s3_client,
                    subtests,
                )

    with subtests.test(msg="Should report import status after success"):
        expected_status_payload = {
            STATUS_CODE_KEY: HTTPStatus.OK,
            BODY_KEY: {
                STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
                VALIDATION_KEY: {STATUS_KEY: Outcome.PASSED.value, ERRORS_KEY: []},
                METADATA_UPLOAD_KEY: {
                    STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE,
                    ERRORS_KEY: {FAILED_TASKS_KEY: 0, FAILURE_REASONS_KEY: []},
                },
                ASSET_UPLOAD_KEY: {
                    STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE,
                    ERRORS_KEY: {FAILED_TASKS_KEY: 0, FAILURE_REASONS_KEY: []},
                },
            },
        }
        status_response = lambda_client.invoke(
            FunctionName=ResourceName.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.value,
            Payload=dumps(
                {
                    HTTP_METHOD_KEY: "GET",
                    BODY_KEY: {EXECUTION_ARN_KEY: execution["executionArn"]},
                }
            ).encode(),
        )
        status_payload = load(status_response["Payload"])
        assert status_payload == expected_status_payload


@mark.timeout(1200)
@mark.infrastructure
def should_successfully_run_dataset_version_creation_process_with_single_asset(
    # pylint:disable=too-many-arguments
    step_functions_client: SFNClient,
    lambda_client: LambdaClient,
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    subtests: SubTests,
) -> None:
    # pylint: disable=too-many-locals
    key_prefix = any_safe_file_path()

    root_metadata_filename = any_safe_filename()
    child_metadata_filename = any_safe_filename()

    asset_contents = any_file_contents()
    asset_filename = any_safe_filename()

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{asset_filename}",
    ) as asset_s3_object, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ASSETS_KEY: {
                    any_asset_name(): {
                        STAC_HREF_KEY: asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: sha256_hex_digest_to_multihash(
                            sha256(asset_contents).hexdigest()
                        ),
                    },
                },
            }
        ),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{child_metadata_filename}",
    ) as child_metadata_file, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: child_metadata_file.url, STAC_REL_KEY: STAC_REL_CHILD}
                ],
            }
        ),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{root_metadata_filename}",
    ) as root_metadata_file, Dataset() as dataset:

        # When
        try:
            dataset_versions_response = lambda_client.invoke(
                FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                Payload=dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            DATASET_ID_SHORT_KEY: dataset.dataset_id,
                            METADATA_URL_KEY: root_metadata_file.url,
                            S3_ROLE_ARN_KEY: get_s3_role_arn(),
                        },
                    }
                ).encode(),
            )
            dataset_versions_payload = load(dataset_versions_response["Payload"])

            with subtests.test(msg="Dataset Versions endpoint returns success"):
                assert (
                    dataset_versions_payload.get(STATUS_CODE_KEY) == HTTPStatus.CREATED
                ), dataset_versions_payload

            dataset_versions_body = dataset_versions_payload[BODY_KEY]
            with subtests.test(msg="Should complete Step Function successfully"):

                # Then poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=dataset_versions_body[EXECUTION_ARN_KEY]
                    )
                )["status"] == "RUNNING":
                    sleep(5)  # pragma: no cover

            assert (execution_output := execution.get("output")), execution

            account_id = get_account_number()

            import_dataset_response = loads(execution_output)[IMPORT_DATASET_KEY]
            metadata_copy_job_result, asset_copy_job_result = wait_for_copy_jobs(
                import_dataset_response,
                account_id,
                s3_control_client,
                subtests,
            )
        finally:
            # Cleanup
            dataset_prefix = f"{dataset.title}{DATASET_KEY_SEPARATOR}{dataset.dataset_id}"
            for filename in [root_metadata_filename, child_metadata_filename, asset_filename]:
                new_key = f"{dataset_prefix}/{dataset_versions_body[VERSION_ID_KEY]}/{filename}"
                with subtests.test(msg=f"Delete {new_key}"):
                    delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, new_key, s3_client)

            with subtests.test(msg="Delete copy job files"):
                delete_copy_job_files(
                    metadata_copy_job_result,
                    asset_copy_job_result,
                    ResourceName.STORAGE_BUCKET_NAME.value,
                    s3_client,
                    subtests,
                )

    with subtests.test(msg="Should report import status after success"):
        expected_status_payload = {
            STATUS_CODE_KEY: HTTPStatus.OK,
            BODY_KEY: {
                STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
                VALIDATION_KEY: {STATUS_KEY: Outcome.PASSED.value, ERRORS_KEY: []},
                METADATA_UPLOAD_KEY: {
                    STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE,
                    ERRORS_KEY: {FAILED_TASKS_KEY: 0, FAILURE_REASONS_KEY: []},
                },
                ASSET_UPLOAD_KEY: {
                    STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE,
                    ERRORS_KEY: {FAILED_TASKS_KEY: 0, FAILURE_REASONS_KEY: []},
                },
            },
        }
        status_response = lambda_client.invoke(
            FunctionName=ResourceName.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.value,
            Payload=dumps(
                {
                    HTTP_METHOD_KEY: "GET",
                    BODY_KEY: {EXECUTION_ARN_KEY: execution["executionArn"]},
                }
            ).encode(),
        )
        status_payload = load(status_response["Payload"])
        assert status_payload == expected_status_payload


@mark.infrastructure
def should_not_copy_files_when_there_is_a_checksum_mismatch(
    lambda_client: LambdaClient,
    s3_client: S3Client,
    step_functions_client: SFNClient,
    subtests: SubTests,
) -> None:
    # pylint:disable=too-many-locals
    # Given an asset with an invalid checksum
    key_prefix = any_safe_file_path()

    metadata_filename = any_safe_filename()
    asset_filename = any_safe_filename()

    with S3Object(
        file_object=BytesIO(),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{asset_filename}",
    ) as asset_s3_object, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ASSETS_KEY: {
                    any_asset_name(): {
                        STAC_HREF_KEY: asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: any_hex_multihash(),
                    },
                },
            }
        ),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{metadata_filename}",
    ) as s3_metadata_file, Dataset() as dataset:

        # When creating a dataset version
        dataset_version_creation_response = lambda_client.invoke(
            FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
            Payload=dumps(
                {
                    HTTP_METHOD_KEY: "POST",
                    BODY_KEY: {
                        DATASET_ID_SHORT_KEY: dataset.dataset_id,
                        METADATA_URL_KEY: s3_metadata_file.url,
                        S3_ROLE_ARN_KEY: get_s3_role_arn(),
                    },
                }
            ).encode(),
        )

        response_payload = load(dataset_version_creation_response["Payload"])
        with subtests.test(msg="Dataset Versions endpoint status code"):
            assert response_payload.get(STATUS_CODE_KEY) == HTTPStatus.CREATED, response_payload

        dataset_versions_body = response_payload[BODY_KEY]
        with subtests.test(msg="Step function result"):
            # Then poll for State Machine State
            state_machine_arn = dataset_versions_body[EXECUTION_ARN_KEY]
            while (
                execution := step_functions_client.describe_execution(
                    executionArn=state_machine_arn
                )
            )["status"] == "RUNNING":

                sleep(5)  # pragma: no cover

            assert execution["status"] == "SUCCEEDED", execution

    # Then the files should not be copied
    dataset_version = dataset_versions_body[VERSION_ID_KEY]
    dataset_prefix = f"{dataset.title}{DATASET_KEY_SEPARATOR}{dataset.dataset_id}"
    for filename in [metadata_filename, asset_filename]:
        with subtests.test(msg=filename), raises(AssertionError):
            delete_s3_key(
                ResourceName.STORAGE_BUCKET_NAME.value,
                f"{dataset_prefix}/{dataset_version}/{filename}",
                s3_client,
            )
