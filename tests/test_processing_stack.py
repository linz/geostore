# pylint: disable=too-many-lines

from copy import deepcopy
from datetime import timedelta
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
from geostore.aws_keys import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from geostore.dataset_properties import DATASET_KEY_SEPARATOR
from geostore.models import DB_KEY_SEPARATOR
from geostore.parameter_store import ParameterName
from geostore.populate_catalog.task import (
    CATALOG_FILENAME,
    ROOT_CATALOG_DESCRIPTION,
    ROOT_CATALOG_ID,
    ROOT_CATALOG_TITLE,
)
from geostore.processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX
from geostore.stac_format import (
    LINZ_STAC_CREATED_KEY,
    LINZ_STAC_UPDATED_KEY,
    STAC_ASSETS_KEY,
    STAC_DESCRIPTION_KEY,
    STAC_EXTENSIONS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_LINKS_KEY,
    STAC_MEDIA_TYPE_JSON,
    STAC_REL_CHILD,
    STAC_REL_ITEM,
    STAC_REL_KEY,
    STAC_REL_PARENT,
    STAC_REL_ROOT,
    STAC_REL_SELF,
    STAC_TITLE_KEY,
    STAC_TYPE_KEY,
)
from geostore.step_function import Outcome, get_hash_key
from geostore.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_SHORT_KEY,
    DESCRIPTION_KEY,
    ERRORS_KEY,
    EXECUTION_ARN_KEY,
    FAILED_TASKS_KEY,
    FAILURE_REASONS_KEY,
    IMPORT_DATASET_KEY,
    METADATA_UPLOAD_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    S3_ROLE_ARN_KEY,
    STEP_FUNCTION_KEY,
    TITLE_KEY,
    VALIDATION_KEY,
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
    wait_for_s3_key,
)
from .file_utils import json_dict_to_file_object
from .general_generators import (
    any_file_contents,
    any_past_datetime_string,
    any_safe_file_path,
    any_safe_filename,
)
from .stac_generators import (
    any_asset_name,
    any_dataset_description,
    any_dataset_title,
    any_hex_multihash,
    sha256_hex_digest_to_multihash,
)
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

    metadata_url_prefix = (
        f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/{key_prefix}"
    )
    collection_metadata_url = f"{metadata_url_prefix}/{collection_metadata_filename}"
    catalog_metadata_url = f"{metadata_url_prefix}/{catalog_metadata_filename}"
    item_metadata_url = f"{metadata_url_prefix}/{item_metadata_filename}"

    collection_title = any_dataset_title()
    collection_dict = {
        **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
        STAC_TITLE_KEY: collection_title,
    }
    first_asset_contents = any_file_contents()
    first_asset_filename = any_safe_filename()
    first_asset_name = any_asset_name()
    first_asset_hex_digest = sha256_hex_digest_to_multihash(
        sha256(first_asset_contents).hexdigest()
    )
    first_asset_created = any_past_datetime_string()
    first_asset_updated = any_past_datetime_string()
    second_asset_contents = any_file_contents()
    second_asset_filename = any_safe_filename()
    second_asset_name = any_asset_name()
    second_asset_hex_digest = sha256_hex_digest_to_multihash(
        sha256(second_asset_contents).hexdigest()
    )
    second_asset_created = any_past_datetime_string()
    second_asset_updated = any_past_datetime_string()

    metadata_copy_job_result = None
    asset_copy_job_result = None

    dataset_title = any_dataset_title()

    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_contents),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{first_asset_filename}",
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_contents),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
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
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{catalog_metadata_filename}",
    ) as catalog_metadata_file, S3Object(
        file_object=json_dict_to_file_object(
            {
                **collection_dict,
                STAC_ASSETS_KEY: {
                    second_asset_name: {
                        LINZ_STAC_CREATED_KEY: second_asset_created,
                        LINZ_STAC_UPDATED_KEY: second_asset_updated,
                        STAC_HREF_KEY: f"./{second_asset_filename}",
                        STAC_FILE_CHECKSUM_KEY: second_asset_hex_digest,
                    },
                },
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: item_metadata_url, STAC_REL_KEY: STAC_REL_ITEM},
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_PARENT},
                    {STAC_HREF_KEY: collection_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                ],
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{collection_metadata_filename}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                STAC_ASSETS_KEY: {
                    first_asset_name: {
                        LINZ_STAC_CREATED_KEY: first_asset_created,
                        LINZ_STAC_UPDATED_KEY: first_asset_updated,
                        STAC_HREF_KEY: first_asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
                    },
                },
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                    {STAC_HREF_KEY: collection_metadata_url, STAC_REL_KEY: STAC_REL_PARENT},
                    {STAC_HREF_KEY: item_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                ],
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{item_metadata_filename}",
    ):

        # When
        try:

            dataset_response = lambda_client.invoke(
                FunctionName=Resource.DATASETS_ENDPOINT_FUNCTION_NAME.resource_name,
                Payload=dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            TITLE_KEY: dataset_title,
                            DESCRIPTION_KEY: any_dataset_description(),
                        },
                    }
                ).encode(),
            )
            dataset_payload = load(dataset_response["Payload"])

            dataset_id = dataset_payload[BODY_KEY][DATASET_ID_SHORT_KEY]
            dataset_prefix = f"{dataset_title}{DATASET_KEY_SEPARATOR}{dataset_id}/"

            dataset_versions_response = lambda_client.invoke(
                FunctionName=Resource.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.resource_name,
                Payload=dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            DATASET_ID_SHORT_KEY: dataset_id,
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

            wait_for_s3_key(
                Resource.STORAGE_BUCKET_NAME.resource_name,
                CATALOG_FILENAME,
                s3_client,
            )

            storage_bucket_prefix = f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/"

            # Catalog contents
            imported_catalog_key = f"{dataset_prefix}{catalog_metadata_filename}"
            with subtests.test(msg="Imported catalog has relative keys"), smart_open.open(
                f"{storage_bucket_prefix}{imported_catalog_key}", mode="rb"
            ) as imported_catalog_file:
                assert load(imported_catalog_file) == {
                    **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                    STAC_EXTENSIONS_KEY: [],  # pystac writes this field.
                    STAC_LINKS_KEY: [
                        {
                            STAC_HREF_KEY: f"./{collection_metadata_filename}",
                            STAC_REL_KEY: STAC_REL_CHILD,
                            STAC_TITLE_KEY: collection_title,
                        },
                        {
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_REL_KEY: STAC_REL_ROOT,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                        {
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_REL_KEY: STAC_REL_PARENT,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                    ],
                }

            # Collection contents
            imported_collection_key = f"{dataset_prefix}{collection_metadata_filename}"
            with subtests.test(msg="Imported collection has relative keys"), smart_open.open(
                f"{storage_bucket_prefix}{imported_collection_key}", mode="rb"
            ) as imported_collection_file:
                assert load(imported_collection_file) == {
                    **collection_dict,
                    STAC_ASSETS_KEY: {
                        second_asset_name: {
                            LINZ_STAC_CREATED_KEY: second_asset_created,
                            LINZ_STAC_UPDATED_KEY: second_asset_updated,
                            STAC_HREF_KEY: second_asset_filename,
                            STAC_FILE_CHECKSUM_KEY: second_asset_hex_digest,
                        },
                    },
                    STAC_LINKS_KEY: [
                        {
                            STAC_HREF_KEY: f"./{item_metadata_filename}",
                            STAC_REL_KEY: STAC_REL_ITEM,
                        },
                        {
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_REL_KEY: STAC_REL_ROOT,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                        {
                            STAC_HREF_KEY: f"./{catalog_metadata_filename}",
                            STAC_REL_KEY: STAC_REL_PARENT,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                    ],
                }

            # Item contents
            imported_item_key = f"{dataset_prefix}{item_metadata_filename}"

            with subtests.test(msg="Imported item has relative keys"), smart_open.open(
                f"{storage_bucket_prefix}{imported_item_key}", mode="rb"
            ) as imported_item_file:
                assert load(imported_item_file) == {
                    **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                    STAC_ASSETS_KEY: {
                        first_asset_name: {
                            LINZ_STAC_CREATED_KEY: first_asset_created,
                            LINZ_STAC_UPDATED_KEY: first_asset_updated,
                            STAC_HREF_KEY: f"./{first_asset_filename}",
                            STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
                        },
                    },
                    STAC_LINKS_KEY: [
                        {
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_REL_KEY: STAC_REL_ROOT,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                        {
                            STAC_HREF_KEY: f"./{collection_metadata_filename}",
                            STAC_REL_KEY: STAC_REL_PARENT,
                            STAC_TITLE_KEY: collection_title,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                    ],
                }

            # First asset contents
            imported_first_asset_key = f"{dataset_prefix}{first_asset_filename}"
            with subtests.test(msg="Verify first asset contents"), smart_open.open(
                f"{storage_bucket_prefix}{imported_first_asset_key}", mode="rb"
            ) as imported_first_asset_file:
                assert imported_first_asset_file.read() == first_asset_contents

            # Second asset contents
            imported_second_asset_key = f"{dataset_prefix}{second_asset_filename}"
            with subtests.test(msg="Verify second asset contents"), smart_open.open(
                f"{storage_bucket_prefix}{imported_second_asset_key}", mode="rb"
            ) as imported_second_asset_file:
                assert imported_second_asset_file.read() == second_asset_contents
        finally:
            # Cleanup

            for key in [
                CATALOG_FILENAME,
                imported_catalog_key,
                imported_collection_key,
                imported_item_key,
                imported_first_asset_key,
                imported_second_asset_key,
            ]:
                with subtests.test(msg=f"Delete {key}"):
                    delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, key, s3_client)

            with subtests.test(msg="Delete copy job files"):
                assert metadata_copy_job_result is not None
                assert asset_copy_job_result is not None
                delete_copy_job_files(
                    metadata_copy_job_result,
                    asset_copy_job_result,
                    Resource.STORAGE_BUCKET_NAME.resource_name,
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
            FunctionName=Resource.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.resource_name,
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

    metadata_copy_job_result = None
    asset_copy_job_result = None

    dataset_title = any_dataset_title()

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{asset_filename}",
    ) as asset_s3_object, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ASSETS_KEY: {
                    any_asset_name(): {
                        LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
                        LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
                        STAC_HREF_KEY: asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: sha256_hex_digest_to_multihash(
                            sha256(asset_contents).hexdigest()
                        ),
                    },
                },
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
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
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{root_metadata_filename}",
    ) as root_metadata_file:

        # When
        try:

            dataset_response = lambda_client.invoke(
                FunctionName=Resource.DATASETS_ENDPOINT_FUNCTION_NAME.resource_name,
                Payload=dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            TITLE_KEY: dataset_title,
                            DESCRIPTION_KEY: any_dataset_description(),
                        },
                    }
                ).encode(),
            )
            dataset_payload = load(dataset_response["Payload"])
            dataset_id = dataset_payload[BODY_KEY][DATASET_ID_SHORT_KEY]

            dataset_versions_response = lambda_client.invoke(
                FunctionName=Resource.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.resource_name,
                Payload=dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            DATASET_ID_SHORT_KEY: dataset_id,
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

            wait_for_s3_key(
                Resource.STORAGE_BUCKET_NAME.resource_name,
                CATALOG_FILENAME,
                s3_client,
            )
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
            for filename in [root_metadata_filename, child_metadata_filename, asset_filename]:
                new_key = f"{dataset_title}{DATASET_KEY_SEPARATOR}{dataset_id}/{filename}"
                with subtests.test(msg=f"Delete {new_key}"):
                    delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, new_key, s3_client)

            with subtests.test(msg="Delete copy job files"):
                assert metadata_copy_job_result is not None
                assert asset_copy_job_result is not None
                delete_copy_job_files(
                    metadata_copy_job_result,
                    asset_copy_job_result,
                    Resource.STORAGE_BUCKET_NAME.resource_name,
                    s3_client,
                    subtests,
                )

            delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, CATALOG_FILENAME, s3_client)

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
            FunctionName=Resource.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.resource_name,
            Payload=dumps(
                {
                    HTTP_METHOD_KEY: "GET",
                    BODY_KEY: {EXECUTION_ARN_KEY: execution["executionArn"]},
                }
            ).encode(),
        )
        status_payload = load(status_response["Payload"])
        assert status_payload == expected_status_payload


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_successfully_run_dataset_version_creation_process_with_partial_upload(  # pylint:disable=too-many-statements
    step_functions_client: SFNClient,
    lambda_client: LambdaClient,
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    subtests: SubTests,
) -> None:
    # pylint: disable=too-many-locals

    with Dataset() as dataset:
        key_prefix = any_safe_file_path()

        collection_metadata_filename = any_safe_filename()
        catalog_metadata_filename = any_safe_filename()
        item_metadata_filename = any_safe_filename()
        first_asset_filename = any_safe_filename()
        second_asset_filename = any_safe_filename()

        metadata_url_prefix = (
            f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/{key_prefix}"
        )
        collection_metadata_url = f"{metadata_url_prefix}/{collection_metadata_filename}"
        catalog_metadata_url = f"{metadata_url_prefix}/{catalog_metadata_filename}"
        item_metadata_url = f"{metadata_url_prefix}/{item_metadata_filename}"

        collection_title = any_dataset_title()

        first_asset_contents = any_file_contents()
        first_asset_name = any_asset_name()
        first_asset_hex_digest = sha256_hex_digest_to_multihash(
            sha256(first_asset_contents).hexdigest()
        )
        first_asset_created = any_past_datetime_string()
        first_asset_updated = any_past_datetime_string()

        second_asset_contents = any_file_contents()
        second_asset_name = any_asset_name()
        second_asset_hex_digest = sha256_hex_digest_to_multihash(
            sha256(second_asset_contents).hexdigest()
        )
        second_asset_created = any_past_datetime_string()
        second_asset_updated = any_past_datetime_string()
        second_asset_staging_url = f"{metadata_url_prefix}/{second_asset_filename}"

        metadata_copy_job_result = None
        asset_copy_job_result = None

        root_catalog_dict = {
            **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
            STAC_EXTENSIONS_KEY: [],
            STAC_ID_KEY: ROOT_CATALOG_ID,
            STAC_DESCRIPTION_KEY: ROOT_CATALOG_DESCRIPTION,
            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
            STAC_LINKS_KEY: [
                {
                    STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{catalog_metadata_filename}",
                    STAC_REL_KEY: STAC_REL_CHILD,
                    STAC_TITLE_KEY: dataset.title,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
                {
                    STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                    STAC_REL_KEY: STAC_REL_ROOT,
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
            ],
        }

        catalog_dict = {
            **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
            STAC_TITLE_KEY: dataset.title,
            STAC_EXTENSIONS_KEY: [],
            STAC_LINKS_KEY: [
                {
                    STAC_HREF_KEY: f"./{collection_metadata_filename}",
                    STAC_REL_KEY: STAC_REL_CHILD,
                    STAC_TITLE_KEY: collection_title,
                },
                {
                    STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                    STAC_REL_KEY: STAC_REL_ROOT,
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
                {
                    STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                    STAC_REL_KEY: STAC_REL_PARENT,
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
            ],
        }

        collection_dict = {
            **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
            STAC_TITLE_KEY: collection_title,
            STAC_LINKS_KEY: [
                {
                    STAC_HREF_KEY: f"./{item_metadata_filename}",
                    STAC_REL_KEY: STAC_REL_ITEM,
                },
                {
                    STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                    STAC_REL_KEY: STAC_REL_ROOT,
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
                {
                    STAC_HREF_KEY: f"./{catalog_metadata_filename}",
                    STAC_REL_KEY: STAC_REL_PARENT,
                    STAC_TITLE_KEY: dataset.title,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
            ],
        }

        with S3Object(
            file_object=BytesIO(initial_bytes=first_asset_contents),
            bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
            key=f"{key_prefix}/{first_asset_filename}",
        ) as first_asset_s3_object, S3Object(
            file_object=BytesIO(initial_bytes=second_asset_contents),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=f"{dataset.dataset_prefix}/{second_asset_filename}",
        ) as second_asset_s3_object, S3Object(
            file_object=json_dict_to_file_object(root_catalog_dict),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=CATALOG_FILENAME,
        ) as root_catalog_metadata_file, S3Object(
            file_object=json_dict_to_file_object(catalog_dict),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=f"{dataset.dataset_prefix}/{catalog_metadata_filename}",
        ) as catalog_metadata_file, S3Object(
            file_object=json_dict_to_file_object(collection_dict),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=f"{dataset.dataset_prefix}/{collection_metadata_filename}",
        ) as collection_metadata_file, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                    STAC_ASSETS_KEY: {
                        first_asset_name: {
                            LINZ_STAC_CREATED_KEY: first_asset_created,
                            LINZ_STAC_UPDATED_KEY: first_asset_updated,
                            STAC_HREF_KEY: first_asset_s3_object.url,
                            STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
                        },
                        second_asset_name: {
                            LINZ_STAC_CREATED_KEY: second_asset_created,
                            LINZ_STAC_UPDATED_KEY: second_asset_updated,
                            STAC_HREF_KEY: second_asset_staging_url,
                            STAC_FILE_CHECKSUM_KEY: second_asset_hex_digest,
                        },
                    },
                    STAC_LINKS_KEY: [
                        {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                        {STAC_HREF_KEY: collection_metadata_url, STAC_REL_KEY: STAC_REL_PARENT},
                        {STAC_HREF_KEY: item_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                    ],
                }
            ),
            bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
            key=f"{key_prefix}/{item_metadata_filename}",
        ):

            # When
            try:
                dataset_versions_response = lambda_client.invoke(
                    FunctionName=Resource.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.resource_name,
                    Payload=dumps(
                        {
                            HTTP_METHOD_KEY: "POST",
                            BODY_KEY: {
                                DATASET_ID_SHORT_KEY: dataset.dataset_id,
                                METADATA_URL_KEY: catalog_metadata_url,
                                S3_ROLE_ARN_KEY: get_s3_role_arn(),
                            },
                        }
                    ).encode(),
                )
                dataset_versions_body = load(dataset_versions_response["Payload"])[BODY_KEY]

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

                hash_key = get_hash_key(
                    dataset.dataset_id, dataset_versions_body[NEW_VERSION_ID_KEY]
                )

                processing_assets_model = processing_assets_model_with_meta()
                expected_metadata_items = [
                    processing_assets_model(
                        hash_key=hash_key,
                        range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}0",
                        url=catalog_metadata_url,
                        exists_in_staging=False,
                    ),
                    processing_assets_model(
                        hash_key=hash_key,
                        range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}1",
                        url=collection_metadata_url,
                        exists_in_staging=False,
                    ),
                    processing_assets_model(
                        hash_key=hash_key,
                        range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}2",
                        url=item_metadata_url,
                        exists_in_staging=True,
                    ),
                ]
                actual_metadata_items = processing_assets_model.query(
                    hash_key,
                    processing_assets_model.sk.startswith(
                        f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}"
                    ),
                    consistent_read=True,
                )
                for expected_item in expected_metadata_items:
                    with subtests.test(msg=f"Metadata {expected_item.pk}"):
                        assert (
                            actual_metadata_items.next().attribute_values
                            == expected_item.attribute_values
                        )

                expected_asset_items = [
                    processing_assets_model(
                        hash_key=hash_key,
                        range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
                        multihash=first_asset_hex_digest,
                        url=first_asset_s3_object.url,
                        exists_in_staging=True,
                    ),
                    processing_assets_model(
                        hash_key=hash_key,
                        range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}1",
                        multihash=second_asset_hex_digest,
                        url=second_asset_staging_url,
                        exists_in_staging=False,
                    ),
                ]
                actual_asset_items = processing_assets_model.query(
                    hash_key,
                    processing_assets_model.sk.startswith(
                        f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}"
                    ),
                    consistent_read=True,
                )
                for expected_item in expected_asset_items:
                    with subtests.test(msg=f"Metadata {expected_item.pk}"):
                        assert (
                            actual_asset_items.next().attribute_values
                            == expected_item.attribute_values
                        )

                account_id = get_account_number()

                import_dataset_response = loads(execution_output)[IMPORT_DATASET_KEY]
                metadata_copy_job_result, asset_copy_job_result = wait_for_copy_jobs(
                    import_dataset_response, account_id, s3_control_client, subtests
                )

                storage_bucket_prefix = (
                    f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/"
                )

                #  Wait for Item metadata file to include a link to geostore root catalog
                expected_link_object = {
                    STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                    STAC_REL_KEY: STAC_REL_ROOT,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                }

                while (
                    expected_link_object
                    not in (
                        load(
                            smart_open.open(
                                f"{storage_bucket_prefix}{dataset.dataset_prefix}"
                                f"/{item_metadata_filename}",
                                mode="rb",
                            )
                        )
                    )[STAC_LINKS_KEY]
                ):
                    sleep(5)  # pragma: no cover

                # Root Catalog contents
                with subtests.test(msg="Root catalog is unchanged"), smart_open.open(
                    root_catalog_metadata_file.url, mode="rb"
                ) as existing_root_catalog_file:
                    assert load(existing_root_catalog_file) == root_catalog_dict

                # Catalog contents
                with subtests.test(msg="Catalog is unchanged"), smart_open.open(
                    catalog_metadata_file.url, mode="rb"
                ) as existing_catalog_file:
                    assert load(existing_catalog_file) == catalog_dict

                # Collection contents
                with subtests.test(msg="Collection is unchanged"), smart_open.open(
                    collection_metadata_file.url, mode="rb"
                ) as existing_collection_file:
                    assert load(existing_collection_file) == collection_dict

                # Item contents
                imported_item_key = f"{dataset.dataset_prefix}/{item_metadata_filename}"
                with subtests.test(msg="Imported item has correct links"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_item_key}", mode="rb"
                ) as imported_item_file:
                    assert load(imported_item_file) == {
                        **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                        STAC_ASSETS_KEY: {
                            first_asset_name: {
                                LINZ_STAC_CREATED_KEY: first_asset_created,
                                LINZ_STAC_UPDATED_KEY: first_asset_updated,
                                STAC_HREF_KEY: f"./{first_asset_filename}",
                                STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
                            },
                            second_asset_name: {
                                LINZ_STAC_CREATED_KEY: second_asset_created,
                                LINZ_STAC_UPDATED_KEY: second_asset_updated,
                                STAC_HREF_KEY: f"./{second_asset_filename}",
                                STAC_FILE_CHECKSUM_KEY: second_asset_hex_digest,
                            },
                        },
                        STAC_LINKS_KEY: [
                            {
                                STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                                STAC_REL_KEY: STAC_REL_ROOT,
                                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                            },
                            {
                                STAC_HREF_KEY: f"./{collection_metadata_filename}",
                                STAC_REL_KEY: STAC_REL_PARENT,
                                STAC_TITLE_KEY: collection_title,
                                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                            },
                        ],
                    }

                # First asset contents
                imported_first_asset_key = f"{dataset.dataset_prefix}/{first_asset_filename}"
                with subtests.test(msg="Verify first asset contents"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_first_asset_key}", mode="rb"
                ) as imported_first_asset_file:
                    assert imported_first_asset_file.read() == first_asset_contents

                # Second asset contents
                with subtests.test(msg="Verify second asset contents"), smart_open.open(
                    second_asset_s3_object.url, mode="rb"
                ) as second_asset_file:
                    assert second_asset_file.read() == second_asset_contents

            finally:
                # Cleanup

                for key in [
                    imported_item_key,
                    imported_first_asset_key,
                ]:
                    with subtests.test(msg=f"Delete {key}"):
                        delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, key, s3_client)

                with subtests.test(msg="Delete copy job files"):
                    assert metadata_copy_job_result is not None
                    assert asset_copy_job_result is not None
                    delete_copy_job_files(
                        metadata_copy_job_result,
                        asset_copy_job_result,
                        Resource.STORAGE_BUCKET_NAME.resource_name,
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
                FunctionName=Resource.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.resource_name,
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

    dataset_title = any_dataset_title()

    with S3Object(
        file_object=BytesIO(),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{asset_filename}",
    ) as asset_s3_object, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ASSETS_KEY: {
                    any_asset_name(): {
                        LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
                        LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
                        STAC_HREF_KEY: asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: any_hex_multihash(),
                    },
                },
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{metadata_filename}",
    ) as s3_metadata_file:

        dataset_response = lambda_client.invoke(
            FunctionName=Resource.DATASETS_ENDPOINT_FUNCTION_NAME.resource_name,
            Payload=dumps(
                {
                    HTTP_METHOD_KEY: "POST",
                    BODY_KEY: {
                        TITLE_KEY: dataset_title,
                        DESCRIPTION_KEY: any_dataset_description(),
                    },
                }
            ).encode(),
        )
        dataset_payload = load(dataset_response["Payload"])
        dataset_id = dataset_payload[BODY_KEY][DATASET_ID_SHORT_KEY]

        # When creating a dataset version
        dataset_version_creation_response = lambda_client.invoke(
            FunctionName=Resource.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.resource_name,
            Payload=dumps(
                {
                    HTTP_METHOD_KEY: "POST",
                    BODY_KEY: {
                        DATASET_ID_SHORT_KEY: dataset_id,
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
    dataset_prefix = f"{dataset_title}{DATASET_KEY_SEPARATOR}{dataset_id}"
    for filename in [metadata_filename, asset_filename]:
        with subtests.test(msg=filename), raises(AssertionError):
            delete_s3_key(
                Resource.STORAGE_BUCKET_NAME.resource_name,
                f"{dataset_prefix}/{filename}",
                s3_client,
            )
