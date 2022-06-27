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
from geostore.datasets_model import datasets_model_with_meta
from geostore.models import DATASET_ID_PREFIX
from geostore.parameter_store import ParameterName
from geostore.populate_catalog.task import CATALOG_FILENAME, ROOT_CATALOG_TITLE
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX
from geostore.stac_format import (
    LINZ_STAC_CREATED_KEY,
    LINZ_STAC_UPDATED_KEY,
    STAC_ASSETS_KEY,
    STAC_EXTENSIONS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
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
from geostore.step_function import Outcome
from geostore.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_SHORT_KEY,
    DATASET_TITLE_KEY,
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
    VALIDATION_KEY,
)
from geostore.sts import get_account_number

from .aws_utils import (
    S3_BATCH_JOB_COMPLETED_STATE,
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
                            DATASET_TITLE_KEY: dataset_title,
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
                new_key = f"{dataset_title}/{filename}"
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
def should_successfully_run_dataset_version_creation_process_and_again_with_partial_upload(
    # pylint:disable=too-many-statements
    step_functions_client: SFNClient,
    lambda_client: LambdaClient,
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    subtests: SubTests,
) -> None:
    # pylint: disable=too-many-locals

    key_prefix = any_safe_file_path()
    dataset_title = any_dataset_title()

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
    collection_dict = {
        **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
        STAC_TITLE_KEY: collection_title,
    }

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

    first_metadata_copy_job_result = None
    first_asset_copy_job_result = None
    second_metadata_copy_job_result = None
    second_asset_copy_job_result = None

    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_contents),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{first_asset_filename}",
    ), S3Object(
        file_object=BytesIO(initial_bytes=second_asset_contents),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{second_asset_filename}",
    ) as second_asset_s3_object, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_TITLE_KEY: dataset_title,
                STAC_EXTENSIONS_KEY: [],
                STAC_LINKS_KEY: [
                    {
                        STAC_HREF_KEY: collection_metadata_url,
                        STAC_REL_KEY: STAC_REL_CHILD,
                    },
                    {
                        STAC_HREF_KEY: catalog_metadata_url,
                        STAC_REL_KEY: STAC_REL_ROOT,
                    },
                ],
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{catalog_metadata_filename}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **collection_dict,
                STAC_ASSETS_KEY: {
                    first_asset_name: {
                        LINZ_STAC_CREATED_KEY: first_asset_created,
                        LINZ_STAC_UPDATED_KEY: first_asset_updated,
                        STAC_HREF_KEY: f"./{first_asset_filename}",
                        STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
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
                    second_asset_name: {
                        LINZ_STAC_CREATED_KEY: second_asset_created,
                        LINZ_STAC_UPDATED_KEY: second_asset_updated,
                        STAC_HREF_KEY: second_asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: second_asset_hex_digest,
                    }
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
                            DATASET_TITLE_KEY: dataset_title,
                            DESCRIPTION_KEY: any_dataset_description(),
                        },
                    }
                ).encode(),
            )
            dataset_payload = load(dataset_response["Payload"])
            dataset_id = dataset_payload[BODY_KEY][DATASET_ID_SHORT_KEY]

            first_dataset_versions_response = lambda_client.invoke(
                FunctionName=Resource.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.resource_name,
                Payload=dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            DATASET_ID_SHORT_KEY: dataset_id,
                            METADATA_URL_KEY: catalog_metadata_url,
                            S3_ROLE_ARN_KEY: get_s3_role_arn(),
                        },
                    }
                ).encode(),
            )
            first_dataset_versions_body = load(first_dataset_versions_response["Payload"])[BODY_KEY]
            first_dataset_version = first_dataset_versions_body[NEW_VERSION_ID_KEY]

            with subtests.test(msg="Should complete Step Function successfully"):

                # Then poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=first_dataset_versions_body[EXECUTION_ARN_KEY]
                    )
                )["status"] == "RUNNING":
                    sleep(5)  # pragma: no cover

                assert execution["status"] == "SUCCEEDED", execution

            assert (execution_output := execution.get("output")), execution

            account_id = get_account_number()

            first_import_dataset_response = loads(execution_output)[IMPORT_DATASET_KEY]
            first_metadata_copy_job_result, first_asset_copy_job_result = wait_for_copy_jobs(
                first_import_dataset_response, account_id, s3_control_client, subtests
            )

            #  Wait for Item metadata file to include a link to geostore root catalog
            expected_link_object = {
                STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
            }

            storage_bucket_prefix = f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/"

            while (
                expected_link_object
                not in (
                    load(
                        smart_open.open(
                            f"{storage_bucket_prefix}{dataset_title}/{item_metadata_filename}",
                            mode="rb",
                        )
                    )
                )[STAC_LINKS_KEY]
            ):
                sleep(5)  # pragma: no cover

            datasets_model = datasets_model_with_meta()
            dataset = datasets_model.get(
                hash_key=f"{DATASET_ID_PREFIX}{dataset_id}", consistent_read=True
            )
            assert dataset.current_dataset_version == first_dataset_version

            # delete some files in staging bucket and trigger a new version
            # to simulate a partial update\

            second_version_key_prefix = any_safe_file_path()
            second_version_metadata_url_prefix = (
                f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/"
                f"{second_version_key_prefix}"
            )
            second_version_catalog_url = (
                f"{second_version_metadata_url_prefix}/{catalog_metadata_filename}"
            )
            second_version_collection_url = (
                f"{second_version_metadata_url_prefix}/{collection_metadata_filename}"
            )
            second_version_item_url = (
                f"{second_version_metadata_url_prefix}/{item_metadata_filename}"
            )
            third_asset_contents = any_file_contents()
            third_asset_name = any_asset_name()
            third_asset_hex_digest = sha256_hex_digest_to_multihash(
                sha256(third_asset_contents).hexdigest()
            )
            third_asset_created = any_past_datetime_string()
            third_asset_updated = any_past_datetime_string()
            third_asset_filename = any_safe_filename()

            with S3Object(
                file_object=BytesIO(initial_bytes=third_asset_contents),
                bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
                key=f"{second_version_key_prefix}/{third_asset_filename}",
            ) as third_asset_s3_object, S3Object(
                file_object=json_dict_to_file_object(
                    {
                        **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                        STAC_ASSETS_KEY: {
                            third_asset_name: {
                                LINZ_STAC_CREATED_KEY: third_asset_created,
                                LINZ_STAC_UPDATED_KEY: third_asset_updated,
                                STAC_HREF_KEY: third_asset_s3_object.url,
                                STAC_FILE_CHECKSUM_KEY: third_asset_hex_digest,
                            },
                        },
                        STAC_LINKS_KEY: [
                            {
                                STAC_HREF_KEY: second_version_catalog_url,
                                STAC_REL_KEY: STAC_REL_ROOT,
                            },
                            {
                                STAC_HREF_KEY: second_version_collection_url,
                                STAC_REL_KEY: STAC_REL_PARENT,
                            },
                            {
                                STAC_HREF_KEY: second_version_item_url,
                                STAC_REL_KEY: STAC_REL_SELF,
                            },
                        ],
                    }
                ),
                bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
                key=f"{second_version_key_prefix}/{item_metadata_filename}",
            ):

                second_dataset_versions_response = lambda_client.invoke(
                    FunctionName=Resource.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.resource_name,
                    Payload=dumps(
                        {
                            HTTP_METHOD_KEY: "POST",
                            BODY_KEY: {
                                DATASET_ID_SHORT_KEY: dataset_id,
                                METADATA_URL_KEY: second_version_catalog_url,
                                S3_ROLE_ARN_KEY: get_s3_role_arn(),
                            },
                        }
                    ).encode(),
                )
                second_dataset_versions_body = load(second_dataset_versions_response["Payload"])[
                    BODY_KEY
                ]
                with subtests.test(msg="Should complete Step Function successfully"):

                    # Then poll for State Machine State
                    while (
                        execution := step_functions_client.describe_execution(
                            executionArn=second_dataset_versions_body[EXECUTION_ARN_KEY]
                        )
                    )["status"] == "RUNNING":
                        sleep(5)  # pragma: no cover

                    assert execution["status"] == "SUCCEEDED", execution

                assert (execution_output := execution.get("output")), execution

                second_import_dataset_response = loads(execution_output)[IMPORT_DATASET_KEY]
                second_metadata_copy_job_result, second_asset_copy_job_result = wait_for_copy_jobs(
                    second_import_dataset_response, account_id, s3_control_client, subtests
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
                                f"{storage_bucket_prefix}{dataset_title}/{item_metadata_filename}",
                                mode="rb",
                            )
                        )
                    )[STAC_LINKS_KEY]
                ):
                    sleep(5)  # pragma: no cover

                # Catalog contents
                imported_catalog_key = f"{dataset_title}/{catalog_metadata_filename}"
                with subtests.test(msg="Imported catalog has relative keys"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_catalog_key}", mode="rb"
                ) as imported_catalog_file:
                    assert load(imported_catalog_file) == {
                        **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                        STAC_EXTENSIONS_KEY: [],  # pystac writes this field.
                        STAC_TITLE_KEY: dataset_title,
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
                imported_collection_key = f"{dataset_title}/{collection_metadata_filename}"
                with subtests.test(msg="Imported collection has relative keys"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_collection_key}", mode="rb"
                ) as imported_collection_file:
                    assert load(imported_collection_file) == {
                        **collection_dict,
                        STAC_ASSETS_KEY: {
                            first_asset_name: {
                                LINZ_STAC_CREATED_KEY: first_asset_created,
                                LINZ_STAC_UPDATED_KEY: first_asset_updated,
                                STAC_HREF_KEY: first_asset_filename,
                                STAC_FILE_CHECKSUM_KEY: first_asset_hex_digest,
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
                                STAC_TITLE_KEY: dataset_title,
                                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                            },
                        ],
                    }

                # Item contents
                imported_item_key = f"{dataset_title}/{item_metadata_filename}"
                with subtests.test(msg="Imported item has correct links"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_item_key}", mode="rb"
                ) as imported_item_file:
                    assert load(imported_item_file) == {
                        **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                        STAC_ASSETS_KEY: {
                            third_asset_name: {
                                LINZ_STAC_CREATED_KEY: third_asset_created,
                                LINZ_STAC_UPDATED_KEY: third_asset_updated,
                                STAC_HREF_KEY: f"./{third_asset_filename}",
                                STAC_FILE_CHECKSUM_KEY: third_asset_hex_digest,
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
                imported_first_asset_key = f"{dataset_title}/{first_asset_filename}"
                with subtests.test(msg="Verify first asset contents"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_first_asset_key}", mode="rb"
                ) as imported_first_asset_file:
                    assert imported_first_asset_file.read() == first_asset_contents

                # Second asset contents
                imported_second_asset_key = f"{dataset_title}/{second_asset_filename}"
                with subtests.test(msg="Verify second asset contents"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_second_asset_key}", mode="rb"
                ) as second_asset_file:
                    assert second_asset_file.read() == second_asset_contents

                # Third asset contents
                imported_third_asset_key = f"{dataset_title}/{third_asset_filename}"
                with subtests.test(msg="Verify third asset contents"), smart_open.open(
                    f"{storage_bucket_prefix}{imported_third_asset_key}", mode="rb"
                ) as third_asset_file:
                    assert third_asset_file.read() == third_asset_contents

        finally:
            # Cleanup

            for key in [
                CATALOG_FILENAME,
                imported_catalog_key,
                imported_collection_key,
                imported_item_key,
                imported_first_asset_key,
                imported_second_asset_key,
                imported_third_asset_key,
            ]:
                with subtests.test(msg=f"Delete {key}"):
                    delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, key, s3_client)

            with subtests.test(msg="Delete first version copy job files"):
                assert first_metadata_copy_job_result is not None
                assert first_asset_copy_job_result is not None
                delete_copy_job_files(
                    first_metadata_copy_job_result,
                    first_asset_copy_job_result,
                    Resource.STORAGE_BUCKET_NAME.resource_name,
                    s3_client,
                    subtests,
                )
            with subtests.test(msg="Delete second version copy job files"):
                assert second_metadata_copy_job_result is not None
                assert second_asset_copy_job_result is not None
                delete_copy_job_files(
                    second_metadata_copy_job_result,
                    second_asset_copy_job_result,
                    Resource.STORAGE_BUCKET_NAME.resource_name,
                    s3_client,
                    subtests,
                )


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
                        DATASET_TITLE_KEY: dataset_title,
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
    for filename in [metadata_filename, asset_filename]:
        with subtests.test(msg=filename), raises(AssertionError):
            delete_s3_key(
                Resource.STORAGE_BUCKET_NAME.resource_name,
                f"{dataset_title}/{filename}",
                s3_client,
            )
