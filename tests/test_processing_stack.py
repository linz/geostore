import json
import logging
import time
from copy import deepcopy
from hashlib import sha256
from http import HTTPStatus
from io import BytesIO

from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from mypy_boto3_sts import STSClient
from pytest import mark, raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.api_keys import STATUS_KEY
from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from backend.datasets_model import DATASET_KEY_SEPARATOR
from backend.parameter_store import ParameterName
from backend.resources import ResourceName
from backend.s3 import S3_URL_PREFIX
from backend.stac_format import (
    STAC_ASSETS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_LINKS_KEY,
    STAC_REL_CHILD,
    STAC_REL_KEY,
    STAC_REL_ROOT,
    STAC_REL_SELF,
)
from backend.step_function import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_SHORT_KEY,
    ERRORS_KEY,
    EXECUTION_ARN_KEY,
    IMPORT_DATASET_KEY,
    METADATA_UPLOAD_KEY,
    METADATA_URL_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
    Outcome,
)

from .aws_utils import (
    S3_BATCH_JOB_COMPLETED_STATE,
    Dataset,
    S3Object,
    delete_copy_job_files,
    delete_s3_key,
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

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


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
def should_successfully_run_dataset_version_creation_process_with_multiple_assets(
    # pylint:disable=too-many-arguments
    step_functions_client: SFNClient,
    lambda_client: LambdaClient,
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    sts_client: STSClient,
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
    second_asset_contents = any_file_contents()
    second_asset_filename = any_safe_filename()

    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_contents),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{first_asset_filename}",
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_contents),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{second_asset_filename}",
    ) as second_asset_s3_object, S3Object(
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
                    any_asset_name(): {
                        STAC_HREF_KEY: second_asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: sha256_hex_digest_to_multihash(
                            sha256(second_asset_contents).hexdigest()
                        ),
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
                    any_asset_name(): {
                        STAC_HREF_KEY: first_asset_s3_object.url,
                        STAC_FILE_CHECKSUM_KEY: sha256_hex_digest_to_multihash(
                            sha256(first_asset_contents).hexdigest()
                        ),
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
            resp = lambda_client.invoke(
                FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                Payload=json.dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            DATASET_ID_SHORT_KEY: dataset.dataset_id,
                            METADATA_URL_KEY: catalog_metadata_file.url,
                        },
                    }
                ).encode(),
            )
            json_resp = json.load(resp["Payload"])

            with subtests.test(msg="Dataset Versions endpoint returns success"):
                assert json_resp.get(STATUS_CODE_KEY) == HTTPStatus.CREATED, json_resp

            with subtests.test(msg="Should complete Step Function successfully"):

                LOGGER.info("Executed State Machine: %s", json_resp)

                # Then poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=json_resp[BODY_KEY][EXECUTION_ARN_KEY]
                    )
                )["status"] == "RUNNING":
                    LOGGER.info("Polling for State Machine state %s", "." * 6)  # pragma: no cover
                    time.sleep(5)  # pragma: no cover

                assert execution["status"] == "SUCCEEDED", execution

            assert (execution_output := execution.get("output")), execution

            account_id = sts_client.get_caller_identity()["Account"]

            import_dataset_response = json.loads(execution_output)[IMPORT_DATASET_KEY]
            metadata_copy_job_result, asset_copy_job_result = wait_for_copy_jobs(
                import_dataset_response, account_id, s3_control_client, subtests
            )
        finally:
            # Cleanup
            dataset_prefix = f"{dataset.title}{DATASET_KEY_SEPARATOR}{dataset.dataset_id}"
            for filename in [
                catalog_metadata_filename,
                collection_metadata_filename,
                item_metadata_filename,
                first_asset_filename,
                second_asset_filename,
            ]:
                new_key = f"{dataset_prefix}/{json_resp[BODY_KEY][VERSION_ID_KEY]}/{filename}"
                with subtests.test(msg=f"Delete {new_key}"):
                    delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, new_key, s3_client)

            storage_bucket_name = ResourceName.STORAGE_BUCKET_NAME.value
            delete_copy_job_files(
                metadata_copy_job_result,
                asset_copy_job_result,
                storage_bucket_name,
                s3_client,
                subtests,
            )

    with subtests.test(msg="Should report import status after success"):
        expected_response = {
            STATUS_CODE_KEY: HTTPStatus.OK,
            BODY_KEY: {
                STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
                VALIDATION_KEY: {STATUS_KEY: Outcome.PASSED.value, ERRORS_KEY: []},
                METADATA_UPLOAD_KEY: {STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE, ERRORS_KEY: []},
                ASSET_UPLOAD_KEY: {STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE, ERRORS_KEY: []},
            },
        }
        status_resp = lambda_client.invoke(
            FunctionName=ResourceName.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.value,
            Payload=json.dumps(
                {
                    HTTP_METHOD_KEY: "GET",
                    BODY_KEY: {EXECUTION_ARN_KEY: execution["executionArn"]},
                }
            ).encode(),
        )
        status_json_resp = json.load(status_resp["Payload"])
        assert status_json_resp == expected_response


@mark.timeout(1200)
@mark.infrastructure
def should_successfully_run_dataset_version_creation_process_with_single_asset(
    # pylint:disable=too-many-arguments
    step_functions_client: SFNClient,
    lambda_client: LambdaClient,
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    sts_client: STSClient,
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
        key=("{}/{}".format(key_prefix, child_metadata_filename)),
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
        key=("{}/{}".format(key_prefix, root_metadata_filename)),
    ) as root_metadata_file, Dataset() as dataset:

        # When
        try:
            resp = lambda_client.invoke(
                FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                Payload=json.dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {
                            DATASET_ID_SHORT_KEY: dataset.dataset_id,
                            METADATA_URL_KEY: root_metadata_file.url,
                        },
                    }
                ).encode(),
            )
            json_resp = json.load(resp["Payload"])

            with subtests.test(msg="Dataset Versions endpoint returns success"):
                assert json_resp.get(STATUS_CODE_KEY) == HTTPStatus.CREATED, json_resp

            with subtests.test(msg="Should complete Step Function successfully"):

                LOGGER.info("Executed State Machine: %s", json_resp)

                # Then poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=json_resp[BODY_KEY][EXECUTION_ARN_KEY]
                    )
                )["status"] == "RUNNING":
                    LOGGER.info("Polling for State Machine state %s", "." * 6)  # pragma: no cover
                    time.sleep(5)  # pragma: no cover

            assert (execution_output := execution.get("output")), execution

            account_id = sts_client.get_caller_identity()["Account"]

            import_dataset_response = json.loads(execution_output)[IMPORT_DATASET_KEY]
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
                new_key = f"{dataset_prefix}/{json_resp[BODY_KEY][VERSION_ID_KEY]}/{filename}"
                with subtests.test(msg=f"Delete {new_key}"):
                    delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, new_key, s3_client)

            delete_copy_job_files(
                metadata_copy_job_result,
                asset_copy_job_result,
                ResourceName.STORAGE_BUCKET_NAME.value,
                s3_client,
                subtests,
            )

    with subtests.test(msg="Should report import status after success"):
        expected_response = {
            STATUS_CODE_KEY: HTTPStatus.OK,
            BODY_KEY: {
                STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
                VALIDATION_KEY: {STATUS_KEY: Outcome.PASSED.value, ERRORS_KEY: []},
                METADATA_UPLOAD_KEY: {STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE, ERRORS_KEY: []},
                ASSET_UPLOAD_KEY: {STATUS_KEY: S3_BATCH_JOB_COMPLETED_STATE, ERRORS_KEY: []},
            },
        }
        status_resp = lambda_client.invoke(
            FunctionName=ResourceName.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.value,
            Payload=json.dumps(
                {
                    HTTP_METHOD_KEY: "GET",
                    BODY_KEY: {EXECUTION_ARN_KEY: execution["executionArn"]},
                }
            ).encode(),
        )
        status_json_resp = json.load(status_resp["Payload"])
        assert status_json_resp == expected_response


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
            Payload=json.dumps(
                {
                    HTTP_METHOD_KEY: "POST",
                    BODY_KEY: {
                        DATASET_ID_SHORT_KEY: dataset.dataset_id,
                        METADATA_URL_KEY: s3_metadata_file.url,
                    },
                }
            ).encode(),
        )

        response_payload = json.load(dataset_version_creation_response["Payload"])
        with subtests.test(msg="Dataset Versions endpoint status code"):
            assert response_payload.get(STATUS_CODE_KEY) == HTTPStatus.CREATED, response_payload

        with subtests.test(msg="Step function result"):
            # Then poll for State Machine State
            state_machine_arn = response_payload[BODY_KEY][EXECUTION_ARN_KEY]
            while (
                execution := step_functions_client.describe_execution(
                    executionArn=state_machine_arn
                )
            )["status"] == "RUNNING":
                LOGGER.info(  # pragma: no cover
                    "Polling for State Machine %s state", state_machine_arn
                )
                time.sleep(5)  # pragma: no cover

            assert execution["status"] == "SUCCEEDED", execution

    # Then the files should not be copied
    dataset_version = response_payload[BODY_KEY][VERSION_ID_KEY]
    dataset_prefix = f"{dataset.title}{DATASET_KEY_SEPARATOR}{dataset.dataset_id}"
    for filename in [metadata_filename, asset_filename]:
        with subtests.test(msg=filename), raises(AssertionError):
            delete_s3_key(
                ResourceName.STORAGE_BUCKET_NAME.value,
                f"{dataset_prefix}/{dataset_version}/{filename}",
                s3_client,
            )
