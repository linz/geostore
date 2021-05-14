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
from backend.import_status.get import ERRORS_KEY, IMPORT_DATASET_KEY, Outcome
from backend.parameter_store import ParameterName
from backend.resources import ResourceName
from backend.step_function_event_keys import (
    ASSET_UPLOAD_KEY,
    EXECUTION_ARN_KEY,
    METADATA_UPLOAD_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
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
    """Test if Data Lake State Machine ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(
        Name=ParameterName.PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN.value
    )
    assert (
        parameter_response["Parameter"]["Name"]
        == ParameterName.PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN.value
    )
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "stateMachine" in parameter_response["Parameter"]["Value"]


@mark.infrastructure
def should_check_s3_batch_copy_role_arn_parameter_exists(ssm_client: SSMClient) -> None:
    """Test if Data Lake S3 Batch Copy Role ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(
        Name=ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN.value
    )
    assert (
        parameter_response["Parameter"]["Name"]
        == ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN.value
    )
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "iam" in parameter_response["Parameter"]["Value"]


class TestWithStagingBucket:
    staging_bucket_name: str
    storage_bucket_name: str

    @classmethod
    def setup_class(cls) -> None:
        cls.staging_bucket_name = ResourceName.STAGING_BUCKET_NAME.value
        cls.storage_bucket_name = ResourceName.STORAGE_BUCKET_NAME.value

    @mark.timeout(1200)
    @mark.infrastructure
    def should_successfully_run_dataset_version_creation_process_with_multiple_assets(
        # pylint:disable=too-many-arguments
        self,
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

        collection_metadata_url = (
            f"s3://{self.staging_bucket_name}/{key_prefix}/{collection_metadata_filename}"
        )
        catalog_metadata_url = (
            f"s3://{self.staging_bucket_name}/{key_prefix}/{catalog_metadata_filename}"
        )
        item_metadata_url = f"s3://{self.staging_bucket_name}/{key_prefix}/{item_metadata_filename}"

        first_asset_contents = any_file_contents()
        first_asset_filename = any_safe_filename()
        second_asset_contents = any_file_contents()
        second_asset_filename = any_safe_filename()

        with S3Object(
            file_object=BytesIO(initial_bytes=first_asset_contents),
            bucket_name=self.staging_bucket_name,
            key=f"{key_prefix}/{first_asset_filename}",
        ) as first_asset_s3_object, S3Object(
            file_object=BytesIO(initial_bytes=second_asset_contents),
            bucket_name=self.staging_bucket_name,
            key=f"{key_prefix}/{second_asset_filename}",
        ) as second_asset_s3_object, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                    "links": [
                        {"href": collection_metadata_url, "rel": "child"},
                        {"href": catalog_metadata_url, "rel": "root"},
                        {"href": catalog_metadata_url, "rel": "self"},
                    ],
                }
            ),
            bucket_name=self.staging_bucket_name,
            key=f"{key_prefix}/{catalog_metadata_filename}",
        ) as catalog_metadata_file, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    "assets": {
                        any_asset_name(): {
                            "href": second_asset_s3_object.url,
                            "file:checksum": sha256_hex_digest_to_multihash(
                                sha256(second_asset_contents).hexdigest()
                            ),
                        },
                    },
                    "links": [
                        {"href": item_metadata_url, "rel": "child"},
                        {"href": catalog_metadata_url, "rel": "root"},
                        {"href": collection_metadata_url, "rel": "self"},
                    ],
                }
            ),
            bucket_name=self.staging_bucket_name,
            key=f"{key_prefix}/{collection_metadata_filename}",
        ), S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                    "assets": {
                        any_asset_name(): {
                            "href": first_asset_s3_object.url,
                            "file:checksum": sha256_hex_digest_to_multihash(
                                sha256(first_asset_contents).hexdigest()
                            ),
                        },
                    },
                    "links": [
                        {"href": catalog_metadata_url, "rel": "root"},
                        {"href": item_metadata_url, "rel": "self"},
                    ],
                }
            ),
            bucket_name=self.staging_bucket_name,
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
                                "id": dataset.dataset_id,
                                "metadata_url": catalog_metadata_file.url,
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
                        LOGGER.info("Polling for State Machine state %s", "." * 6)
                        time.sleep(5)

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
                    new_key = f"{dataset_prefix}/{json_resp['body']['dataset_version']}/{filename}"
                    with subtests.test(msg=f"Delete {new_key}"):
                        delete_s3_key(self.storage_bucket_name, new_key, s3_client)

                storage_bucket_name = self.storage_bucket_name
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
        self,
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
            bucket_name=self.staging_bucket_name,
            key=f"{key_prefix}/{asset_filename}",
        ) as asset_s3_object, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    "assets": {
                        any_asset_name(): {
                            "href": asset_s3_object.url,
                            "file:checksum": sha256_hex_digest_to_multihash(
                                sha256(asset_contents).hexdigest()
                            ),
                        },
                    },
                }
            ),
            bucket_name=self.staging_bucket_name,
            key=("{}/{}".format(key_prefix, child_metadata_filename)),
        ) as child_metadata_file, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    "links": [
                        {"href": f"{child_metadata_file.url}", "rel": "child"},
                    ],
                }
            ),
            bucket_name=self.staging_bucket_name,
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
                                "id": dataset.dataset_id,
                                "metadata_url": root_metadata_file.url,
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
                        LOGGER.info("Polling for State Machine state %s", "." * 6)
                        time.sleep(5)

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
                    new_key = f"{dataset_prefix}/{json_resp['body']['dataset_version']}/{filename}"
                    with subtests.test(msg=f"Delete {new_key}"):
                        delete_s3_key(self.storage_bucket_name, new_key, s3_client)

                delete_copy_job_files(
                    metadata_copy_job_result,
                    asset_copy_job_result,
                    self.storage_bucket_name,
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
        self,
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
            bucket_name=self.staging_bucket_name,
            key=f"{key_prefix}/{asset_filename}",
        ) as asset_s3_object, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    "assets": {
                        any_asset_name(): {
                            "href": asset_s3_object.url,
                            "file:checksum": any_hex_multihash(),
                        },
                    },
                }
            ),
            bucket_name=self.staging_bucket_name,
            key=f"{key_prefix}/{metadata_filename}",
        ) as s3_metadata_file, Dataset() as dataset:

            # When creating a dataset version
            dataset_version_creation_response = lambda_client.invoke(
                FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                Payload=json.dumps(
                    {
                        HTTP_METHOD_KEY: "POST",
                        BODY_KEY: {"id": dataset.dataset_id, "metadata_url": s3_metadata_file.url},
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
                    LOGGER.info("Polling for State Machine %s state", state_machine_arn)
                    time.sleep(5)

                assert execution["status"] == "SUCCEEDED", execution

        # Then the files should not be copied
        dataset_version = response_payload[BODY_KEY]["dataset_version"]
        dataset_prefix = f"{dataset.title}{DATASET_KEY_SEPARATOR}{dataset.dataset_id}"
        for filename in [metadata_filename, asset_filename]:
            with subtests.test(msg=filename), raises(AssertionError):
                delete_s3_key(
                    self.storage_bucket_name,
                    f"{dataset_prefix}/{dataset_version}/{filename}",
                    s3_client,
                )
