import json
import logging
import time
from copy import deepcopy
from hashlib import sha256
from io import BytesIO

from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from mypy_boto3_sts import STSClient
from pytest import mark, raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.dataset_versions.create import DATASET_VERSION_CREATION_STEP_FUNCTION
from backend.import_dataset.task import S3_BATCH_COPY_ROLE_PARAMETER_NAME
from backend.resources import ResourceName

from .aws_utils import (
    MINIMAL_VALID_STAC_OBJECT,
    Dataset,
    S3Object,
    delete_s3_key,
    delete_s3_prefix,
    s3_object_arn_to_key,
)
from .file_utils import json_dict_to_file_object
from .general_generators import any_file_contents, any_safe_file_path, any_safe_filename
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_hex_multihash,
    any_valid_dataset_type,
    sha256_hex_digest_to_multihash,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@mark.infrastructure
def should_check_state_machine_arn_parameter_exists(ssm_client: SSMClient) -> None:
    """Test if Data Lake State Machine ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(Name=DATASET_VERSION_CREATION_STEP_FUNCTION)
    assert parameter_response["Parameter"]["Name"] == DATASET_VERSION_CREATION_STEP_FUNCTION
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "stateMachine" in parameter_response["Parameter"]["Value"]


@mark.infrastructure
def should_check_s3_batch_copy_role_arn_parameter_exists(ssm_client: SSMClient) -> None:
    """Test if Data Lake S3 Batch Copy Role ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(Name=S3_BATCH_COPY_ROLE_PARAMETER_NAME)
    assert parameter_response["Parameter"]["Name"] == S3_BATCH_COPY_ROLE_PARAMETER_NAME
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "iam" in parameter_response["Parameter"]["Value"]


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
    metadata = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    first_asset_contents = any_file_contents()
    second_asset_contents = any_file_contents()

    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_contents),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{any_safe_filename()}.txt",
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_contents),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{any_safe_filename()}.txt",
    ) as second_asset_s3_object:
        metadata["assets"] = {
            any_asset_name(): {
                "href": first_asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(first_asset_contents).hexdigest()
                ),
            },
            any_asset_name(): {
                "href": second_asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(second_asset_contents).hexdigest()
                ),
            },
        }

        with S3Object(
            file_object=json_dict_to_file_object(metadata),
            bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
            key=("{}/{}.json".format(key_prefix, any_safe_filename())),
        ) as s3_metadata_file:
            dataset_id = any_dataset_id()
            dataset_type = any_valid_dataset_type()
            with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

                # When
                resp = lambda_client.invoke(
                    FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                    Payload=json.dumps(
                        {
                            "httpMethod": "POST",
                            "body": {
                                "id": dataset_id,
                                "metadata-url": s3_metadata_file.url,
                                "type": dataset_type,
                            },
                        }
                    ).encode(),
                    InvocationType="RequestResponse",
                )
                json_resp = json.load(resp["Payload"])

                with subtests.test(msg="Dataset Versions endpoint returns success"):
                    assert json_resp.get("statusCode") == 201, json_resp

                with subtests.test(msg="Should complete Step Function successfully"):

                    logger.info("Executed State Machine: %s", json_resp)

                    # Then poll for State Machine State
                    while (
                        execution := step_functions_client.describe_execution(
                            executionArn=json_resp["body"]["execution_arn"]
                        )
                    )["status"] == "RUNNING":
                        logger.info("Polling for State Machine state %s", "." * 6)
                        time.sleep(5)

                    assert execution["status"] == "SUCCEEDED", execution

                with subtests.test(msg="Should complete S3 batch copy operation successfully"):

                    s3_batch_copy_arn = json.loads(execution["output"])["s3_batch_copy"]["job_id"]
                    final_states = ["Complete", "Failed", "Cancelled"]

                    # poll for S3 Batch Copy completion
                    while (
                        copy_job := s3_control_client.describe_job(
                            AccountId=sts_client.get_caller_identity()["Account"],
                            JobId=s3_batch_copy_arn,
                        )
                    )["Job"]["Status"] not in final_states:
                        time.sleep(5)

                    assert copy_job["Job"]["Status"] == "Complete", copy_job

                    # Cleanup
                    for key in [
                        s3_metadata_file.key,
                        first_asset_s3_object.key,
                        second_asset_s3_object.key,
                    ]:
                        delete_s3_key(
                            ResourceName.STORAGE_BUCKET_NAME.value,
                            f"{dataset_id}/{json_resp['body']['dataset_version']}/{key}",
                            s3_client,
                        )

                    delete_s3_key(
                        ResourceName.STORAGE_BUCKET_NAME.value,
                        s3_object_arn_to_key(copy_job["Job"]["Manifest"]["Location"]["ObjectArn"]),
                        s3_client,
                    )

                    delete_s3_prefix(
                        ResourceName.STORAGE_BUCKET_NAME.value,
                        copy_job["Job"]["Report"]["Prefix"],
                        s3_client,
                    )

                with subtests.test(msg="Should report import status after success"):
                    expected_response = {
                        "statusCode": 200,
                        "body": {
                            "validation": {"status": "SUCCEEDED", "errors": []},
                            "upload": {"status": "Complete", "errors": []},
                        },
                    }
                    status_resp = lambda_client.invoke(
                        FunctionName=ResourceName.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.value,
                        Payload=json.dumps(
                            {
                                "httpMethod": "GET",
                                "body": {"execution_arn": execution["executionArn"]},
                            }
                        ).encode(),
                        InvocationType="RequestResponse",
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
    metadata = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    asset_contents = any_file_contents()

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{any_safe_filename()}.txt",
    ) as asset_s3_object:
        metadata["assets"] = {
            any_asset_name(): {
                "href": asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(asset_contents).hexdigest()
                ),
            },
        }

        with S3Object(
            file_object=json_dict_to_file_object(metadata),
            bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
            key=("{}/{}.json".format(key_prefix, any_safe_filename())),
        ) as s3_metadata_file:
            dataset_id = any_dataset_id()
            dataset_type = any_valid_dataset_type()
            with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

                # When
                resp = lambda_client.invoke(
                    FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                    Payload=json.dumps(
                        {
                            "httpMethod": "POST",
                            "body": {
                                "id": dataset_id,
                                "metadata-url": s3_metadata_file.url,
                                "type": dataset_type,
                            },
                        }
                    ).encode(),
                    InvocationType="RequestResponse",
                )
                json_resp = json.load(resp["Payload"])

                with subtests.test(msg="Dataset Versions endpoint returns success"):
                    assert json_resp.get("statusCode") == 201, json_resp

                with subtests.test(msg="Should complete Step Function successfully"):

                    logger.info("Executed State Machine: %s", json_resp)

                    # Then poll for State Machine State
                    while (
                        execution := step_functions_client.describe_execution(
                            executionArn=json_resp["body"]["execution_arn"]
                        )
                    )["status"] == "RUNNING":
                        logger.info("Polling for State Machine state %s", "." * 6)
                        time.sleep(5)

                    assert execution["status"] == "SUCCEEDED", execution

                with subtests.test(msg="Should complete S3 batch copy operation successfully"):

                    s3_batch_copy_arn = json.loads(execution["output"])["s3_batch_copy"]["job_id"]
                    final_states = ["Complete", "Failed", "Cancelled"]

                    # poll for S3 Batch Copy completion
                    while (
                        copy_job := s3_control_client.describe_job(
                            AccountId=sts_client.get_caller_identity()["Account"],
                            JobId=s3_batch_copy_arn,
                        )
                    )["Job"]["Status"] not in final_states:
                        time.sleep(5)

                    assert copy_job["Job"]["Status"] == "Complete", copy_job

                    # Cleanup
                    for key in [s3_metadata_file.key, asset_s3_object.key]:
                        delete_s3_key(
                            ResourceName.STORAGE_BUCKET_NAME.value,
                            f"{dataset_id}/{json_resp['body']['dataset_version']}/{key}",
                            s3_client,
                        )

                    delete_s3_key(
                        ResourceName.STORAGE_BUCKET_NAME.value,
                        s3_object_arn_to_key(copy_job["Job"]["Manifest"]["Location"]["ObjectArn"]),
                        s3_client,
                    )

                    delete_s3_prefix(
                        ResourceName.STORAGE_BUCKET_NAME.value,
                        copy_job["Job"]["Report"]["Prefix"],
                        s3_client,
                    )

                with subtests.test(msg="Should report import status after success"):
                    expected_response = {
                        "statusCode": 200,
                        "body": {
                            "validation": {"status": "SUCCEEDED", "errors": []},
                            "upload": {"status": "Complete", "errors": []},
                        },
                    }
                    status_resp = lambda_client.invoke(
                        FunctionName=ResourceName.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.value,
                        Payload=json.dumps(
                            {
                                "httpMethod": "GET",
                                "body": {"execution_arn": execution["executionArn"]},
                            }
                        ).encode(),
                        InvocationType="RequestResponse",
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
    # Given an asset with an invalid checksum
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    key_prefix = any_safe_file_path()

    with S3Object(
        file_object=BytesIO(),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{any_safe_filename()}.txt",
    ) as asset_s3_object:
        metadata = {
            **deepcopy(MINIMAL_VALID_STAC_OBJECT),
            "assets": {
                any_asset_name(): {
                    "href": asset_s3_object.url,
                    "checksum:multihash": any_hex_multihash(),
                },
            },
        }

        with S3Object(
            file_object=json_dict_to_file_object(metadata),
            bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
            key=f"{key_prefix}/{any_safe_filename()}.json",
        ) as s3_metadata_file, Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

            # When creating a dataset version
            dataset_version_creation_response = lambda_client.invoke(
                FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
                Payload=json.dumps(
                    {
                        "httpMethod": "POST",
                        "body": {
                            "id": dataset_id,
                            "metadata-url": s3_metadata_file.url,
                            "type": dataset_type,
                        },
                    }
                ).encode(),
                InvocationType="RequestResponse",
            )

            response_payload = json.load(dataset_version_creation_response["Payload"])
            with subtests.test(msg="Dataset Versions endpoint status code"):
                assert response_payload.get("statusCode") == 201, response_payload

            with subtests.test(msg="Step function result"):
                # Then poll for State Machine State
                state_machine_arn = response_payload["body"]["execution_arn"]
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=state_machine_arn
                    )
                )["status"] == "RUNNING":
                    logger.info("Polling for State Machine %s state", state_machine_arn)
                    time.sleep(5)

                assert execution["status"] == "SUCCEEDED", execution

    # Then the files should not be copied
    for key in [s3_metadata_file.key, asset_s3_object.key]:
        with subtests.test(msg=key), raises(AssertionError):
            delete_s3_key(
                ResourceName.STORAGE_BUCKET_NAME.value,
                f"{dataset_id}/{response_payload['body']['dataset_version']}/{key}",
                s3_client,
            )
