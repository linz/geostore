import json
import logging
import time
from contextlib import nullcontext
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from json import dumps
from typing import ContextManager, Optional

import _pytest
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from mypy_boto3_sts import STSClient
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.dataset_versions.create import DATASET_VERSION_CREATION_STEP_FUNCTION
from backend.import_dataset.task import S3_BATCH_COPY_ROLE_PARAMETER_NAME
from backend.resources import ResourceName

from .aws_utils import MINIMAL_VALID_STAC_OBJECT, Dataset, S3Object
from .general_generators import (
    any_boolean,
    any_file_contents,
    any_safe_file_path,
    any_safe_filename,
)
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
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
def should_successfully_run_dataset_version_creation_process(
    # pylint:disable=too-many-arguments
    step_functions_client: SFNClient,
    lambda_client: LambdaClient,
    s3_control_client: S3ControlClient,
    sts_client: STSClient,
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
    processing_assets_db_teardown: _pytest.fixtures.FixtureDef[
        object
    ],  # pylint:disable=unused-argument
    storage_bucket_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
    subtests: SubTests,
) -> None:
    # pylint: disable=too-many-locals
    key_prefix = any_safe_file_path()
    metadata = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    mandatory_asset_contents = any_file_contents()

    # Test either branch of check_files_checksums_maybe_array randomly. Trade-off between cost of
    # running test (~2m) and coverage.
    optional_asset: ContextManager[Optional[S3Object]]
    if any_boolean():
        optional_asset_contents = any_file_contents()
        optional_asset = S3Object(
            file_object=BytesIO(initial_bytes=optional_asset_contents),
            bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
            key=f"{key_prefix}/{any_safe_filename()}.txt",
        )
    else:
        optional_asset = nullcontext()

    with S3Object(
        file_object=BytesIO(initial_bytes=mandatory_asset_contents),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=f"{key_prefix}/{any_safe_filename()}.txt",
    ) as mandatory_asset_s3_object, optional_asset as optional_asset_s3_object:
        metadata["item_assets"] = {
            any_asset_name(): {
                "href": mandatory_asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(mandatory_asset_contents).hexdigest()
                ),
            },
        }
        if optional_asset_s3_object is not None:
            metadata["item_assets"][any_asset_name()] = {
                "href": optional_asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(optional_asset_contents).hexdigest()
                ),
            }

        with S3Object(
            file_object=BytesIO(initial_bytes=dumps(metadata).encode()),
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

                assert json_resp.get("statusCode") == 201, json_resp

                # When

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

            with subtests.test(msg="Import Status Endpoint"):
                status_resp = lambda_client.invoke(
                    FunctionName=ResourceName.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.value,
                    Payload=json.dumps(
                        {"httpMethod": "GET", "body": {"execution_arn": execution["executionArn"]}}
                    ).encode(),
                    InvocationType="RequestResponse",
                )
                status_json_resp = json.load(status_resp["Payload"])
                assert status_json_resp["statusCode"] == 200, status_json_resp
                assert status_json_resp["body"]["validation"]["status"] == "SUCCEEDED"
                assert status_json_resp["body"]["upload"]["status"] == "Complete"
