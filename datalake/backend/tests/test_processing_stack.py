import logging
import time
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from json import dumps

from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from pytest import mark

from ..endpoints.dataset_versions import entrypoint
from ..endpoints.dataset_versions.create import DATASET_VERSION_CREATION_STEP_FUNCTION
from ..endpoints.utils import ResourceName
from .utils import (
    EMPTY_FILE_MULTIHASH,
    MINIMAL_VALID_STAC_OBJECT,
    Dataset,
    S3Object,
    any_dataset_id,
    any_file_contents,
    any_lambda_context,
    any_safe_file_path,
    any_safe_filename,
    any_stac_asset_name,
    any_valid_dataset_type,
    sha256_hex_digest_to_multihash,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@mark.infrastructure
def test_should_create_state_machine_arn_parameter(ssm_client: SSMClient) -> None:
    """Test if Data Lake State Machine ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(Name=DATASET_VERSION_CREATION_STEP_FUNCTION)
    assert parameter_response["Parameter"]["Name"] == DATASET_VERSION_CREATION_STEP_FUNCTION
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "stateMachine" in parameter_response["Parameter"]["Value"]


@mark.timeout(1200)
@mark.infrastructure
def test_should_successfully_run_dataset_version_creation_process_with_a_single_asset(
    step_functions_client: SFNClient,
) -> None:
    key_prefix = any_safe_file_path()
    metadata_object_key = "{}/{}.json".format(key_prefix, any_safe_filename())
    metadata = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    s3_bucket_name = ResourceName.DATASET_STAGING_BUCKET_NAME.value

    with S3Object(
        BytesIO(initial_bytes=b""),
        s3_bucket_name,
        f"{key_prefix}/{any_safe_filename()}.tif",
    ) as asset_s3_object:
        metadata["assets"] = {
            any_stac_asset_name(): {
                "href": asset_s3_object.url,
                "checksum:multihash": EMPTY_FILE_MULTIHASH,
            },
        }

        with S3Object(
            BytesIO(initial_bytes=dumps(metadata).encode()),
            s3_bucket_name,
            metadata_object_key,
        ) as s3_metadata_file:
            dataset_id = any_dataset_id()
            dataset_type = any_valid_dataset_type()
            with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

                body = {}
                body["id"] = dataset_id
                body["metadata-url"] = s3_metadata_file.url
                body["type"] = dataset_type

                launch_response = entrypoint.lambda_handler(
                    {"httpMethod": "POST", "body": body}, any_lambda_context()
                )["body"]
                logger.info("Executed State Machine: %s", launch_response)

                # poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=launch_response["execution_arn"]
                    )
                )["status"] == "RUNNING":
                    logger.info("Polling for State Machine state %s", "." * 6)
                    time.sleep(5)

                assert execution["status"] == "SUCCEEDED", execution


@mark.timeout(1200)
@mark.infrastructure
def test_should_successfully_run_dataset_version_creation_process_via_array_job(
    step_functions_client: SFNClient,
) -> None:

    metadata_object_key = "{}/{}.json".format(any_safe_file_path(), any_safe_filename())
    metadata = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    s3_bucket_name = ResourceName.DATASET_STAGING_BUCKET_NAME.value

    first_asset_contents = any_file_contents()
    second_asset_contents = any_file_contents()
    with S3Object(
        BytesIO(initial_bytes=first_asset_contents),
        s3_bucket_name,
        f"{any_safe_file_path()}/{any_safe_filename()}.txt",
    ) as first_asset_s3_object, S3Object(
        BytesIO(initial_bytes=second_asset_contents),
        s3_bucket_name,
        f"{any_safe_file_path()}/{any_safe_filename()}.txt",
    ) as second_asset_s3_object:
        metadata["assets"] = {
            any_stac_asset_name(): {
                "href": first_asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(first_asset_contents).hexdigest()
                ),
            },
            any_stac_asset_name(): {
                "href": second_asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(second_asset_contents).hexdigest()
                ),
            },
        }

        with S3Object(
            BytesIO(initial_bytes=dumps(metadata).encode()),
            s3_bucket_name,
            metadata_object_key,
        ) as s3_metadata_file:
            dataset_id = any_dataset_id()
            dataset_type = any_valid_dataset_type()
            with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

                body = {}
                body["id"] = dataset_id
                body["metadata-url"] = s3_metadata_file.url
                body["type"] = dataset_type

                launch_response = entrypoint.lambda_handler(
                    {"httpMethod": "POST", "body": body}, any_lambda_context()
                )["body"]
                logger.info("Executed State Machine: %s", launch_response)

                # poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=launch_response["execution_arn"]
                    )
                )["status"] == "RUNNING":
                    logger.info("Polling for State Machine state %s", "." * 6)
                    time.sleep(5)

                assert execution["status"] == "SUCCEEDED", execution
