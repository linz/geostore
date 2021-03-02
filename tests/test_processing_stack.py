import logging
import time
from contextlib import nullcontext
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from json import dumps
from typing import ContextManager, Optional

import _pytest
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from pytest import mark

from backend.dataset_versions import entrypoint
from backend.dataset_versions.create import DATASET_VERSION_CREATION_STEP_FUNCTION
from backend.import_dataset.task import S3_BATCH_COPY_ROLE_PARAMETER_NAME
from backend.utils import ResourceName

from .utils import (
    MINIMAL_VALID_STAC_OBJECT,
    Dataset,
    S3Object,
    any_boolean,
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
def test_should_check_state_machine_arn_parameter_exists(ssm_client: SSMClient) -> None:
    """Test if Data Lake State Machine ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(Name=DATASET_VERSION_CREATION_STEP_FUNCTION)
    assert parameter_response["Parameter"]["Name"] == DATASET_VERSION_CREATION_STEP_FUNCTION
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "stateMachine" in parameter_response["Parameter"]["Value"]


@mark.infrastructure
def test_should_check_s3_batch_copy_role_arn_parameter_exists(ssm_client: SSMClient) -> None:
    """Test if Data Lake S3 Batch Copy Role ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(Name=S3_BATCH_COPY_ROLE_PARAMETER_NAME)
    assert parameter_response["Parameter"]["Name"] == S3_BATCH_COPY_ROLE_PARAMETER_NAME
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "iam" in parameter_response["Parameter"]["Value"]


@mark.timeout(1200)
@mark.infrastructure
def test_should_successfully_run_dataset_version_creation_process(
    step_functions_client: SFNClient,
    datasets_db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
    processing_assets_db_teardown: _pytest.fixtures.FixtureDef[
        object
    ],  # pylint:disable=unused-argument
    storage_bucket_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
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
            any_stac_asset_name(): {
                "href": mandatory_asset_s3_object.url,
                "checksum:multihash": sha256_hex_digest_to_multihash(
                    sha256(mandatory_asset_contents).hexdigest()
                ),
            },
        }
        if optional_asset_s3_object is not None:
            metadata["item_assets"][any_stac_asset_name()] = {
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
                launch_response = entrypoint.lambda_handler(
                    {
                        "httpMethod": "POST",
                        "body": {
                            "id": dataset_id,
                            "metadata-url": s3_metadata_file.url,
                            "type": dataset_type,
                        },
                    },
                    any_lambda_context(),
                )["body"]
                logger.info("Executed State Machine: %s", launch_response)

                # Then poll for State Machine State
                while (
                    execution := step_functions_client.describe_execution(
                        executionArn=launch_response["execution_arn"]
                    )
                )["status"] == "RUNNING":
                    logger.info("Polling for State Machine state %s", "." * 6)
                    time.sleep(5)

                assert execution["status"] == "SUCCEEDED", execution
