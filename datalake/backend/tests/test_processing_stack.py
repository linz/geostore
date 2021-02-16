import logging
import time
from io import BytesIO
from json import dumps
from typing import Any, Dict

from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from pytest import mark

from ..endpoints.dataset_versions import entrypoint
from ..endpoints.dataset_versions.create import DATASET_VERSION_CREATION_STEP_FUNCTION
from ..endpoints.utils import ResourceName
from .utils import (
    Dataset,
    S3Object,
    any_dataset_description,
    any_dataset_id,
    any_lambda_context,
    any_past_datetime_string,
    any_safe_file_path,
    any_safe_filename,
    any_valid_dataset_type,
)

STAC_VERSION = "1.0.0-beta.2"

MINIMAL_VALID_STAC_OBJECT: Dict[str, Any] = {
    "stac_version": STAC_VERSION,
    "id": any_dataset_id(),
    "description": any_dataset_description(),
    "links": [],
    "license": "MIT",
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[any_past_datetime_string(), None]]},
    },
}

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
def test_should_successfully_run_dataset_version_creation_process(
    step_functions_client: SFNClient,
) -> None:

    metadata_file = "{}/{}.json".format(any_safe_file_path(), any_safe_filename())
    metadata_content = dumps(MINIMAL_VALID_STAC_OBJECT)
    s3_bucket = ResourceName.DATASET_STAGING_BUCKET_NAME.value

    with S3Object(
        BytesIO(f"{metadata_content}".encode()),
        f"{s3_bucket}",
        f"{metadata_file}",
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
