import json
import logging
import time
from io import BytesIO
from json import dumps
from typing import Any, Dict

from pytest import mark

from app import ENV, ENVIRONMENT_TYPE_TAG_NAME

from ..endpoints.utils import ResourceName
from .utils import (
    S3Object,
    any_dataset_description,
    any_dataset_id,
    any_past_datetime_string,
    any_safe_file_path,
    any_safe_filename,
)

APPLICATION_TAG_KEY = "ApplicationName"
APPLICATION_TAG_VAL = "geospatial-data-lake"

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


class NoStateMachineFound(Exception):
    pass


class NoComputeEnvironmentFound(Exception):
    pass


def get_state_machine(stepfunctions_client):
    state_machines_detection_response = stepfunctions_client.list_state_machines()

    # make sure there are no more records in next pages
    assert state_machines_detection_response.get("nextToken") is None

    for state_machine in state_machines_detection_response["stateMachines"]:

        tags_detection_response = stepfunctions_client.list_tags_for_resource(
            resourceArn=state_machine["stateMachineArn"]
        )

        # reformat returned array of tags to dictionary
        state_machine_tags = {tag["key"]: tag["value"] for tag in tags_detection_response["tags"]}

        if (
            state_machine_tags[ENVIRONMENT_TYPE_TAG_NAME] == ENV
            and state_machine_tags.get(APPLICATION_TAG_KEY, None) == APPLICATION_TAG_VAL
        ):
            datalake_state_machine = state_machine
            logger.info("Datalake State Machine: %s", datalake_state_machine)

            return datalake_state_machine

    raise NoStateMachineFound(APPLICATION_TAG_VAL, ENV)


@mark.timeout(1200)
@mark.infrastructure
def test_should_successfully_run_dataset_version_creation_process(stepfunctions_client):

    metadata_file = "{}/{}.json".format(any_safe_file_path(), any_safe_filename())
    metadata_content = dumps(MINIMAL_VALID_STAC_OBJECT)
    s3_bucket = ResourceName.DATASET_STAGING_BUCKET_NAME.value

    with S3Object(
        BytesIO(f"{metadata_content}".encode()),
        f"{s3_bucket}",
        f"{metadata_file}",
    ) as s3_metadata_file:

        state_machine_input = json.dumps(
            {
                "dataset_id": "1xyz",
                "version_id": "2xyz",
                "type": "RASTER",
                "metadata_url": s3_metadata_file.url,
            }
        )

        # launch State Machine
        datalake_state_machine = get_state_machine(stepfunctions_client)
        execution_response = stepfunctions_client.start_execution(
            stateMachineArn=datalake_state_machine["stateMachineArn"], input=state_machine_input
        )
        logger.info("Executed State Machine: %s", execution_response)

        # poll for State Machine State
        while (
            execution := stepfunctions_client.describe_execution(
                executionArn=execution_response["executionArn"]
            )
        )["status"] == "RUNNING":
            logger.info("Polling for State Machine state %s", "." * 6)
            time.sleep(5)

        assert execution["status"] == "SUCCEEDED", execution
