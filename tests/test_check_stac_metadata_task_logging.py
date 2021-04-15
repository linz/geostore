import logging
from json import dumps
from unittest.mock import patch

from backend.check_stac_metadata.task import lambda_handler
from backend.step_function_event_keys import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from tests.aws_utils import any_lambda_context, any_s3_url
from tests.stac_generators import any_dataset_id, any_dataset_version_id

LOGGER = logging.getLogger("backend.check_stac_metadata.task")


def should_log_event_payload() -> None:
    payload = {
        DATASET_ID_KEY: any_dataset_id(),
        VERSION_ID_KEY: any_dataset_version_id(),
        METADATA_URL_KEY: any_s3_url(),
    }

    expected_log = dumps({"event": payload})

    with patch.object(
        logging.getLogger("backend.check_stac_metadata.task"), "debug"
    ) as logger_mock, patch("backend.check_stac_metadata.task.STACDatasetValidator.run"):
        lambda_handler(
            payload,
            any_lambda_context(),
        )
        logger_mock.assert_any_call(expected_log)
