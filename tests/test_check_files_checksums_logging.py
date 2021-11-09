import sys
from os import environ
from unittest.mock import patch

from pynamodb.exceptions import DoesNotExist
from pytest import mark, raises
from pytest_subtests import SubTests

from geostore.api_keys import MESSAGE_KEY
from geostore.check_files_checksums.task import main
from geostore.check_files_checksums.utils import ARRAY_INDEX_VARIABLE_NAME
from geostore.error_response_keys import ERROR_KEY
from geostore.logging_keys import LOG_MESSAGE_VALIDATION_FAILURE
from geostore.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from geostore.parameter_store import ParameterName, get_param
from geostore.processing_assets_model import ProcessingAssetType, ProcessingAssetsModelBase

from .aws_utils import get_s3_role_arn
from .general_generators import any_program_name
from .stac_generators import any_dataset_id, any_dataset_version_id


@mark.infrastructure
def should_log_missing_item(subtests: SubTests) -> None:
    # Given
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    index = 0
    expected_log = {
        ERROR_KEY: {MESSAGE_KEY: ProcessingAssetsModelBase.DoesNotExist.msg},
        "parameters": {
            "hash_key": (
                f"{DATASET_ID_PREFIX}{dataset_id}"
                f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
            ),
            "range_key": f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{index}",
        },
    }

    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
        f"--first-item={index}",
        f"--assets-table-name={get_param(ParameterName.PROCESSING_ASSETS_TABLE_NAME)}",
        f"--results-table-name={get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)}",
        f"--s3-role-arn={get_s3_role_arn()}",
    ]

    # When/Then
    with patch("geostore.check_files_checksums.task.LOGGER.error") as logger_mock, patch.dict(
        environ, {ARRAY_INDEX_VARIABLE_NAME: "0"}
    ):
        with subtests.test(msg="Return code"), raises(DoesNotExist):
            main()

        with subtests.test(msg="Log message"):
            logger_mock.assert_any_call(LOG_MESSAGE_VALIDATION_FAILURE, error=expected_log)
