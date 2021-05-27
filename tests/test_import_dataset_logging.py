import json
import logging
from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.error_response_keys import ERROR_KEY
from backend.import_dataset.task import EVENT_KEY, lambda_handler
from backend.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from backend.step_function import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    S3_BATCH_RESPONSE_KEY,
    VERSION_ID_KEY,
)

from .aws_utils import Dataset, ProcessingAsset, any_lambda_context, any_s3_url
from .general_generators import any_error_message, any_etag
from .stac_generators import any_dataset_version_id, any_hex_multihash


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.import_dataset.task")

    @patch("backend.import_dataset.task.S3_CLIENT.head_object")
    @mark.infrastructure
    def should_log_payload(self, head_object_mock: MagicMock) -> None:
        # Given
        head_object_mock.return_value = {"ETag": any_etag()}

        with patch(
            "backend.import_dataset.task.S3CONTROL_CLIENT.create_job"
        ), Dataset() as dataset, patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.import_dataset.task.validate"
        ), patch(
            "backend.import_dataset.task.smart_open"
        ):
            event = {
                DATASET_ID_KEY: dataset.dataset_id,
                DATASET_PREFIX_KEY: dataset.dataset_prefix,
                METADATA_URL_KEY: any_s3_url(),
                VERSION_ID_KEY: any_dataset_version_id(),
            }
            expected_payload_log = dumps({EVENT_KEY: event})

            # When
            lambda_handler(event, any_lambda_context())

            # Then
            logger_mock.assert_any_call(expected_payload_log)

    @patch("backend.import_dataset.task.validate")
    def should_log_schema_validation_warning(self, validate_schema_mock: MagicMock) -> None:
        # Given

        error_message = any_error_message()
        validate_schema_mock.side_effect = ValidationError(error_message)
        expected_log = dumps({ERROR_KEY: error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # When
            lambda_handler(
                {METADATA_URL_KEY: any_s3_url(), VERSION_ID_KEY: any_dataset_version_id()},
                any_lambda_context(),
            )

            # Then
            logger_mock.assert_any_call(expected_log)

    @patch("backend.import_dataset.task.S3_CLIENT.head_object")
    @mark.infrastructure
    def should_log_assets_added_to_manifest(
        self,
        head_object_mock: MagicMock,
        subtests: SubTests,
    ) -> None:
        # Given
        with Dataset() as dataset:
            version_id = any_dataset_version_id()
            asset_id = (
                f"{DATASET_ID_PREFIX}{dataset.dataset_id}"
                f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
            )
            head_object_mock.return_value = {"ETag": any_etag()}

            with ProcessingAsset(
                asset_id=asset_id, multihash=None, url=any_s3_url()
            ) as metadata_processing_asset, ProcessingAsset(
                asset_id=asset_id,
                multihash=any_hex_multihash(),
                url=any_s3_url(),
            ) as processing_asset, patch.object(
                self.logger, "debug"
            ) as logger_mock, patch(
                "backend.import_dataset.task.smart_open"
            ), patch(
                "backend.import_dataset.task.S3CONTROL_CLIENT.create_job"
            ):

                expected_asset_log = dumps({"Adding file to manifest": processing_asset.url})
                expected_metadata_log = dumps(
                    {"Adding file to manifest": metadata_processing_asset.url}
                )

                # When
                lambda_handler(
                    {
                        DATASET_ID_KEY: dataset.dataset_id,
                        DATASET_PREFIX_KEY: dataset.dataset_prefix,
                        METADATA_URL_KEY: any_s3_url(),
                        VERSION_ID_KEY: version_id,
                    },
                    any_lambda_context(),
                )

                # Then
                with subtests.test():
                    logger_mock.assert_any_call(expected_asset_log)
                with subtests.test():
                    logger_mock.assert_any_call(expected_metadata_log)

    @patch("backend.import_dataset.task.S3CONTROL_CLIENT.create_job")
    @patch("backend.import_dataset.task.S3_CLIENT.head_object")
    @mark.infrastructure
    def should_log_s3_batch_response(
        self, head_object_mock: MagicMock, create_job_mock: MagicMock
    ) -> None:
        # Given

        create_job_mock.return_value = response = {"JobId": "Some Response"}
        expected_response_log = json.dumps({S3_BATCH_RESPONSE_KEY: response})
        head_object_mock.return_value = {"ETag": any_etag()}

        with Dataset() as dataset, patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.import_dataset.task.smart_open"
        ):

            # When
            lambda_handler(
                {
                    DATASET_ID_KEY: dataset.dataset_id,
                    DATASET_PREFIX_KEY: dataset.dataset_prefix,
                    METADATA_URL_KEY: any_s3_url(),
                    VERSION_ID_KEY: any_dataset_version_id(),
                },
                any_lambda_context(),
            )

            # Then
            logger_mock.assert_any_call(expected_response_log)
