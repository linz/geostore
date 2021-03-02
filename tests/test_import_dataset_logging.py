import json
import logging
from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.import_dataset.task import lambda_handler

from .utils import (
    ProcessingAsset,
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
    any_lambda_context,
    any_s3_url,
)


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.import_dataset.task")

    @mark.infrastructure
    @patch("backend.import_dataset.task.S3CONTROL_CLIENT.create_job")
    def test_should_log_payload(self, create_job_mock: MagicMock) -> None:
        # Given

        create_job_mock.return_value = {"JobId": "Some Response"}

        body = {}
        body["dataset_id"] = any_dataset_id()
        body["metadata_url"] = any_s3_url()
        body["version_id"] = any_dataset_version_id()
        expected_payload_log = dumps({"payload": body})

        with patch.object(self.logger, "debug") as log_mock:

            # When
            lambda_handler(body, any_lambda_context())

            # Then
            log_mock.assert_any_call(expected_payload_log)

    @patch("backend.import_dataset.task.validate")
    def test_should_log_missing_argument_warning(self, validate_schema_mock: MagicMock) -> None:
        # Given

        error_message = "Some error message"
        validate_schema_mock.side_effect = ValidationError(error_message)
        expected_log = dumps({"error": error_message})

        body = {}
        body["metadata_url"] = any_s3_url()
        body["version_id"] = any_dataset_version_id()

        with patch.object(self.logger, "warning") as log_mock:
            # When
            lambda_handler(body, any_lambda_context())

            # Then
            log_mock.assert_any_call(expected_log)

    @patch("backend.import_dataset.task.S3CONTROL_CLIENT.create_job")
    @mark.infrastructure
    def test_should_log_assets_added_to_manifest(
        self, create_job_mock: MagicMock, subtests: SubTests
    ) -> None:
        # Given
        create_job_mock.return_value = {"JobId": "Some Response"}
        dataset_id = any_dataset_id()
        version_id = any_dataset_version_id()
        asset_id = f"DATASET#{dataset_id}#VERSION#{version_id}"

        with ProcessingAsset(
            asset_id=asset_id, multihash=None, url=any_s3_url()
        ) as metadata_processing_asset, ProcessingAsset(
            asset_id=asset_id,
            multihash=any_hex_multihash(),
            url=any_s3_url(),
        ) as processing_asset:

            expected_asset_log = dumps({"Adding file to manifest": processing_asset.url})
            expected_metadata_log = dumps(
                {"Adding file to manifest": metadata_processing_asset.url}
            )

            body = {}
            body["dataset_id"] = dataset_id
            body["metadata_url"] = any_s3_url()
            body["version_id"] = version_id

            with patch.object(self.logger, "debug") as log_mock:
                # When
                lambda_handler(body, any_lambda_context())

                # Then
                with subtests.test():
                    log_mock.assert_any_call(expected_asset_log)
                with subtests.test():
                    log_mock.assert_any_call(expected_metadata_log)

    @patch("backend.import_dataset.task.S3CONTROL_CLIENT.create_job")
    @mark.infrastructure
    def test_should_log_s3_batch_repsonse(self, create_job_mock: MagicMock) -> None:
        # Given

        create_job_mock.return_value = response = {"JobId": "Some Response"}

        body = {}
        body["dataset_id"] = any_dataset_id()
        body["metadata_url"] = any_s3_url()
        body["version_id"] = any_dataset_version_id()
        expected_response_log = json.dumps({"s3 batch response": response})

        with patch.object(self.logger, "debug") as log_mock:

            # When
            lambda_handler(body, any_lambda_context())

            # Then
            log_mock.assert_any_call(expected_response_log)
