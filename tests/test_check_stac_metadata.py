import logging
import sys
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256, sha512
from io import BytesIO
from json import dumps
from typing import Dict, List
from unittest.mock import MagicMock, call, patch

import _pytest
from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark, raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.check_stac_metadata.task import main
from backend.check_stac_metadata.utils import (
    JSON_SCHEMA_VALIDATION_NAME,
    STACDatasetValidator,
    STACSchemaValidator,
)
from backend.processing_assets_model import ProcessingAssetsModel
from backend.resources import ResourceName
from backend.validation_results_model import ValidationResultsModel

from .aws_utils import (
    MINIMAL_VALID_STAC_OBJECT,
    MockJSONURLReader,
    MockValidationResultFactory,
    S3Object,
    any_s3_url,
)
from .general_generators import (
    any_error_message,
    any_file_contents,
    any_https_url,
    any_program_name,
    any_safe_filename,
)
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
    any_stac_relation,
)


@patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
def should_return_non_zero_exit_code_on_validation_failure(
    validate_url_mock: MagicMock,
) -> None:
    validate_url_mock.side_effect = ValidationError(any_error_message())
    sys.argv = [
        any_program_name(),
        f"--metadata-url={any_s3_url()}",
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
    ]

    assert main() == 1


@mark.infrastructure
def should_save_json_schema_validation_results_per_file(subtests: SubTests) -> None:
    base_url = f"s3://{ResourceName.DATASET_STAGING_BUCKET_NAME.value}/"
    root_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    valid_child_key = any_safe_filename()
    invalid_child_key = any_safe_filename()
    root_stac_object["links"] = [
        {"href": f"{base_url}{valid_child_key}", "rel": "child"},
        {"href": f"{base_url}{invalid_child_key}", "rel": "child"},
    ]
    invalid_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    invalid_stac_object.pop("id")

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with S3Object(
        file_object=BytesIO(initial_bytes=dumps(root_stac_object).encode()),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=any_safe_filename(),
    ) as root_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=dumps(deepcopy(MINIMAL_VALID_STAC_OBJECT)).encode()),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=valid_child_key,
    ) as valid_child_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=dumps(invalid_stac_object).encode()),
        bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        key=invalid_child_key,
    ) as invalid_child_s3_object, subtests.test(
        msg="Exit code"
    ):
        sys.argv = [
            any_program_name(),
            f"--metadata-url={root_s3_object.url}",
            f"--dataset-id={dataset_id}",
            f"--version-id={version_id}",
        ]

        with subtests.test(msg="Exit code"):
            assert main() == 1

    hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"
    with subtests.test(msg="Root validation results"):
        root_result = ValidationResultsModel.get(
            hash_key=hash_key,
            range_key=f"CHECK#{JSON_SCHEMA_VALIDATION_NAME}#URL#{root_s3_object.url}",
            consistent_read=True,
        )
        assert root_result.success

    with subtests.test(msg="Valid child validation results"):
        valid_child_result = ValidationResultsModel.get(
            hash_key=hash_key,
            range_key=f"CHECK#{JSON_SCHEMA_VALIDATION_NAME}#URL#{valid_child_s3_object.url}",
            consistent_read=True,
        )
        assert valid_child_result.success

    with subtests.test(msg="Invalid child validation results"):
        invalid_child_result = ValidationResultsModel.get(
            hash_key=hash_key,
            range_key=f"CHECK#{JSON_SCHEMA_VALIDATION_NAME}#URL#{invalid_child_s3_object.url}",
            consistent_read=True,
        )
        assert not invalid_child_result.success


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_insert_asset_urls_and_checksums_into_database(
    subtests: SubTests,
    processing_assets_db_teardown: _pytest.fixtures.FixtureDef[
        object
    ],  # pylint:disable=unused-argument
) -> None:
    # pylint: disable=too-many-locals
    # Given a metadata file with two assets
    first_asset_content = any_file_contents()
    first_asset_multihash = sha256(first_asset_content).hexdigest()

    second_asset_content = any_file_contents()
    second_asset_multihash = sha512(second_asset_content).hexdigest()

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_content),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=any_safe_filename(),
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_content),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=any_safe_filename(),
    ) as second_asset_s3_object:
        expected_hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"

        metadata_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        metadata_stac_object["item_assets"] = {
            any_asset_name(): {
                "href": first_asset_s3_object.url,
                "checksum:multihash": first_asset_multihash,
            },
            any_asset_name(): {
                "href": second_asset_s3_object.url,
                "checksum:multihash": second_asset_multihash,
            },
        }
        metadata_content = dumps(metadata_stac_object).encode()
        with S3Object(
            file_object=BytesIO(initial_bytes=metadata_content),
            bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
            key=any_safe_filename(),
        ) as metadata_s3_object:
            # When

            expected_asset_items = [
                ProcessingAssetsModel(
                    hash_key=expected_hash_key,
                    range_key="DATA_ITEM_INDEX#0",
                    url=first_asset_s3_object.url,
                    multihash=first_asset_multihash,
                ),
                ProcessingAssetsModel(
                    hash_key=expected_hash_key,
                    range_key="DATA_ITEM_INDEX#1",
                    url=second_asset_s3_object.url,
                    multihash=second_asset_multihash,
                ),
            ]

            expected_metadata_items = [
                ProcessingAssetsModel(
                    hash_key=expected_hash_key,
                    range_key="METADATA_ITEM_INDEX#0",
                    url=metadata_s3_object.url,
                ),
            ]

            sys.argv = [
                any_program_name(),
                f"--metadata-url={metadata_s3_object.url}",
                f"--dataset-id={dataset_id}",
                f"--version-id={version_id}",
            ]

            assert main() == 0

            # Then
            actual_items = ProcessingAssetsModel.query(
                expected_hash_key, ProcessingAssetsModel.sk.startswith("DATA_ITEM_INDEX#")
            )
            for actual_item, expected_item in zip(actual_items, expected_asset_items):
                with subtests.test():
                    assert actual_item.attribute_values == expected_item.attribute_values

            actual_items = ProcessingAssetsModel.query(
                expected_hash_key, ProcessingAssetsModel.sk.startswith("METADATA_ITEM_INDEX#")
            )
            for actual_item, expected_item in zip(actual_items, expected_metadata_items):
                with subtests.test():
                    assert actual_item.attribute_values == expected_item.attribute_values


@patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
def should_validate_given_url(validate_url_mock: MagicMock) -> None:
    url = any_s3_url()
    sys.argv = [
        any_program_name(),
        f"--metadata-url={url}",
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
    ]

    with patch("backend.processing_assets_model.ProcessingAssetsModel"):
        assert main() == 0

    validate_url_mock.assert_called_once_with(url)


def should_treat_minimal_stac_object_as_valid() -> None:
    STACSchemaValidator().validate(deepcopy(MINIMAL_VALID_STAC_OBJECT))


def should_treat_any_missing_top_level_key_as_invalid(subtests: SubTests) -> None:
    for key in MINIMAL_VALID_STAC_OBJECT:
        with subtests.test(msg=key):
            stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
            stac_object.pop(key)

            with raises(ValidationError):
                STACSchemaValidator().validate(stac_object)


def should_detect_invalid_datetime() -> None:
    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    stac_object["extent"]["temporal"]["interval"][0][0] = "not a datetime"
    with raises(ValidationError):
        STACSchemaValidator().validate(stac_object)


class TestsWithLogger:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.check_stac_metadata.task")

    def should_validate_metadata_files_recursively(self) -> None:
        base_url = any_s3_url()
        parent_url = f"{base_url}/{any_safe_filename()}"
        child_url = f"{base_url}/{any_safe_filename()}"

        stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        stac_object["links"].append({"href": child_url, "rel": "child"})
        url_reader = MockJSONURLReader(
            {parent_url: stac_object, child_url: deepcopy(MINIMAL_VALID_STAC_OBJECT)}
        )

        STACDatasetValidator(url_reader, MockValidationResultFactory(), self.logger).validate(
            parent_url
        )

        assert url_reader.mock_calls == [call(parent_url), call(child_url)]

    def should_only_validate_each_file_once(self) -> None:
        base_url = any_s3_url()
        root_url = f"{base_url}/{any_safe_filename()}"
        child_url = f"{base_url}/{any_safe_filename()}"
        leaf_url = f"{base_url}/{any_safe_filename()}"

        root_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        root_stac_object["links"] = [
            {"href": child_url, "rel": "child"},
            {"href": root_url, "rel": "root"},
            {"href": root_url, "rel": "self"},
        ]
        child_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        child_stac_object["links"] = [
            {"href": leaf_url, "rel": "child"},
            {"href": root_url, "rel": "root"},
            {"href": child_url, "rel": "self"},
        ]
        leaf_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        leaf_stac_object["links"] = [
            {"href": root_url, "rel": "root"},
            {"href": leaf_url, "rel": "self"},
        ]
        url_reader = MockJSONURLReader(
            {
                root_url: root_stac_object,
                child_url: child_stac_object,
                leaf_url: leaf_stac_object,
            },
            call_limit=3,
        )

        STACDatasetValidator(url_reader, MockValidationResultFactory(), self.logger).validate(
            root_url
        )

        assert url_reader.mock_calls == [call(root_url), call(child_url), call(leaf_url)]

    def should_raise_exception_if_metadata_file_is_in_different_directory(self) -> None:
        base_url = any_s3_url()
        root_url = f"{base_url}/{any_safe_filename()}"
        other_url = f"{base_url}/{any_safe_filename()}/{any_safe_filename()}"

        stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        stac_object["links"].append({"href": other_url, "rel": any_stac_relation()})

        url_reader = MockJSONURLReader({root_url: stac_object})

        with raises(
            AssertionError,
            match=f"“{root_url}” links to metadata file in different directory: “{other_url}”",
        ):
            STACDatasetValidator(url_reader, MockValidationResultFactory(), self.logger).validate(
                root_url
            )

    def should_raise_exception_if_non_s3_url_is_passed(self) -> None:
        https_url = any_https_url()
        url_reader = MockJSONURLReader({})

        with raises(AssertionError, match=f"URL doesn't start with “s3://”: “{https_url}”"):
            STACDatasetValidator(url_reader, MockValidationResultFactory(), self.logger).validate(
                https_url
            )

    def should_raise_exception_if_asset_file_is_in_different_directory(self) -> None:
        base_url = any_s3_url()
        root_url = f"{base_url}/{any_safe_filename()}"
        other_url = f"{base_url}/{any_safe_filename()}/{any_safe_filename()}"

        stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        stac_object["item_assets"] = {
            any_asset_name(): {"href": other_url, "checksum:multihash": any_hex_multihash()}
        }

        url_reader = MockJSONURLReader({root_url: stac_object})

        with raises(
            AssertionError,
            match=f"“{root_url}” links to asset file in different directory: “{other_url}”",
        ):
            STACDatasetValidator(url_reader, MockValidationResultFactory(), self.logger).validate(
                root_url
            )

    def should_return_assets_from_validated_metadata_files(
        self,
        subtests: SubTests,
    ) -> None:
        base_url = any_s3_url()
        metadata_url = f"{base_url}/{any_safe_filename()}"
        stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        first_asset_url = f"{base_url}/{any_safe_filename()}"
        first_asset_multihash = any_hex_multihash()
        second_asset_url = f"{base_url}/{any_safe_filename()}"
        second_asset_multihash = any_hex_multihash()
        stac_object["item_assets"] = {
            any_asset_name(): {
                "href": first_asset_url,
                "checksum:multihash": first_asset_multihash,
            },
            any_asset_name(): {
                "href": second_asset_url,
                "checksum:multihash": second_asset_multihash,
            },
        }
        expected_assets = [
            {"multihash": first_asset_multihash, "url": first_asset_url},
            {"multihash": second_asset_multihash, "url": second_asset_url},
        ]
        expected_metadata = [
            {"url": metadata_url},
        ]
        url_reader = MockJSONURLReader({metadata_url: stac_object})

        validator = STACDatasetValidator(url_reader, MockValidationResultFactory(), self.logger)

        validator.validate(metadata_url)

        with subtests.test():
            assert _sort_assets(validator.dataset_assets) == _sort_assets(expected_assets)
        with subtests.test():
            assert validator.dataset_metadata == expected_metadata


def _sort_assets(assets: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return sorted(assets, key=lambda entry: entry["url"])
