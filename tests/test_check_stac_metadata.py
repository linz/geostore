import logging
import sys
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256, sha512
from io import BytesIO
from json import dumps
from typing import Dict, List
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError  # type: ignore[import]
from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark, raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.check import Check
from backend.check_stac_metadata.task import main
from backend.check_stac_metadata.utils import STACDatasetValidator, STACSchemaValidator
from backend.parameter_store import ParameterName, get_param
from backend.processing_assets_model import ProcessingAssetType, ProcessingAssetsModel
from backend.validation_results_model import ValidationResult, ValidationResultsModel

from .aws_utils import (
    MINIMAL_VALID_STAC_OBJECT,
    MockJSONURLReader,
    MockValidationResultFactory,
    S3Object,
    any_s3_url,
)
from .file_utils import json_dict_to_file_object
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


@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_save_non_s3_url_validation_results(
    validation_results_factory_mock: MagicMock,
    subtests: SubTests,
) -> None:

    non_s3_url = any_https_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    sys.argv = [
        any_program_name(),
        f"--metadata-url={non_s3_url}",
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
    ]

    with subtests.test(msg="Exit code"):
        assert main() == 1

    hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"
    with subtests.test(msg="S3 url validation results"):
        assert validation_results_factory_mock.mock_calls == [
            call(hash_key),
            call().save(
                non_s3_url,
                Check.NON_S3_URL,
                ValidationResult.FAILED,
                details={"message": f"URL doesn't start with “s3://”: “{non_s3_url}”"},
            ),
        ]


@mark.infrastructure
@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_save_asset_multiple_directories_validation_results(
    validation_results_factory_mock: MagicMock,
    subtests: SubTests,
) -> None:
    staging_bucket_name = get_param(ParameterName.STAGING_BUCKET_NAME)

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    base_url = f"s3://{staging_bucket_name}/"
    first_invalid_key = f"{any_safe_filename()}/{any_safe_filename()}"
    second_invalid_key = f"{any_safe_filename()}/{any_safe_filename()}"

    with S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                "assets": {
                    any_asset_name(): {
                        "href": f"{base_url}{first_invalid_key}",
                        "checksum:multihash": any_hex_multihash(),
                    },
                    any_asset_name(): {
                        "href": f"{base_url}{second_invalid_key}",
                        "checksum:multihash": any_hex_multihash(),
                    },
                },
            }
        ),
        bucket_name=staging_bucket_name,
        key=any_safe_filename(),
    ) as root_s3_object, S3Object(
        file_object=json_dict_to_file_object(deepcopy(MINIMAL_VALID_STAC_OBJECT)),
        bucket_name=staging_bucket_name,
        key=first_invalid_key,
    ) as first_invalid_asset, S3Object(
        file_object=json_dict_to_file_object(deepcopy(MINIMAL_VALID_STAC_OBJECT)),
        bucket_name=staging_bucket_name,
        key=second_invalid_key,
    ) as second_invalid_asset:

        sys.argv = [
            any_program_name(),
            f"--metadata-url={root_s3_object.url}",
            f"--dataset-id={dataset_id}",
            f"--version-id={version_id}",
        ]

        with subtests.test(msg="Exit code"):
            assert main() == 0

        hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"
        root_metadata_path = root_s3_object.url.rsplit("/", maxsplit=1)[0]

        with subtests.test(msg="S3 url validation results"):
            assert validation_results_factory_mock.mock_calls == [
                call(hash_key),
                call().save(
                    root_s3_object.url,
                    Check.JSON_SCHEMA,
                    ValidationResult.PASSED,
                ),
                call().save(
                    first_invalid_asset.url,
                    Check.MULTIPLE_DIRECTORIES,
                    ValidationResult.FAILED,
                    details={
                        f"“metadata file: {root_s3_object.url} links to {first_invalid_asset.url}”"
                        f" which exists in a different directory to the root metadata file"
                        f" directory: “{root_metadata_path}”"
                    },
                ),
                call().save(
                    second_invalid_asset.url,
                    Check.MULTIPLE_DIRECTORIES,
                    ValidationResult.FAILED,
                    details={
                        f"“metadata file: {root_s3_object.url} links to"
                        f" {second_invalid_asset.url}” which exists in a different directory"
                        f" to the root metadata file directory: “{root_metadata_path}”"
                    },
                ),
            ]


@mark.infrastructure
@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_save_metadata_multiple_directories_validation_results(
    validation_results_factory_mock: MagicMock,
    subtests: SubTests,
) -> None:
    staging_bucket_name = get_param(ParameterName.STAGING_BUCKET_NAME)
    base_url = f"s3://{staging_bucket_name}/"
    child_dir = any_safe_filename()
    invalid_child_key = f"{child_dir}/{any_safe_filename()}"
    invalid_grandchild_key = f"{child_dir}/{any_safe_filename()}/{any_safe_filename()}"

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                "links": [
                    {"href": f"{base_url}{invalid_child_key}", "rel": "child"},
                ],
            }
        ),
        bucket_name=staging_bucket_name,
        key=any_safe_filename(),
    ) as root_s3_object, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                "links": [
                    {"href": f"{base_url}{invalid_grandchild_key}", "rel": "child"},
                ],
            }
        ),
        bucket_name=staging_bucket_name,
        key=invalid_child_key,
    ) as invalid_child_s3_object, S3Object(
        file_object=json_dict_to_file_object(deepcopy(MINIMAL_VALID_STAC_OBJECT)),
        bucket_name=staging_bucket_name,
        key=invalid_grandchild_key,
    ) as invalid_grandchild_s3_object:
        sys.argv = [
            any_program_name(),
            f"--metadata-url={root_s3_object.url}",
            f"--dataset-id={dataset_id}",
            f"--version-id={version_id}",
        ]
        root_metadata_path = root_s3_object.url.rsplit("/", maxsplit=1)[0]

        with subtests.test(msg="Exit code"):
            assert main() == 0

        hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"
        with subtests.test(msg="S3 url validation results"):
            assert validation_results_factory_mock.mock_calls == [
                call(hash_key),
                call().save(
                    root_s3_object.url,
                    Check.JSON_SCHEMA,
                    ValidationResult.PASSED,
                ),
                call().save(
                    invalid_child_s3_object.url,
                    Check.MULTIPLE_DIRECTORIES,
                    ValidationResult.FAILED,
                    details={
                        "message": f"“metadata file: {root_s3_object.url} links to"
                        f" {invalid_child_s3_object.url}” which exists in a different directory"
                        f" to the root metadata file directory: “{root_metadata_path}”"
                    },
                ),
                call().save(
                    invalid_child_s3_object.url,
                    Check.JSON_SCHEMA,
                    ValidationResult.PASSED,
                ),
                call().save(
                    invalid_grandchild_s3_object.url,
                    Check.MULTIPLE_DIRECTORIES,
                    ValidationResult.FAILED,
                    details={
                        "message": f"“metadata file: {invalid_child_s3_object.url} links to "
                        f"{invalid_grandchild_s3_object.url}” which exists in a different"
                        f" directory to the root metadata file directory: “{root_metadata_path}”"
                    },
                ),
                call().save(
                    invalid_grandchild_s3_object.url,
                    Check.JSON_SCHEMA,
                    ValidationResult.PASSED,
                ),
            ]


@mark.infrastructure
@patch("backend.check_stac_metadata.task.S3_CLIENT.get_object")
@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_save_staging_access_validation_results(
    validation_results_factory_mock: MagicMock,
    get_object_mock: MagicMock,
    subtests: SubTests,
) -> None:

    expected_error = ClientError(
        {"Error": {"Code": "TEST", "Message": "TEST"}}, operation_name="get_object"
    )
    get_object_mock.side_effect = expected_error

    s3_url = any_s3_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    sys.argv = [
        any_program_name(),
        f"--metadata-url={s3_url}",
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
    ]

    with subtests.test(msg="Exit code"):
        assert main() == 1

    hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"
    with subtests.test(msg="Root validation results"):
        assert validation_results_factory_mock.mock_calls == [
            call(hash_key),
            call().save(
                s3_url,
                Check.STAGING_ACCESS,
                ValidationResult.FAILED,
                details={"message": str(expected_error)},
            ),
        ]


@mark.infrastructure
def should_save_json_schema_validation_results_per_file(subtests: SubTests) -> None:
    staging_bucket_name = get_param(ParameterName.STAGING_BUCKET_NAME)
    base_url = f"s3://{staging_bucket_name}/"
    valid_child_key = any_safe_filename()
    invalid_child_key = any_safe_filename()
    invalid_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    invalid_stac_object.pop("id")

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                "links": [
                    {"href": f"{base_url}{valid_child_key}", "rel": "child"},
                    {"href": f"{base_url}{invalid_child_key}", "rel": "child"},
                ],
            }
        ),
        bucket_name=staging_bucket_name,
        key=any_safe_filename(),
    ) as root_s3_object, S3Object(
        file_object=json_dict_to_file_object(deepcopy(MINIMAL_VALID_STAC_OBJECT)),
        bucket_name=staging_bucket_name,
        key=valid_child_key,
    ) as valid_child_s3_object, S3Object(
        file_object=json_dict_to_file_object(invalid_stac_object),
        bucket_name=staging_bucket_name,
        key=invalid_child_key,
    ) as invalid_child_s3_object:
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
            range_key=f"CHECK#{Check.JSON_SCHEMA.value}#URL#{root_s3_object.url}",
            consistent_read=True,
        )
        assert root_result.result == ValidationResult.PASSED.value

    with subtests.test(msg="Valid child validation results"):
        valid_child_result = ValidationResultsModel.get(
            hash_key=hash_key,
            range_key=f"CHECK#{Check.JSON_SCHEMA.value}#URL#{valid_child_s3_object.url}",
            consistent_read=True,
        )
        assert valid_child_result.result == ValidationResult.PASSED.value

    with subtests.test(msg="Invalid child validation results"):
        invalid_child_result = ValidationResultsModel.get(
            hash_key=hash_key,
            range_key=f"CHECK#{Check.JSON_SCHEMA.value}#URL#{invalid_child_s3_object.url}",
            consistent_read=True,
        )
        assert invalid_child_result.result == ValidationResult.FAILED.value


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_insert_asset_urls_and_checksums_into_database(subtests: SubTests) -> None:
    # pylint: disable=too-many-locals
    # Given a metadata file with two assets
    first_asset_content = any_file_contents()
    first_asset_multihash = sha256(first_asset_content).hexdigest()

    second_asset_content = any_file_contents()
    second_asset_multihash = sha512(second_asset_content).hexdigest()

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    storage_bucket_name = get_param(ParameterName.STORAGE_BUCKET_NAME)
    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_content),
        bucket_name=storage_bucket_name,
        key=any_safe_filename(),
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_content),
        bucket_name=storage_bucket_name,
        key=any_safe_filename(),
    ) as second_asset_s3_object:
        expected_hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"

        metadata_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        metadata_stac_object["assets"] = {
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
            bucket_name=storage_bucket_name,
            key=any_safe_filename(),
        ) as metadata_s3_object:
            # When

            expected_asset_items = [
                ProcessingAssetsModel(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.DATA.value}#0",
                    url=first_asset_s3_object.url,
                    multihash=first_asset_multihash,
                ),
                ProcessingAssetsModel(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.DATA.value}#1",
                    url=second_asset_s3_object.url,
                    multihash=second_asset_multihash,
                ),
            ]

            expected_metadata_items = [
                ProcessingAssetsModel(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.METADATA.value}#0",
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
                expected_hash_key,
                ProcessingAssetsModel.sk.startswith(f"{ProcessingAssetType.DATA.value}#"),
            )
            for actual_item, expected_item in zip(actual_items, expected_asset_items):
                with subtests.test():
                    assert actual_item.attribute_values == expected_item.attribute_values

            actual_items = ProcessingAssetsModel.query(
                expected_hash_key,
                ProcessingAssetsModel.sk.startswith(f"{ProcessingAssetType.METADATA.value}#"),
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

    def should_raise_exception_if_non_s3_url_is_passed(self) -> None:
        https_url = any_https_url()
        url_reader = MockJSONURLReader({})

        with raises(AssertionError, match=f"URL doesn't start with “s3://”: “{https_url}”"):
            STACDatasetValidator(url_reader, MockValidationResultFactory(), self.logger).run(
                https_url
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
        stac_object["assets"] = {
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
