import sys
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256, sha512
from io import BytesIO, StringIO
from json import dump, dumps
from typing import Any, Dict, List, Optional, TextIO
from unittest.mock import MagicMock, Mock, call, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark, raises
from pytest_subtests import SubTests  # type: ignore[import]

from ..endpoints.utils import ResourceName
from ..processing.check_stac_metadata.task import ProcessingAssetsModel, STACSchemaValidator, main
from .utils import (
    S3Object,
    any_dataset_description,
    any_dataset_id,
    any_dataset_version_id,
    any_error_message,
    any_file_contents,
    any_hex_multihash,
    any_https_url,
    any_past_datetime_string,
    any_program_name,
    any_s3_url,
    any_safe_filename,
    any_stac_asset_name,
    any_stac_relation,
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


class MockJSONURLReader(Mock):
    def __init__(
        self, url_to_json: Dict[str, Any], call_limit: Optional[int] = None, **kwargs: Any
    ):
        super().__init__(**kwargs)

        self.url_to_json = url_to_json
        self.call_limit = call_limit
        self.side_effect = self.read_url

    def read_url(self, url: str) -> TextIO:
        if self.call_limit is not None:
            assert self.call_count <= self.call_limit

        result = StringIO()
        dump(self.url_to_json[url], result)
        result.seek(0)
        return result


def test_should_treat_minimal_stac_object_as_valid() -> None:
    url = any_s3_url()
    url_reader = MockJSONURLReader({url: deepcopy(MINIMAL_VALID_STAC_OBJECT)})
    STACSchemaValidator(url_reader).validate(url)


def test_should_treat_any_missing_top_level_key_as_invalid(
    subtests: SubTests,
) -> None:
    url = any_s3_url()
    for key in MINIMAL_VALID_STAC_OBJECT:
        with subtests.test(msg=key):
            stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
            stac_object.pop(key)

            url_reader = MockJSONURLReader({url: stac_object})
            with raises(ValidationError):
                STACSchemaValidator(url_reader).validate(url)


def test_should_detect_invalid_datetime() -> None:
    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    stac_object["extent"]["temporal"]["interval"][0][0] = "not a datetime"
    url = any_s3_url()
    url_reader = MockJSONURLReader({url: stac_object})
    with raises(ValidationError):
        STACSchemaValidator(url_reader).validate(url)


@patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
def test_should_validate_given_url(validate_url_mock: MagicMock) -> None:
    url = any_s3_url()
    sys.argv = [
        any_program_name(),
        f"--metadata-url={url}",
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
    ]

    with patch("datalake.backend.processing.check_stac_metadata.task.ProcessingAssetsModel"):
        assert main() == 0

    validate_url_mock.assert_called_once_with(url)


@patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
def test_should_print_json_output_on_validation_failure(validate_url_mock: MagicMock) -> None:
    validate_url_mock.side_effect = ValidationError(any_error_message())
    sys.argv = [
        any_program_name(),
        f"--metadata-url={any_s3_url()}",
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
    ]

    assert main() == 1


def test_should_validate_metadata_files_recursively() -> None:
    base_url = any_s3_url()
    parent_url = f"{base_url}/{any_safe_filename()}"
    child_url = f"{base_url}/{any_safe_filename()}"

    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    stac_object["links"].append({"href": child_url, "rel": "child"})
    url_reader = MockJSONURLReader(
        {parent_url: stac_object, child_url: deepcopy(MINIMAL_VALID_STAC_OBJECT)}
    )

    STACSchemaValidator(url_reader).validate(parent_url)

    assert url_reader.mock_calls == [call(parent_url), call(child_url)]


def test_should_only_validate_each_file_once() -> None:
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

    STACSchemaValidator(url_reader).validate(root_url)

    assert url_reader.mock_calls == [call(root_url), call(child_url), call(leaf_url)]


def test_should_raise_exception_if_related_file_is_in_different_directory() -> None:
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
        STACSchemaValidator(url_reader).validate(root_url)


def test_should_raise_exception_if_non_s3_url_is_passed() -> None:
    https_url = any_https_url()
    url_reader = MockJSONURLReader({})

    with raises(AssertionError, match=f"URL doesn't start with “s3://”: “{https_url}”"):
        STACSchemaValidator(url_reader).validate(https_url)


def test_should_return_assets_from_validated_metadata_files() -> None:
    url = any_s3_url()
    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    first_asset_url = any_s3_url()
    first_asset_multihash = any_hex_multihash()
    second_asset_url = any_s3_url()
    second_asset_multihash = any_hex_multihash()
    stac_object["assets"] = {
        any_stac_asset_name(): {
            "href": first_asset_url,
            "checksum:multihash": first_asset_multihash,
        },
        any_stac_asset_name(): {
            "href": second_asset_url,
            "checksum:multihash": second_asset_multihash,
        },
    }
    expected_assets = [
        {"url": first_asset_url, "multihash": first_asset_multihash},
        {"url": second_asset_url, "multihash": second_asset_multihash},
    ]
    url_reader = MockJSONURLReader({url: stac_object})

    assets = STACSchemaValidator(url_reader).validate(url)

    assert _sort_assets(assets) == _sort_assets(expected_assets)


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def test_should_insert_asset_urls_and_checksums_into_database(
    subtests: SubTests,
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
        BytesIO(initial_bytes=first_asset_content),
        ResourceName.STORAGE_BUCKET_NAME.value,
        any_safe_filename(),
    ) as first_asset_s3_object, S3Object(
        BytesIO(initial_bytes=second_asset_content),
        ResourceName.STORAGE_BUCKET_NAME.value,
        any_safe_filename(),
    ) as second_asset_s3_object:
        expected_hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"
        expected_items = [
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

        metadata_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        metadata_stac_object["assets"] = {
            any_stac_asset_name(): {
                "href": first_asset_s3_object.url,
                "checksum:multihash": first_asset_multihash,
            },
            any_stac_asset_name(): {
                "href": second_asset_s3_object.url,
                "checksum:multihash": second_asset_multihash,
            },
        }
        metadata_content = dumps(metadata_stac_object).encode()
        with S3Object(
            BytesIO(initial_bytes=metadata_content),
            ResourceName.STORAGE_BUCKET_NAME.value,
            any_safe_filename(),
        ) as metadata_s3_object:
            # When

            sys.argv = [
                any_program_name(),
                f"--metadata-url={metadata_s3_object.url}",
                f"--dataset-id={dataset_id}",
                f"--version-id={version_id}",
            ]

            assert main() == 0

            # Then
            actual_items = ProcessingAssetsModel.query(expected_hash_key)
            for actual_item, expected_item in zip(actual_items, expected_items):
                with subtests.test():
                    assert actual_item.attribute_values == expected_item.attribute_values


def _sort_assets(assets: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return sorted(assets, key=lambda entry: entry["url"])
