import sys
from copy import deepcopy
from io import StringIO
from json import dump
from typing import Any, Dict, Optional, TextIO
from unittest.mock import Mock, call, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import raises

from ..processing.check_stac_metadata.task import STACSchemaValidator, main
from .utils import (
    any_dataset_description,
    any_dataset_id,
    any_https_url,
    any_past_datetime_string,
    any_program_name,
    any_s3_url,
    any_safe_filename,
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
    url_reader = MockJSONURLReader({url: MINIMAL_VALID_STAC_OBJECT})
    STACSchemaValidator(url_reader).validate(url)


def test_should_treat_any_missing_top_level_key_as_invalid() -> None:
    url = any_s3_url()
    for key in MINIMAL_VALID_STAC_OBJECT:
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
def test_should_validate_given_url(validate_url_mock) -> None:
    url = any_s3_url()
    sys.argv = [any_program_name(), f"--metadata-url={url}"]

    assert main() == 0

    validate_url_mock.assert_called_once_with(url)


def test_should_validate_metadata_files_recursively() -> None:
    base_url = any_s3_url()
    parent_url = f"{base_url}/{any_safe_filename()}"
    child_url = f"{base_url}/{any_safe_filename()}"

    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    stac_object["links"].append({"href": child_url, "rel": "child"})
    url_reader = MockJSONURLReader({parent_url: stac_object, child_url: MINIMAL_VALID_STAC_OBJECT})

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
