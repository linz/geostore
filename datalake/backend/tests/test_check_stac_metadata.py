import logging
import sys
from argparse import Namespace
from copy import deepcopy
from io import StringIO
from json import dump
from typing import Any, Dict, Optional, TextIO
from unittest.mock import Mock, call, patch

from jsonschema import ValidationError
from pytest import raises

from ..processing.check_stac_metadata.task import STACSchemaValidator, main
from .utils import (
    any_dataset_description,
    any_dataset_id,
    any_past_datetime_string,
    any_program_name,
    any_url,
)

STAC_VERSION = "1.0.0-beta.2"

MINIMAL_VALID_STAC_OBJECT = {
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
        self, url_to_json: Dict[str, Dict], call_limit: Optional[int] = None, **kwargs: Any
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
    url = any_url()
    url_reader = MockJSONURLReader({url: MINIMAL_VALID_STAC_OBJECT})
    STACSchemaValidator(url_reader).validate(url)


def test_should_treat_any_missing_top_level_key_as_invalid() -> None:
    url = any_url()
    for key in MINIMAL_VALID_STAC_OBJECT:
        stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        stac_object.pop(key)

        url_reader = MockJSONURLReader({url: stac_object})
        with raises(ValidationError):
            STACSchemaValidator(url_reader).validate(url)


def test_should_detect_invalid_datetime() -> None:
    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    stac_object["extent"]["temporal"]["interval"][0][0] = "not a datetime"
    url = any_url()
    url_reader = MockJSONURLReader({url: stac_object})
    with raises(ValidationError):
        STACSchemaValidator(url_reader).validate(url)


@patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
def test_should_validate_given_url(validate_url_mock) -> None:
    url = any_url()
    sys.argv = [any_program_name(), f"--metadata-url={url}"]

    assert main() == 0

    validate_url_mock.assert_called_once_with(url)


@patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
def test_should_log_arguments(validate_url_mock) -> None:
    validate_url_mock.return_value = None
    url = any_url()
    sys.argv = [any_program_name(), f"--metadata-url={url}"]
    logger = logging.getLogger("datalake.backend.processing.check_stac_metadata.task")

    with patch.object(logger, "debug") as logger_mock:
        main()

        logger_mock.assert_called_once_with(Namespace(metadata_url=url))


def test_should_print_json_output_on_validation_success() -> None:
    sys.argv = [any_program_name(), f"--metadata-url={any_url()}"]

    with patch("sys.stdout") as stdout_mock, patch(
        "datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate"
    ):
        main()

        assert stdout_mock.mock_calls == [
            call.write('{"success": true, "message": ""}'),
            call.write("\n"),
        ]


@patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
def test_should_print_json_output_on_validation_failure(validate_url_mock) -> None:
    error_message = "Some error message"
    expected_calls = [
        call.write(f'{{"success": false, "message": "{error_message}"}}'),
        call.write("\n"),
    ]
    validate_url_mock.side_effect = ValidationError(error_message)
    sys.argv = [any_program_name(), f"--metadata-url={any_url()}"]

    with patch("sys.stdout") as stdout_mock:
        main()

        assert stdout_mock.mock_calls == expected_calls


def test_should_validate_metadata_files_recursively() -> None:
    parent_url = any_url()
    child_url = any_url()

    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    stac_object["links"].append({"href": child_url, "rel": "child"})
    url_reader = MockJSONURLReader({parent_url: stac_object, child_url: MINIMAL_VALID_STAC_OBJECT})

    STACSchemaValidator(url_reader).validate(parent_url)

    assert url_reader.mock_calls == [call(parent_url), call(child_url)]


def test_should_only_validate_each_file_once() -> None:
    root_url = any_url()
    child_url = any_url()
    leaf_url = any_url()

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
