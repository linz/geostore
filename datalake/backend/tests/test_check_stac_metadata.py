import logging
import sys
from argparse import Namespace
from copy import deepcopy
from io import StringIO
from json import dump
from typing import Callable, Dict, TextIO
from unittest.mock import ANY, call, patch

from jsonschema import ValidationError
from pytest import raises

from ..processing.check_stac_metadata.task import main, validate_url
from .utils import (
    any_dataset_description,
    any_dataset_id,
    any_past_datetime_string,
    any_program_name,
)

STAC_VERSION = "1.0.0-beta.2"
ANY_URL = "s3://any-bucket/any-file"

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


def fake_json_url_reader(url_to_json: Dict[str, Dict]) -> Callable[[str], TextIO]:
    def read_url(url: str) -> TextIO:
        result = StringIO()
        dump(url_to_json[url], result)
        result.seek(0)
        return result

    return read_url


def test_should_treat_minimal_stac_object_as_valid() -> None:
    url_reader = fake_json_url_reader({ANY_URL: MINIMAL_VALID_STAC_OBJECT})
    validate_url(ANY_URL, url_reader)


def test_should_treat_any_missing_top_level_key_as_invalid() -> None:
    for key in MINIMAL_VALID_STAC_OBJECT:
        stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        stac_object.pop(key)

        url_reader = fake_json_url_reader({ANY_URL: stac_object})
        with raises(ValidationError):
            validate_url(ANY_URL, url_reader)


def test_should_detect_invalid_datetime() -> None:
    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    stac_object["extent"]["temporal"]["interval"][0][0] = "not a datetime"
    url_reader = fake_json_url_reader({ANY_URL: stac_object})
    with raises(ValidationError):
        validate_url(ANY_URL, url_reader)


@patch("datalake.backend.processing.check_stac_metadata.task.validate_url")
def test_should_validate_given_url(validate_url_mock) -> None:
    sys.argv = [any_program_name(), f"--metadata-url={ANY_URL}"]

    assert main() == 0

    validate_url_mock.assert_called_once_with(ANY_URL, ANY)


@patch("datalake.backend.processing.check_stac_metadata.task.validate_url")
def test_should_log_arguments(validate_url_mock) -> None:
    validate_url_mock.return_value = None
    sys.argv = [any_program_name(), f"--metadata-url={ANY_URL}"]
    logger = logging.getLogger("datalake.backend.processing.check_stac_metadata.task")

    with patch.object(logger, "debug") as logger_mock:
        main()

        logger_mock.assert_called_once_with(Namespace(metadata_url=ANY_URL))


def test_should_print_json_output_on_validation_success() -> None:
    sys.argv = [any_program_name(), f"--metadata-url={ANY_URL}"]

    with patch("sys.stdout") as stdout_mock, patch(
        "datalake.backend.processing.check_stac_metadata.task.validate_url"
    ):
        main()

        assert stdout_mock.mock_calls == [
            call.write('{"success": true, "message": ""}'),
            call.write("\n"),
        ]


@patch("datalake.backend.processing.check_stac_metadata.task.validate_url")
def test_should_print_json_output_on_validation_failure(validate_url_mock) -> None:
    error_message = "Some error message"
    expected_calls = [
        call.write(f'{{"success": false, "message": "{error_message}"}}'),
        call.write("\n"),
    ]
    validate_url_mock.side_effect = ValidationError(error_message)
    sys.argv = [any_program_name(), f"--metadata-url={ANY_URL}"]

    with patch("sys.stdout") as stdout_mock:
        main()

        assert stdout_mock.mock_calls == expected_calls
