from copy import deepcopy
from io import StringIO
from json import dump
from typing import Callable, Dict, TextIO

from jsonschema import ValidationError
from pytest import raises

from ..processing.check_stac_metadata.task import validate_url
from .utils import any_dataset_description, any_dataset_id, any_past_datetime_string

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
