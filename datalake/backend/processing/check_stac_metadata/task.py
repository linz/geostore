#!/usr/bin/env python3

from json import load
from os.path import dirname, join
from typing import Callable, TextIO

from jsonschema import FormatChecker, validate

SCHEMA_PATH = join(dirname(__file__), "stac-spec/collection-spec/json-schema/collection.json")


def validate_url(url: str, url_reader: Callable[[str], TextIO]) -> None:
    url_stream = url_reader(url)
    url_json = load(url_stream)

    with open(SCHEMA_PATH) as schema_file:
        schema_json = load(schema_file)

    validate(url_json, schema_json, format_checker=FormatChecker())
