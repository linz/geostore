from json import load
from os.path import dirname, join
from typing import List

from jsonschema import Draft7Validator, FormatChecker, RefResolver
from jsonschema._utils import URIDict

from ..stac_format import LATEST_LINZ_SCHEMA_PATH
from ..types import JsonObject


class BaseSTACValidator(Draft7Validator):  # type: ignore[misc]
    def __init__(self, schema: str, extra_schemas: List[str]) -> None:
        self.script_dir = dirname(__file__)

        item_schema = self.get_schema_dict(schema)

        schema_store = {}
        uri_dictionary = URIDict()
        for extra_schema in extra_schemas:
            # Normalize URLs the same way as jsonschema does
            schema_dict = self.get_schema_dict(extra_schema)
            schema_store[uri_dictionary.normalize(schema_dict["$id"])] = schema_dict

        resolver = RefResolver.from_schema(item_schema, store=schema_store)

        super().__init__(item_schema, resolver=resolver, format_checker=FormatChecker())

    def get_schema_dict(self, path: str) -> JsonObject:
        with open(join(self.script_dir, path), encoding="utf-8") as file_pointer:
            schema_dict: JsonObject = load(file_pointer)
            return schema_dict


class STACItemSchemaValidator(BaseSTACValidator):
    def __init__(self) -> None:
        extra_schemas = [
            "geojson-spec/Feature.json",
            "geojson-spec/Geometry.json",
            "stac-spec/item-spec/json-schema/basics.json",
            "stac-spec/item-spec/json-schema/datetime.json",
            "stac-spec/item-spec/json-schema/instrument.json",
            "stac-spec/item-spec/json-schema/item.json",
            "stac-spec/item-spec/json-schema/licensing.json",
            "stac-spec/item-spec/json-schema/provider.json",
        ]

        super().__init__("stac-spec/item-spec/json-schema/item.json", extra_schemas)


class STACCollectionSchemaValidator(BaseSTACValidator):
    def __init__(self) -> None:
        extra_schemas = [
            LATEST_LINZ_SCHEMA_PATH,
            "stac-spec/catalog-spec/json-schema/catalog.json",
            "stac-spec/collection-spec/json-schema/collection.json",
            "stac-spec/item-spec/json-schema/basics.json",
            "stac-spec/item-spec/json-schema/datetime.json",
            "stac-spec/item-spec/json-schema/instrument.json",
            "stac-spec/item-spec/json-schema/item.json",
            "stac-spec/item-spec/json-schema/licensing.json",
            "stac-spec/item-spec/json-schema/provider.json",
        ]

        super().__init__(LATEST_LINZ_SCHEMA_PATH, extra_schemas)


class STACCatalogSchemaValidator(BaseSTACValidator):
    def __init__(self) -> None:
        extra_schemas = [
            "stac-spec/catalog-spec/json-schema/catalog.json",
        ]

        super().__init__("stac-spec/catalog-spec/json-schema/catalog.json", extra_schemas)
