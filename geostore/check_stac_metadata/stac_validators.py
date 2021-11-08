from functools import cached_property
from json import load
from os.path import dirname, join

from jsonschema import Draft7Validator, FormatChecker, RefResolver
from jsonschema._utils import URIDict
from jsonschema.validators import extend

from ..stac_format import LINZ_SCHEMA_PATH, QUALITY_SCHEMA_PATH
from ..types import JsonObject


class Schema:
    def __init__(self, path: str):
        self.path = path

    @cached_property
    def as_dict(self) -> JsonObject:
        with open(join(dirname(__file__), self.path), encoding="utf-8") as file_pointer:
            result: JsonObject = load(file_pointer)
            return result

    @cached_property
    def schema_id(self) -> str:
        id_: str = self.as_dict["$id"]
        return id_

    @cached_property
    def uri(self) -> str:
        uri_: str = URIDict().normalize(self.schema_id)
        return uri_


FILE_STAC_SCHEMA_PATH = "file/v2.0.0/schema.json"
PROJECTION_STAC_SCHEMA_PATH = "projection/v1.0.0/schema.json"
VERSION_STAC_SCHEMA_PATH = "version/v1.0.0/schema.json"
FILE_SCHEMA = Schema(FILE_STAC_SCHEMA_PATH)

STAC_SPEC_PATH = "stac-spec/v1.0.0"
CATALOG_SCHEMA = Schema(f"{STAC_SPEC_PATH}/catalog-spec/json-schema/catalog.json")
LINZ_SCHEMA = Schema(LINZ_SCHEMA_PATH)
STAC_ITEM_SPEC_PATH = f"{STAC_SPEC_PATH}/item-spec/json-schema"
ITEM_SCHEMA = Schema(f"{STAC_ITEM_SPEC_PATH}/item.json")

schema_store = {}
for schema in [
    CATALOG_SCHEMA,
    Schema(f"{STAC_SPEC_PATH}/collection-spec/json-schema/collection.json"),
    FILE_SCHEMA,
    Schema("geojson-spec/Feature.json"),
    Schema("geojson-spec/Geometry.json"),
    ITEM_SCHEMA,
    Schema(f"{STAC_ITEM_SPEC_PATH}/basics.json"),
    Schema(f"{STAC_ITEM_SPEC_PATH}/datetime.json"),
    Schema(f"{STAC_ITEM_SPEC_PATH}/instrument.json"),
    Schema(f"{STAC_ITEM_SPEC_PATH}/licensing.json"),
    Schema(f"{STAC_ITEM_SPEC_PATH}/provider.json"),
    LINZ_SCHEMA,
    Schema(PROJECTION_STAC_SCHEMA_PATH),
    Schema(VERSION_STAC_SCHEMA_PATH),
    Schema(QUALITY_SCHEMA_PATH),
]:
    # Normalize URLs the same way as jsonschema does
    schema_store[schema.uri] = schema.as_dict

BaseSTACValidator = extend(Draft7Validator)
BaseSTACValidator.format_checker = FormatChecker()

STACCatalogSchemaValidator = extend(BaseSTACValidator)(
    resolver=RefResolver.from_schema(CATALOG_SCHEMA.as_dict, store=schema_store),
    schema=CATALOG_SCHEMA.as_dict,
)

STACCollectionSchemaValidator = extend(BaseSTACValidator)(
    resolver=RefResolver.from_schema(LINZ_SCHEMA.as_dict, store=schema_store),
    schema=LINZ_SCHEMA.as_dict,
)

STACItemSchemaValidator = extend(BaseSTACValidator)(
    resolver=RefResolver.from_schema(LINZ_SCHEMA.as_dict, store=schema_store),
    schema=LINZ_SCHEMA.as_dict,
)
