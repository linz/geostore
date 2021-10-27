from json import dumps, load
from os.path import basename
from typing import TYPE_CHECKING, Dict, Iterable

import boto3
from linz_logger import get_log

from ..boto3_config import CONFIG
from ..import_dataset_file import get_import_result
from ..stac_format import STAC_ASSETS_KEY, STAC_HREF_KEY, STAC_LINKS_KEY
from ..types import JsonObject

S3_BODY_KEY = "Body"

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef
else:
    PutObjectOutputTypeDef = JsonObject  # pragma: no mutate
    S3Client = object  # pragma: no mutate

TARGET_S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)
LOGGER = get_log()


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    return get_import_result(event, importer)


def importer(
    source_bucket_name: str,
    original_key: str,
    target_bucket_name: str,
    new_key: str,
    source_s3_client: S3Client,
) -> PutObjectOutputTypeDef:
    get_object_response = source_s3_client.get_object(Bucket=source_bucket_name, Key=original_key)
    assert S3_BODY_KEY in get_object_response, get_object_response

    metadata = load(get_object_response["Body"])

    assets = metadata.get(STAC_ASSETS_KEY, {}).values()
    change_href_to_basename(assets)

    links = metadata.get(STAC_LINKS_KEY, [])
    change_href_to_basename(links)

    return TARGET_S3_CLIENT.put_object(
        Bucket=target_bucket_name,
        Key=new_key,
        Body=dumps(metadata).encode(),
    )


def change_href_to_basename(items: Iterable[Dict[str, str]]) -> None:
    for item in items:
        item[STAC_HREF_KEY] = basename(item[STAC_HREF_KEY])
