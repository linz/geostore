from copy import deepcopy
from json import load

from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]
from smart_open import smart_open  # type: ignore[import]

from backend.resources import ResourceName
from backend.types import JsonList
from backend.write_to_catalog import task
from backend.write_to_catalog.task import (
    ROOT_CATALOG_DESCRIPTION,
    ROOT_CATALOG_ID,
    ROOT_CATALOG_KEY,
    ROOT_CATALOG_TITLE,
)
from tests.aws_utils import Dataset, S3Object, any_lambda_context, delete_s3_key
from tests.file_utils import json_dict_to_file_object
from tests.stac_objects import MINIMAL_VALID_STAC_CATALOG_OBJECT


@mark.infrastructure
def should_create_new_root_catalog_if_doesnt_exist(subtests: SubTests, s3_client: S3Client) -> None:

    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                "id": dataset.dataset_id,
                "title": dataset.title,
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{ROOT_CATALOG_KEY}",
    ):

        body = {"Records": [{"body": dataset.dataset_prefix}]}

        try:
            task.lambda_handler(body, any_lambda_context())

            expected_links: JsonList = [
                {"rel": "root", "href": "./catalog.json"},
                {"rel": "child", "href": f"./{dataset.dataset_prefix}/catalog.json"},
            ]
            with smart_open(
                f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/{ROOT_CATALOG_KEY}"
            ) as new_root_metadata_file:
                catalog_json = load(new_root_metadata_file)

                with subtests.test(msg="catalog title"):
                    assert catalog_json["title"] == ROOT_CATALOG_TITLE

                with subtests.test(msg="catalog description"):
                    assert catalog_json["description"] == ROOT_CATALOG_DESCRIPTION

                with subtests.test(msg="catalog description"):
                    assert catalog_json["links"] == expected_links

        finally:
            delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, ROOT_CATALOG_KEY, s3_client)


@mark.infrastructure
def should_update_existing_root_catalog(subtests: SubTests) -> None:

    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                "id": dataset.dataset_id,
                "title": dataset.title,
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{ROOT_CATALOG_KEY}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                "id": ROOT_CATALOG_ID,
                "description": ROOT_CATALOG_DESCRIPTION,
                "title": ROOT_CATALOG_TITLE,
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=ROOT_CATALOG_KEY,
    ):

        body = {"Records": [{"body": dataset.dataset_prefix}]}

        task.lambda_handler(body, any_lambda_context())

        expected_links: JsonList = [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": f"./{dataset.dataset_prefix}/catalog.json"},
        ]

        with smart_open(
            f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/{ROOT_CATALOG_KEY}"
        ) as root_metadata_file:
            catalog_json = load(root_metadata_file)

            with subtests.test(msg="catalog title"):
                assert catalog_json["title"] == ROOT_CATALOG_TITLE

            with subtests.test(msg="catalog description"):
                assert catalog_json["description"] == ROOT_CATALOG_DESCRIPTION

            with subtests.test(msg="catalog description"):
                assert catalog_json["links"] == expected_links
