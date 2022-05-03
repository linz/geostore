from copy import deepcopy
from json import load

import smart_open
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests

from geostore.aws_keys import BODY_KEY
from geostore.populate_catalog.task import (
    CATALOG_FILENAME,
    RECORDS_KEY,
    ROOT_CATALOG_DESCRIPTION,
    ROOT_CATALOG_ID,
    ROOT_CATALOG_TITLE,
    lambda_handler,
)
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX
from geostore.stac_format import (
    STAC_DESCRIPTION_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_LINKS_KEY,
    STAC_MEDIA_TYPE_JSON,
    STAC_REL_CHILD,
    STAC_REL_ITEM,
    STAC_REL_KEY,
    STAC_REL_PARENT,
    STAC_REL_ROOT,
    STAC_TITLE_KEY,
    STAC_TYPE_KEY,
)
from geostore.types import JsonList
from tests.aws_utils import Dataset, S3Object, any_lambda_context, delete_s3_key
from tests.file_utils import json_dict_to_file_object
from tests.general_generators import any_safe_filename
from tests.stac_generators import any_dataset_title
from tests.stac_objects import (
    MINIMAL_VALID_STAC_CATALOG_OBJECT,
    MINIMAL_VALID_STAC_COLLECTION_OBJECT,
    MINIMAL_VALID_STAC_ITEM_OBJECT,
)


@mark.infrastructure
def should_create_new_root_catalog_if_doesnt_exist(subtests: SubTests, s3_client: S3Client) -> None:
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-statements

    with Dataset() as dataset:
        collection_metadata_filename = any_safe_filename()
        catalog_metadata_filename = any_safe_filename()
        item_metadata_filename = any_safe_filename()

        collection_title = any_dataset_title()

        with S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                    STAC_TITLE_KEY: dataset.title,
                    STAC_LINKS_KEY: [
                        {STAC_HREF_KEY: collection_metadata_filename, STAC_REL_KEY: STAC_REL_CHILD},
                        {STAC_HREF_KEY: catalog_metadata_filename, STAC_REL_KEY: STAC_REL_ROOT},
                    ],
                }
            ),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=f"{dataset.dataset_prefix}/{catalog_metadata_filename}",
        ) as catalog_metadata_file, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    STAC_TITLE_KEY: collection_title,
                    STAC_LINKS_KEY: [
                        {STAC_HREF_KEY: catalog_metadata_filename, STAC_REL_KEY: STAC_REL_PARENT},
                        {STAC_HREF_KEY: item_metadata_filename, STAC_REL_KEY: STAC_REL_ITEM},
                        {STAC_HREF_KEY: catalog_metadata_filename, STAC_REL_KEY: STAC_REL_ROOT},
                    ],
                }
            ),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=f"{dataset.dataset_prefix}/{collection_metadata_filename}",
        ) as collection_metadata_file, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                    STAC_LINKS_KEY: [
                        {
                            STAC_HREF_KEY: collection_metadata_filename,
                            STAC_REL_KEY: STAC_REL_PARENT,
                        },
                        {STAC_HREF_KEY: catalog_metadata_filename, STAC_REL_KEY: STAC_REL_ROOT},
                    ],
                }
            ),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=f"{dataset.dataset_prefix}/{item_metadata_filename}",
        ) as item_metadata_file:

            try:
                lambda_handler(
                    {
                        RECORDS_KEY: [
                            {
                                BODY_KEY: catalog_metadata_file.key,
                            }
                        ]
                    },
                    any_lambda_context(),
                )

                with subtests.test(msg="root catalog links"), smart_open.open(
                    f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/"
                    f"{CATALOG_FILENAME}",
                    mode="rb",
                ) as root_metadata_file:
                    catalog_json = load(root_metadata_file)

                    assert catalog_json[STAC_LINKS_KEY] == [
                        {
                            STAC_REL_KEY: STAC_REL_ROOT,
                            STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                        },
                        {
                            STAC_REL_KEY: STAC_REL_CHILD,
                            STAC_HREF_KEY: f"./{catalog_metadata_file.key}",
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                            STAC_TITLE_KEY: dataset.title,
                        },
                    ]

                with subtests.test(msg="catalog links"), smart_open.open(
                    f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/"
                    f"{catalog_metadata_file.key}",
                    mode="rb",
                ) as new_dataset_metadata_file:
                    dataset_json = load(new_dataset_metadata_file)
                    assert dataset_json[STAC_LINKS_KEY] == [
                        {
                            STAC_REL_KEY: STAC_REL_CHILD,
                            STAC_HREF_KEY: f"./{collection_metadata_filename}",
                            STAC_TITLE_KEY: collection_title,
                        },
                        {
                            STAC_REL_KEY: STAC_REL_ROOT,
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                        },
                        {
                            STAC_REL_KEY: STAC_REL_PARENT,
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                        },
                    ]

                with subtests.test(msg="collection links"), smart_open.open(
                    f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/"
                    f"{collection_metadata_file.key}",
                    mode="rb",
                ) as collection:
                    collection_json = load(collection)

                    assert collection_json[STAC_LINKS_KEY] == [
                        {
                            STAC_HREF_KEY: f"./{item_metadata_filename}",
                            STAC_REL_KEY: STAC_REL_ITEM,
                        },
                        {
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_REL_KEY: STAC_REL_ROOT,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                        {
                            STAC_REL_KEY: STAC_REL_PARENT,
                            STAC_HREF_KEY: f"./{catalog_metadata_filename}",
                            STAC_TITLE_KEY: dataset.title,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                    ]

                with subtests.test(msg="item links"), smart_open.open(
                    f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/"
                    f"{item_metadata_file.key}",
                    mode="rb",
                ) as item:
                    item_json = load(item)

                    assert item_json[STAC_LINKS_KEY] == [
                        {
                            STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                            STAC_REL_KEY: STAC_REL_ROOT,
                            STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                        {
                            STAC_HREF_KEY: f"./{collection_metadata_filename}",
                            STAC_REL_KEY: STAC_REL_PARENT,
                            STAC_TITLE_KEY: collection_title,
                            STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                        },
                    ]

            finally:
                delete_s3_key(
                    Resource.STORAGE_BUCKET_NAME.resource_name, CATALOG_FILENAME, s3_client
                )


@mark.infrastructure
def should_update_existing_root_catalog(subtests: SubTests) -> None:

    with Dataset() as existing_dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: existing_dataset.dataset_prefix,
                STAC_TITLE_KEY: existing_dataset.title,
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{existing_dataset.dataset_prefix}/{CATALOG_FILENAME}",
    ):

        original_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{existing_dataset.dataset_prefix}/{CATALOG_FILENAME}",
                STAC_TITLE_KEY: existing_dataset.title,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
        ]

        with Dataset() as dataset, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                    STAC_ID_KEY: dataset.dataset_prefix,
                    STAC_TITLE_KEY: dataset.title,
                }
            ),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=f"{dataset.dataset_prefix}/{CATALOG_FILENAME}",
        ) as new_dataset_metadata, S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                    STAC_ID_KEY: ROOT_CATALOG_ID,
                    STAC_DESCRIPTION_KEY: ROOT_CATALOG_DESCRIPTION,
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_LINKS_KEY: original_links,
                }
            ),
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
            key=CATALOG_FILENAME,
        ):

            expected_root_links: JsonList = original_links + [
                {
                    STAC_REL_KEY: STAC_REL_CHILD,
                    STAC_HREF_KEY: f"./{new_dataset_metadata.key}",
                    STAC_TITLE_KEY: dataset.title,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                }
            ]

            expected_dataset_links: JsonList = [
                {
                    STAC_REL_KEY: STAC_REL_ROOT,
                    STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
                {
                    STAC_REL_KEY: STAC_REL_PARENT,
                    STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                },
            ]

            lambda_handler(
                {
                    RECORDS_KEY: [
                        {
                            BODY_KEY: new_dataset_metadata.key,
                        }
                    ]
                },
                any_lambda_context(),
            )

            with smart_open.open(
                f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{CATALOG_FILENAME}",
                mode="rb",
            ) as root_metadata_file, subtests.test(msg="root catalog links"):
                root_catalog_json = load(root_metadata_file)
                assert root_catalog_json[STAC_LINKS_KEY] == expected_root_links

            with smart_open.open(
                f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}"
                f"/{new_dataset_metadata.key}",
                mode="rb",
            ) as dataset_metadata_file, subtests.test(msg="dataset catalog links"):
                dataset_catalog_json = load(dataset_metadata_file)
                assert dataset_catalog_json[STAC_LINKS_KEY] == expected_dataset_links
