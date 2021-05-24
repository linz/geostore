from copy import deepcopy
from json import load
from typing import Any, Dict

import pytest
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]
from smart_open import smart_open  # type: ignore[import]

from backend.api_responses import BODY_KEY
from backend.populate_catalog.task import (
    CATALOG_KEY,
    MESSAGE_ATTRIBUTES_KEY,
    RECORDS_KEY,
    ROOT_CATALOG_DESCRIPTION,
    ROOT_CATALOG_ID,
    ROOT_CATALOG_TITLE,
    lambda_handler,
)
from backend.resources import ResourceName
from backend.s3 import S3_URL_PREFIX
from backend.sqs_message_attributes import (
    DATA_TYPE_KEY,
    DATA_TYPE_STRING,
    MESSAGE_ATTRIBUTE_TYPE_DATASET,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
    MESSAGE_ATTRIBUTE_TYPE_ROOT,
    STRING_VALUE_KEY,
)
from backend.stac_format import (
    STAC_DESCRIPTION_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_LINKS_KEY,
    STAC_REL_CHILD,
    STAC_REL_KEY,
    STAC_REL_PARENT,
    STAC_REL_ROOT,
    STAC_TITLE_KEY,
    STAC_TYPE_KEY,
)
from backend.types import JsonList
from tests.aws_utils import Dataset, S3Object, any_lambda_context, delete_s3_key
from tests.file_utils import json_dict_to_file_object
from tests.general_generators import any_safe_filename
from tests.stac_generators import any_dataset_version_id
from tests.stac_objects import (
    MINIMAL_VALID_STAC_CATALOG_OBJECT,
    MINIMAL_VALID_STAC_COLLECTION_OBJECT,
)


@mark.infrastructure
def should_create_new_root_catalog_if_doesnt_exist(subtests: SubTests, s3_client: S3Client) -> None:

    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: dataset.dataset_prefix,
                STAC_TITLE_KEY: dataset.title,
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{CATALOG_KEY}",
    ):

        expected_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
        ]

        try:
            lambda_handler(
                {
                    RECORDS_KEY: [
                        {
                            BODY_KEY: dataset.dataset_prefix,
                            MESSAGE_ATTRIBUTES_KEY: {
                                MESSAGE_ATTRIBUTE_TYPE_KEY: {
                                    STRING_VALUE_KEY: MESSAGE_ATTRIBUTE_TYPE_ROOT,
                                    DATA_TYPE_KEY: DATA_TYPE_STRING,
                                }
                            },
                        }
                    ]
                },
                any_lambda_context(),
            )

            with smart_open(
                f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/{CATALOG_KEY}"
            ) as new_root_metadata_file:
                catalog_json = load(new_root_metadata_file)

                with subtests.test(msg="catalog title"):
                    assert catalog_json[STAC_TITLE_KEY] == ROOT_CATALOG_TITLE

                with subtests.test(msg="catalog description"):
                    assert catalog_json[STAC_DESCRIPTION_KEY] == ROOT_CATALOG_DESCRIPTION

                with subtests.test(msg="catalog links"):
                    assert catalog_json[STAC_LINKS_KEY] == expected_links

        finally:
            delete_s3_key(ResourceName.STORAGE_BUCKET_NAME.value, CATALOG_KEY, s3_client)


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
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{existing_dataset.dataset_prefix}/{CATALOG_KEY}",
    ):

        original_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{existing_dataset.dataset_prefix}/{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
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
            bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
            key=f"{dataset.dataset_prefix}/{CATALOG_KEY}",
        ), S3Object(
            file_object=json_dict_to_file_object(
                {
                    **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                    STAC_ID_KEY: ROOT_CATALOG_ID,
                    STAC_DESCRIPTION_KEY: ROOT_CATALOG_DESCRIPTION,
                    STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                    STAC_LINKS_KEY: original_links,
                }
            ),
            bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
            key=CATALOG_KEY,
        ):

            expected_links: JsonList = original_links + [
                {
                    STAC_REL_KEY: STAC_REL_CHILD,
                    STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{CATALOG_KEY}",
                    STAC_TYPE_KEY: "application/json",
                }
            ]

            lambda_handler(
                {
                    RECORDS_KEY: [
                        {
                            BODY_KEY: dataset.dataset_prefix,
                            MESSAGE_ATTRIBUTES_KEY: {
                                MESSAGE_ATTRIBUTE_TYPE_KEY: {
                                    STRING_VALUE_KEY: MESSAGE_ATTRIBUTE_TYPE_ROOT,
                                    DATA_TYPE_KEY: DATA_TYPE_STRING,
                                }
                            },
                        }
                    ]
                },
                any_lambda_context(),
            )

            with smart_open(
                f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/{CATALOG_KEY}"
            ) as root_metadata_file:
                catalog_json = load(root_metadata_file)

                with subtests.test(msg="catalog title"):
                    assert catalog_json[STAC_TITLE_KEY] == ROOT_CATALOG_TITLE

                with subtests.test(msg="catalog description"):
                    assert catalog_json[STAC_DESCRIPTION_KEY] == ROOT_CATALOG_DESCRIPTION

                with subtests.test(msg="catalog links"):
                    assert catalog_json[STAC_LINKS_KEY] == expected_links


@pytest.mark.parametrize(
    "stac_object", [MINIMAL_VALID_STAC_COLLECTION_OBJECT, MINIMAL_VALID_STAC_CATALOG_OBJECT]
)
@mark.infrastructure
def should_update_dataset_catalog_with_new_version(
    stac_object: Dict[str, Any], subtests: SubTests
) -> None:

    dataset_version = any_dataset_version_id()
    filename = f"{any_safe_filename()}.json"
    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(stac_object),
                STAC_ID_KEY: dataset.dataset_prefix,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"{dataset.dataset_prefix}/{filename}",
                        STAC_TYPE_KEY: "application/json",
                    },
                ],
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{dataset_version}/{filename}",
    ) as dataset_version_metadata, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: dataset.dataset_prefix,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{CATALOG_KEY}",
                        STAC_TYPE_KEY: "application/json",
                    },
                    {
                        STAC_REL_KEY: STAC_REL_PARENT,
                        STAC_HREF_KEY: f"./{CATALOG_KEY}",
                        STAC_TYPE_KEY: "application/json",
                    },
                ],
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{CATALOG_KEY}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: ROOT_CATALOG_ID,
                STAC_DESCRIPTION_KEY: ROOT_CATALOG_DESCRIPTION,
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{CATALOG_KEY}",
                        STAC_TYPE_KEY: "application/json",
                    },
                    {
                        STAC_REL_KEY: STAC_REL_CHILD,
                        STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{CATALOG_KEY}",
                        STAC_TYPE_KEY: "application/json",
                    },
                ],
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=CATALOG_KEY,
    ):

        expected_dataset_catalog_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"./{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset_version_metadata.key}",
                STAC_TYPE_KEY: "application/json",
            },
        ]
        expected_dataset_version_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"./{CATALOG_KEY}/{dataset.dataset_prefix}/{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
        ]

        lambda_handler(
            {
                RECORDS_KEY: [
                    {
                        BODY_KEY: dataset_version_metadata.key,
                        MESSAGE_ATTRIBUTES_KEY: {
                            MESSAGE_ATTRIBUTE_TYPE_KEY: {
                                STRING_VALUE_KEY: MESSAGE_ATTRIBUTE_TYPE_DATASET,
                                DATA_TYPE_KEY: DATA_TYPE_STRING,
                            }
                        },
                    }
                ]
            },
            any_lambda_context(),
        )

        with subtests.test(msg="dataset catalog links"), smart_open(
            f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/"
            f"{dataset.dataset_prefix}/{CATALOG_KEY}"
        ) as updated_dataset_metadata_file:
            collection_json = load(updated_dataset_metadata_file)
            assert collection_json[STAC_LINKS_KEY] == expected_dataset_catalog_links

        with subtests.test(msg="dataset version links"), smart_open(
            f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}"
            f"/{dataset_version_metadata.key}"
            f"{dataset.dataset_prefix}/{CATALOG_KEY}"
        ) as updated_dataset_metadata_file:
            collection_json = load(updated_dataset_metadata_file)
            assert collection_json[STAC_LINKS_KEY] == expected_dataset_version_links
