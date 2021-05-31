from copy import deepcopy
from json import load

from _pytest.python_api import raises
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
    UnhandledSQSMessageException,
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
    STRING_VALUE_KEY_LOWER,
)
from backend.stac_format import (
    STAC_DESCRIPTION_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_LINKS_KEY,
    STAC_REL_CHILD,
    STAC_REL_ITEM,
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
    MINIMAL_VALID_STAC_ITEM_OBJECT,
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
                                    STRING_VALUE_KEY_LOWER: MESSAGE_ATTRIBUTE_TYPE_ROOT,
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

            expected_root_links: JsonList = original_links + [
                {
                    STAC_REL_KEY: STAC_REL_CHILD,
                    STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{CATALOG_KEY}",
                    STAC_TYPE_KEY: "application/json",
                }
            ]

            expected_dataset_links: JsonList = [
                {
                    STAC_REL_KEY: STAC_REL_ROOT,
                    STAC_HREF_KEY: f"../{CATALOG_KEY}",
                    STAC_TYPE_KEY: "application/json",
                },
                {
                    STAC_REL_KEY: STAC_REL_PARENT,
                    STAC_HREF_KEY: f"../{CATALOG_KEY}",
                    STAC_TYPE_KEY: "application/json",
                },
            ]

            lambda_handler(
                {
                    RECORDS_KEY: [
                        {
                            BODY_KEY: dataset.dataset_prefix,
                            MESSAGE_ATTRIBUTES_KEY: {
                                MESSAGE_ATTRIBUTE_TYPE_KEY: {
                                    STRING_VALUE_KEY_LOWER: MESSAGE_ATTRIBUTE_TYPE_ROOT,
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
            ) as root_metadata_file, subtests.test(msg="root catalog links"):
                root_catalog_json = load(root_metadata_file)
                assert root_catalog_json[STAC_LINKS_KEY] == expected_root_links

            with smart_open(
                f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}"
                f"/{dataset.dataset_prefix}/{CATALOG_KEY}"
            ) as dataset_metadata_file, subtests.test(msg="dataset catalog links"):
                dataset_catalog_json = load(dataset_metadata_file)
                assert dataset_catalog_json[STAC_LINKS_KEY] == expected_dataset_links


@mark.infrastructure
def should_update_dataset_catalog_with_new_version_catalog(subtests: SubTests) -> None:

    dataset_version = any_dataset_version_id()
    filename = f"{any_safe_filename()}.json"
    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: dataset_version,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{filename}",
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
                        STAC_HREF_KEY: f"../{CATALOG_KEY}",
                        STAC_TYPE_KEY: "application/json",
                    },
                    {
                        STAC_REL_KEY: STAC_REL_PARENT,
                        STAC_HREF_KEY: f"../{CATALOG_KEY}",
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
                STAC_HREF_KEY: f"../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset_version}/{filename}",
                STAC_TYPE_KEY: "application/json",
            },
        ]
        expected_dataset_version_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"../../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"../{CATALOG_KEY}",
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
                                STRING_VALUE_KEY_LOWER: MESSAGE_ATTRIBUTE_TYPE_DATASET,
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
            catalog_json = load(updated_dataset_metadata_file)
            assert catalog_json[STAC_LINKS_KEY] == expected_dataset_catalog_links

        with subtests.test(msg="dataset version links"), smart_open(
            f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}"
            f"/{dataset_version_metadata.key}"
        ) as updated_dataset_metadata_file:
            version_json = load(updated_dataset_metadata_file)
            assert version_json[STAC_LINKS_KEY] == expected_dataset_version_links


@mark.infrastructure
def should_update_dataset_catalog_with_new_version_collection(subtests: SubTests) -> None:
    dataset_version = any_dataset_version_id()
    collection_filename = f"{any_safe_filename()}.json"
    item_filename = f"{any_safe_filename()}.json"

    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                STAC_ID_KEY: any_dataset_version_id(),
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{collection_filename}",
                        STAC_TYPE_KEY: "application/json",
                    },
                    {
                        STAC_REL_KEY: STAC_REL_PARENT,
                        STAC_HREF_KEY: f"./{collection_filename}",
                        STAC_TYPE_KEY: "application/json",
                    },
                ],
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{dataset_version}/{item_filename}",
    ) as item_metadata, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ID_KEY: dataset_version,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{collection_filename}",
                        STAC_TYPE_KEY: "application/json",
                    },
                    {
                        STAC_REL_KEY: STAC_REL_ITEM,
                        STAC_HREF_KEY: f"./{item_filename}",
                        STAC_TYPE_KEY: "application/json",
                    },
                ],
            }
        ),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=f"{dataset.dataset_prefix}/{dataset_version}/{collection_filename}",
    ) as dataset_version_metadata, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: dataset.dataset_prefix,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"../{CATALOG_KEY}",
                        STAC_TYPE_KEY: "application/json",
                    },
                    {
                        STAC_REL_KEY: STAC_REL_PARENT,
                        STAC_HREF_KEY: f"../{CATALOG_KEY}",
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
                STAC_HREF_KEY: f"../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset_version}/{collection_filename}",
                STAC_TYPE_KEY: "application/json",
            },
        ]
        expected_dataset_version_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"../../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_ITEM,
                STAC_HREF_KEY: f"./{item_filename}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
        ]
        expected_item_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"../../{CATALOG_KEY}",
                STAC_TYPE_KEY: "application/json",
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"./{collection_filename}",
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
                                STRING_VALUE_KEY_LOWER: MESSAGE_ATTRIBUTE_TYPE_DATASET,
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
            catalog_json = load(updated_dataset_metadata_file)
            assert catalog_json[STAC_LINKS_KEY] == expected_dataset_catalog_links

        with subtests.test(msg="dataset version links"), smart_open(
            f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}"
            f"/{dataset_version_metadata.key}"
        ) as updated_dataset_metadata_file:
            version_json = load(updated_dataset_metadata_file)
            assert version_json[STAC_LINKS_KEY] == expected_dataset_version_links

        with subtests.test(msg="item links"), smart_open(
            f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}" f"/{item_metadata.key}"
        ) as updated_item_metadata_file:
            item_json = load(updated_item_metadata_file)
            assert item_json[STAC_LINKS_KEY] == expected_item_links


def should_fail_if_unknown_sqs_message_type() -> None:
    with raises(UnhandledSQSMessageException):
        lambda_handler(
            {
                RECORDS_KEY: [
                    {
                        BODY_KEY: any_dataset_version_id(),
                        MESSAGE_ATTRIBUTES_KEY: {
                            MESSAGE_ATTRIBUTE_TYPE_KEY: {
                                STRING_VALUE_KEY_LOWER: "test",
                                DATA_TYPE_KEY: DATA_TYPE_STRING,
                            }
                        },
                    }
                ]
            },
            any_lambda_context(),
        )
