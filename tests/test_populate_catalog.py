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
    STAC_MEDIA_TYPE_GEOJSON,
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

from .aws_utils import Dataset, S3Object, any_lambda_context, delete_s3_key
from .file_utils import json_dict_to_file_object
from .general_generators import any_safe_filename
from .stac_generators import any_dataset_version_id
from .stac_objects import (
    MINIMAL_VALID_STAC_CATALOG_OBJECT,
    MINIMAL_VALID_STAC_COLLECTION_OBJECT,
    MINIMAL_VALID_STAC_ITEM_OBJECT,
)


@mark.infrastructure
def should_create_new_root_catalog_if_doesnt_exist(subtests: SubTests, s3_client: S3Client) -> None:
    dataset_version = any_dataset_version_id()
    catalog_filename = f"{any_safe_filename()}.json"
    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: dataset_version,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{catalog_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{catalog_filename}",
    ) as dataset_metadata:

        expected_root_catalog_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{catalog_filename}",
                STAC_TITLE_KEY: dataset.title,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
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

        try:
            lambda_handler(
                {RECORDS_KEY: [{BODY_KEY: dataset_metadata.key}]},
                any_lambda_context(),
            )

            with subtests.test(msg="catalog links"), smart_open.open(
                f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{CATALOG_FILENAME}",
                mode="rb",
            ) as new_root_metadata_file:
                catalog_json = load(new_root_metadata_file)
                assert catalog_json[STAC_LINKS_KEY] == expected_root_catalog_links

            with subtests.test(msg="dataset links"), smart_open.open(
                f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}"
                f"/{dataset_metadata.key}",
                mode="rb",
            ) as updated_dataset_metadata_file:
                dataset_json = load(updated_dataset_metadata_file)
                assert dataset_json[STAC_LINKS_KEY] == expected_dataset_links

        finally:
            delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, CATALOG_FILENAME, s3_client)


@mark.infrastructure
def should_update_root_catalog_with_new_version_catalog(subtests: SubTests) -> None:
    collection_filename = f"{any_safe_filename()}.json"
    item_filename = f"{any_safe_filename()}.json"
    dataset_version = any_dataset_version_id()
    catalog_filename = f"{any_safe_filename()}.json"
    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                STAC_ID_KEY: any_dataset_version_id(),
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{catalog_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                    {
                        STAC_REL_KEY: STAC_REL_PARENT,
                        STAC_HREF_KEY: f"./{collection_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{item_filename}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_ID_KEY: dataset_version,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{catalog_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                    {
                        STAC_REL_KEY: STAC_REL_ITEM,
                        STAC_HREF_KEY: f"./{item_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_GEOJSON,
                    },
                    {
                        STAC_REL_KEY: STAC_REL_PARENT,
                        STAC_HREF_KEY: f"./{catalog_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{collection_filename}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: dataset_version,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{catalog_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                    {
                        STAC_REL_KEY: STAC_REL_CHILD,
                        STAC_HREF_KEY: f"./{collection_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{catalog_filename}",
    ) as dataset_metadata, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: ROOT_CATALOG_ID,
                STAC_DESCRIPTION_KEY: ROOT_CATALOG_DESCRIPTION,
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=CATALOG_FILENAME,
    ):

        expected_root_catalog_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{catalog_filename}",
                STAC_TITLE_KEY: dataset.title,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
        ]
        expected_dataset_version_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{collection_filename}",
                STAC_TITLE_KEY: dataset.title,
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
            {RECORDS_KEY: [{BODY_KEY: dataset_metadata.key}]},
            any_lambda_context(),
        )

        with subtests.test(msg="root catalog links"), smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{CATALOG_FILENAME}",
            mode="rb",
        ) as updated_root_metadata_file:
            catalog_json = load(updated_root_metadata_file)
            assert catalog_json[STAC_LINKS_KEY] == expected_root_catalog_links

        with subtests.test(msg="dataset version links"), smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}"
            f"/{dataset_metadata.key}",
            mode="rb",
        ) as updated_dataset_metadata_file:
            version_json = load(updated_dataset_metadata_file)
            assert version_json[STAC_LINKS_KEY] == expected_dataset_version_links


@mark.infrastructure
def should_update_root_catalog_with_new_version_collection(subtests: SubTests) -> None:
    # pylint: disable=too-many-locals
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
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                    {
                        STAC_REL_KEY: STAC_REL_PARENT,
                        STAC_HREF_KEY: f"./{collection_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{item_filename}",
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
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                    {
                        STAC_REL_KEY: STAC_REL_ITEM,
                        STAC_HREF_KEY: f"./{item_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_GEOJSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{collection_filename}",
    ) as dataset_metadata, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: ROOT_CATALOG_ID,
                STAC_DESCRIPTION_KEY: ROOT_CATALOG_DESCRIPTION,
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    }
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=CATALOG_FILENAME,
    ):
        expected_root_catalog_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{collection_filename}",
                STAC_TITLE_KEY: dataset.title,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
        ]
        expected_dataset_version_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_ITEM,
                STAC_HREF_KEY: f"./{item_filename}",
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_GEOJSON,
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
        ]
        expected_item_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_PARENT,
                STAC_HREF_KEY: f"./{collection_filename}",
                STAC_TITLE_KEY: dataset.title,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
        ]

        lambda_handler(
            {RECORDS_KEY: [{BODY_KEY: dataset_metadata.key}]},
            any_lambda_context(),
        )

        with subtests.test(msg="root catalog links"), smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{CATALOG_FILENAME}",
            mode="rb",
        ) as updated_root_metadata_file:
            catalog_json = load(updated_root_metadata_file)
            assert catalog_json[STAC_LINKS_KEY] == expected_root_catalog_links

        with subtests.test(msg="dataset version links"), smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}"
            f"/{dataset_metadata.key}",
            mode="rb",
        ) as updated_dataset_metadata_file:
            version_json = load(updated_dataset_metadata_file)
            assert version_json[STAC_LINKS_KEY] == expected_dataset_version_links

        with subtests.test(msg="item links"), smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{item_metadata.key}",
            mode="rb",
        ) as updated_item_metadata_file:
            item_json = load(updated_item_metadata_file)
            assert item_json[STAC_LINKS_KEY] == expected_item_links


@mark.infrastructure
def should_not_add_duplicate_child_link_to_root(subtests: SubTests) -> None:
    dataset_version = any_dataset_version_id()
    catalog_filename = f"{any_safe_filename()}.json"
    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: dataset_version,
                STAC_TITLE_KEY: dataset.title,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{catalog_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    }
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{catalog_filename}",
    ) as dataset_metadata, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_ID_KEY: ROOT_CATALOG_ID,
                STAC_DESCRIPTION_KEY: ROOT_CATALOG_DESCRIPTION,
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_LINKS_KEY: [
                    {
                        STAC_REL_KEY: STAC_REL_ROOT,
                        STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                    {
                        STAC_REL_KEY: STAC_REL_CHILD,
                        STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{catalog_filename}",
                        STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=CATALOG_FILENAME,
    ):

        expected_root_catalog_links: JsonList = [
            {
                STAC_REL_KEY: STAC_REL_ROOT,
                STAC_HREF_KEY: f"./{CATALOG_FILENAME}",
                STAC_TITLE_KEY: ROOT_CATALOG_TITLE,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
            {
                STAC_REL_KEY: STAC_REL_CHILD,
                STAC_HREF_KEY: f"./{dataset.dataset_prefix}/{catalog_filename}",
                STAC_TITLE_KEY: dataset.title,
                STAC_TYPE_KEY: STAC_MEDIA_TYPE_JSON,
            },
        ]
        expected_dataset_version_links: JsonList = [
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
            {RECORDS_KEY: [{BODY_KEY: dataset_metadata.key}]},
            any_lambda_context(),
        )

        with subtests.test(msg="root catalog links"), smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{CATALOG_FILENAME}",
            mode="rb",
        ) as updated_root_metadata_file:
            catalog_json = load(updated_root_metadata_file)
            assert catalog_json[STAC_LINKS_KEY] == expected_root_catalog_links

        with subtests.test(msg="dataset version links"), smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}"
            f"/{dataset_metadata.key}",
            mode="rb",
        ) as updated_dataset_metadata_file:
            version_json = load(updated_dataset_metadata_file)
            assert version_json[STAC_LINKS_KEY] == expected_dataset_version_links
