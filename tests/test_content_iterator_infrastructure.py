from copy import deepcopy
from os import environ
from typing import Any, Dict
from unittest.mock import patch

from pytest import mark

from backend.aws_keys import AWS_DEFAULT_REGION_KEY

from .aws_profile_utils import any_region_name

with patch.dict(
    environ, {AWS_DEFAULT_REGION_KEY: environ.get(AWS_DEFAULT_REGION_KEY, any_region_name())}
):
    from backend.content_iterator.task import (
        CONTENT_KEY,
        FIRST_ITEM_KEY,
        ITERATION_SIZE_KEY,
        MAX_ITERATION_SIZE,
        NEXT_ITEM_KEY,
        lambda_handler,
    )
    from backend.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
    from backend.processing_assets_model import (
        ProcessingAssetType,
        processing_assets_model_with_meta,
    )
    from backend.step_function_keys import (
        DATASET_ID_KEY,
        METADATA_URL_KEY,
        S3_ROLE_ARN_KEY,
        VERSION_ID_KEY,
    )

    from .aws_utils import any_lambda_context, any_next_item_index, any_role_arn, any_s3_url
    from .stac_generators import any_dataset_id, any_dataset_version_id, any_hex_multihash

INITIAL_EVENT: Dict[str, Any] = {
    DATASET_ID_KEY: any_dataset_id(),
    METADATA_URL_KEY: any_s3_url(),
    S3_ROLE_ARN_KEY: any_role_arn(),
    VERSION_ID_KEY: any_dataset_version_id(),
}

SUBSEQUENT_EVENT: Dict[str, Any] = {
    CONTENT_KEY: {
        FIRST_ITEM_KEY: str(any_next_item_index()),
        ITERATION_SIZE_KEY: MAX_ITERATION_SIZE,
        NEXT_ITEM_KEY: any_next_item_index(),
    },
    DATASET_ID_KEY: any_dataset_id(),
    METADATA_URL_KEY: any_s3_url(),
    S3_ROLE_ARN_KEY: any_role_arn(),
    VERSION_ID_KEY: any_dataset_version_id(),
}


@mark.infrastructure
def should_count_only_asset_files() -> None:
    # Given a single metadata and asset entry in the database
    event = deepcopy(INITIAL_EVENT)
    hash_key = (
        f"{DATASET_ID_PREFIX}{event['dataset_id']}"
        f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{event['version_id']}"
    )
    processing_assets_model = processing_assets_model_with_meta()
    processing_assets_model(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}0",
        url=any_s3_url(),
    ).save()
    processing_assets_model(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
        url=any_s3_url(),
        multihash=any_hex_multihash(),
    ).save()

    # When running the Lambda handler
    response = lambda_handler(event, any_lambda_context())

    # Then the iteration size should be one
    assert response[ITERATION_SIZE_KEY] == 1
