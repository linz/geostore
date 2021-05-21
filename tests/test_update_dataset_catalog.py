from pytest import mark

from backend.step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from backend.update_dataset_catalog.task import lambda_handler
from tests.aws_utils import Dataset, any_lambda_context, any_s3_url
from tests.stac_generators import any_dataset_version_id


@mark.infrastructure
def should_update_dataset_catalog_with_new_version() -> None:
    with Dataset() as dataset:
        lambda_handler(
            {
                DATASET_ID_KEY: dataset.dataset_id,
                VERSION_ID_KEY: any_dataset_version_id(),
                METADATA_URL_KEY: any_s3_url(),
            },
            any_lambda_context(),
        )
