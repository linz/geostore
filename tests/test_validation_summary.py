from backend.validation_summary.task import lambda_handler
from tests.aws_utils import any_lambda_context
from tests.stac_generators import any_dataset_version_id


def should_require_dataset_id() -> None:
    response = lambda_handler({"version_id": any_dataset_version_id()}, any_lambda_context())

    assert response == {"error message": "'dataset_id' is a required property"}
