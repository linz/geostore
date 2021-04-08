"""List all datasets function."""

from ..api_responses import success_response
from ..datasets_model import datasets_model_with_meta
from ..types import JsonObject


def list_datasets() -> JsonObject:
    """GET: List all Datasets."""

    # list all datasets
    datasets_model_class = datasets_model_with_meta()
    datasets = datasets_model_class.scan(
        filter_condition=datasets_model_class.id.startswith("DATASET#")
    )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dataset.as_dict()
        resp_body.append(resp_item)

    return success_response(200, resp_body)
