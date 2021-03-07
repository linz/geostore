"""List all datasets function."""

from ..api_responses import JsonObject, success_response
from ..dataset_model import DatasetModel


def list_datasets() -> JsonObject:
    """GET: List all Datasets."""

    # list all datasets
    datasets = DatasetModel.scan(
        filter_condition=DatasetModel.id.startswith("DATASET#")
        & DatasetModel.type.startswith("TYPE#")
    )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dataset.serialize()
        resp_body.append(resp_item)

    return success_response(200, resp_body)
