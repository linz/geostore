"""List all datasets function."""

from ..api_responses import success_response
from ..datasets_model import DatasetsModel
from ..types import JsonObject


def list_datasets() -> JsonObject:
    """GET: List all Datasets."""

    # list all datasets
    datasets = DatasetsModel.scan(
        filter_condition=DatasetsModel.id.startswith("DATASET#")
        & DatasetsModel.type.startswith("TYPE#")
    )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dataset.as_dict()
        resp_body.append(resp_item)

    return success_response(200, resp_body)
