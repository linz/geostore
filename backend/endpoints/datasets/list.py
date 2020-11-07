"""List all datasets function."""

from endpoints.datasets.model import DatasetModel
from endpoints.datasets.utils import success_response


def list_datasets():
    """GET: List all Datasets."""

    # list all datasets
    datasets = DatasetModel.scan(
        filter_condition=DatasetModel.id.startswith("DATASET#")
        & DatasetModel.type.startswith("TYPE#")
    )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dict(dataset)
        resp_item["id"] = dataset.dataset_id
        resp_item["type"] = dataset.dataset_type
        resp_body.append(resp_item)

    return success_response(200, resp_body)
