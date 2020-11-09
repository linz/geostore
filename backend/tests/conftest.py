"""
Pytest configuration file.
"""

import logging
from datetime import datetime, timedelta, timezone

import pytest
from endpoints.datasets.model import DatasetModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture()
def db_prepare():
    """
    Prepare DB with some dataset records and clean it up after test is
    finished.
    """

    items = [
        DatasetModel(
            id="DATASET#111abc",
            type="TYPE#RASTER",
            title="Dataset ABC",
            owning_group="A_ABC_XYZ",
            created_at=datetime.now(timezone.utc) - timedelta(days=10),
            updated_at=datetime.now(timezone.utc) - timedelta(days=1),
        ),
        DatasetModel(
            id="DATASET#222xyz",
            type="TYPE#RASTER",
            title="Dataset XYZ",
            owning_group="A_ABC_XYZ",
            created_at=datetime.now(timezone.utc) - timedelta(days=100),
            updated_at=datetime.now(timezone.utc) - timedelta(days=10),
        ),
    ]

    logger.debug("Running DB Setup")

    with DatasetModel.batch_write() as batch:
        for item in items:
            batch.save(item)

    yield  # teardown

    logger.debug("Running DB Teardown")

    for item in DatasetModel.scan():
        item.delete()

    return True
