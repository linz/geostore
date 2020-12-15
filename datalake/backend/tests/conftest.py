"""
Pytest configuration file.
"""

import logging

import pytest

from ..endpoints.datasets.model import DatasetModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture()
def db_teardown():
    logger.debug("Removing all dataset instances before test")

    for item in DatasetModel.scan():
        item.delete()

    yield

    return True
