#!/usr/bin/env python3

"""
CDK application entry point file.
"""

from aws_cdk import core
from data_stores.data_lake_stack import DataLakeStack

app = core.App()

DataLakeStack(
    app,
    "data-lake-nonprod",
    stack_name="geospatial-data-lake-nonprod",
    env={"region": "ap-southeast-2", "account": "632223577832"},
)

DataLakeStack(
    app,
    "data-lake-prod",
    stack_name="geospatial-data-lake-prod",
    env={"region": "ap-southeast-2", "account": "715898075157"},
)

app.synth()
