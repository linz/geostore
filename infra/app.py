#!/usr/bin/env python3

"""
CDK application entry point file.
"""

import os

from aws_cdk import core
from data_stores.data_lake_stack import DataLakeStack

if "DEPLOY_ENV" in os.environ:
    ENV = os.environ["DEPLOY_ENV"]
else:
    ENV = "dev"

app = core.App()
DataLakeStack(
    app,
    "geospatial-data-lake",
    stack_name=f"geospatial-data-lake-{ENV}",
    env={"region": "ap-southeast-2"},
)

app.synth()
