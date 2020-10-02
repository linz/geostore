#!/usr/bin/env python3

"""
CDK application entry point file.
"""

import os

from aws_cdk import core
from data_stores.data_lake_stack import DataLakeStack

if "ENVIRONMENT_TYPE" in os.environ:
    env = os.environ["ENVIRONMENT_TYPE"]
else:
    env = "dev"

app = core.App()
DataLakeStack(
    app,
    "geospatial-data-lake",
    stack_name=f"geospatial-data-lake-{env}",
    env={"region": "ap-southeast-2"},
)

app.synth()
