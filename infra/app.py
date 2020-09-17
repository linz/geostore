#!/usr/bin/env python3

from aws_cdk import core
from data_stores.raster_stack import RasterStack

app = core.App()

RasterStack(
    app,
    "raster-nonprod",
    stack_name="geospatial-data-lake-raster-nonprod",
    env={"region": "ap-southeast-2", "account": "632223577832"},
)

RasterStack(
    app,
    "raster-prod",
    stack_name="geospatial-data-lake-raster-prod",
    env={"region": "ap-southeast-2", "account": "715898075157"},
)

app.synth()
