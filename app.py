#!/usr/bin/env python3

"""
CDK application entry point file.
"""
from os import environ

from aws_cdk.core import App, Environment, Tag

from backend.environment import ENV
from infrastructure.api_stack import APIStack
from infrastructure.constructs.batch_job_queue import APPLICATION_NAME, APPLICATION_NAME_TAG_NAME
from infrastructure.lambda_layers_stack import LambdaLayersStack
from infrastructure.lds import LDSStack
from infrastructure.processing_stack import ProcessingStack
from infrastructure.staging_stack import StagingStack
from infrastructure.storage_stack import StorageStack


def main() -> None:
    app = App()

    environment = Environment(
        account=environ["CDK_DEFAULT_ACCOUNT"], region=environ["CDK_DEFAULT_REGION"]
    )

    storage = StorageStack(
        app,
        "storage",
        deploy_env=ENV,
        env=environment,
        stack_name=f"{ENV}-geospatial-data-lake-storage",
    )

    StagingStack(
        app,
        "staging",
        deploy_env=ENV,
        env=environment,
        stack_name=f"{ENV}-geospatial-data-lake-staging",
    )

    lambda_layers = LambdaLayersStack(
        app,
        "lambda-layers",
        deploy_env=ENV,
        env=environment,
        stack_name=f"{ENV}-geospatial-data-lake-lambda-layers",
    )

    processing = ProcessingStack(
        app,
        "processing",
        botocore_lambda_layer=lambda_layers.botocore,
        datasets_table=storage.datasets_table,
        deploy_env=ENV,
        storage_bucket=storage.storage_bucket,
        storage_bucket_parameter=storage.storage_bucket_parameter,
        validation_results_table=storage.validation_results_table,
        env=environment,
        stack_name=f"{ENV}-geospatial-data-lake-processing",
    )

    APIStack(
        app,
        "api",
        botocore_lambda_layer=lambda_layers.botocore,
        datasets_table=storage.datasets_table,
        deploy_env=ENV,
        state_machine=processing.state_machine,
        state_machine_parameter=processing.state_machine_parameter,
        storage_bucket=storage.storage_bucket,
        storage_bucket_parameter=storage.storage_bucket_parameter,
        validation_results_table=storage.validation_results_table,
        env=environment,
        stack_name=f"{ENV}-geospatial-data-lake-api",
    )

    if app.node.try_get_context("enableLDSAccess"):
        LDSStack(
            app,
            "lds",
            deploy_env=ENV,
            storage_bucket=storage.storage_bucket,
            env=environment,
            stack_name=f"{ENV}-geospatial-data-lake-lds",
        )

    # tag all resources in stack
    Tag.add(app, "CostCentre", "100005")
    Tag.add(app, APPLICATION_NAME_TAG_NAME, APPLICATION_NAME)
    Tag.add(app, "Owner", "Bill M. Nelson")
    Tag.add(app, "EnvironmentType", ENV)
    Tag.add(app, "SupportType", "Dev")
    Tag.add(app, "HoursOfOperation", "24x7")

    app.synth()


if __name__ == "__main__":
    main()
