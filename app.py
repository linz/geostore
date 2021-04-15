#!/usr/bin/env python3

"""
CDK application entry point file.
"""
from os import environ

from aws_cdk.core import App, Environment, Tag

from backend.environment import ENV
from infrastructure.api_stack import APIStack
from infrastructure.constructs.batch_job_queue import APPLICATION_NAME, APPLICATION_NAME_TAG_NAME
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
        stack_name=f"{ENV}-geospatial-data-lake-storage",
        env=environment,
        deploy_env=ENV,
    )

    StagingStack(
        app,
        "staging",
        deploy_env=ENV,
        stack_name=f"{ENV}-geospatial-data-lake-staging",
        env=environment,
    )

    processing = ProcessingStack(
        app,
        "processing",
        stack_name=f"{ENV}-geospatial-data-lake-processing",
        env=environment,
        deploy_env=ENV,
        storage_bucket=storage.storage_bucket,
        storage_bucket_parameter=storage.storage_bucket_parameter,
    )

    APIStack(
        app,
        "api",
        stack_name=f"{ENV}-geospatial-data-lake-api",
        env=environment,
        deploy_env=ENV,
        datasets_table=storage.datasets_table,
        validation_results_table=processing.validation_results_table,
        state_machine=processing.state_machine,
        state_machine_parameter=processing.state_machine_parameter,
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
