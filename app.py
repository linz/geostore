#!/usr/bin/env python3

"""
CDK application entry point file.
"""
from os import environ

from aws_cdk import core

from backend.utils import ENV
from infrastructure.api_stack import APIStack
from infrastructure.constructs.batch_job_queue import APPLICATION_NAME, APPLICATION_NAME_TAG_NAME
from infrastructure.processing_stack import ProcessingStack
from infrastructure.staging_stack import StagingStack
from infrastructure.storage_stack import StorageStack
from infrastructure.users_stack import UsersStack

ENVIRONMENT_TYPE_TAG_NAME = "EnvironmentType"


def main() -> None:
    region = environ["CDK_DEFAULT_REGION"]
    account = environ["CDK_DEFAULT_ACCOUNT"]

    app = core.App()

    users = UsersStack(
        app,
        "users",
        stack_name=f"{ENV}-geospatial-data-lake-users",
        env={"region": region, "account": account},
    )

    storage = StorageStack(
        app,
        "storage",
        stack_name=f"{ENV}-geospatial-data-lake-storage",
        env={"region": region, "account": account},
        deploy_env=ENV,
    )

    staging = StagingStack(
        app,
        "staging",
        stack_name=f"{ENV}-geospatial-data-lake-staging",
        env={"region": region, "account": account},
    )

    processing = ProcessingStack(
        app,
        "processing",
        stack_name=f"{ENV}-geospatial-data-lake-processing",
        env={"region": region, "account": account},
        deploy_env=ENV,
        staging_bucket=staging.staging_bucket,
    )
    processing.add_dependency(storage)

    APIStack(
        app,
        "api",
        stack_name=f"{ENV}-geospatial-data-lake-api",
        env={"region": region, "account": account},
        deploy_env=ENV,
        datasets_table=storage.datasets_table,
        users_role=users.users_role,
    ).add_dependency(processing)

    # tag all resources in stack
    core.Tag.add(app, "CostCentre", "100005")
    core.Tag.add(app, APPLICATION_NAME_TAG_NAME, APPLICATION_NAME)
    core.Tag.add(app, "Owner", "Bill M. Nelson")
    core.Tag.add(app, ENVIRONMENT_TYPE_TAG_NAME, ENV)
    core.Tag.add(app, "SupportType", "Dev")
    core.Tag.add(app, "HoursOfOperation", "24x7")

    app.synth()


if __name__ == "__main__":
    main()
