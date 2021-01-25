#!/usr/bin/env python3

"""
CDK application entry point file.
"""
from os import environ

from aws_cdk import core

from datalake.api_stack import APIStack
from datalake.backend.endpoints.utils import ENV
from datalake.networking_stack import NetworkingStack
from datalake.processing_stack import ProcessingStack
from datalake.staging_stack import StagingStack
from datalake.storage_stack import StorageStack
from datalake.users_stack import UsersStack

ENVIRONMENT_TYPE_TAG_NAME = "EnvironmentType"


def main():
    region = environ["CDK_DEFAULT_REGION"]
    account = environ["CDK_DEFAULT_ACCOUNT"]

    app = core.App()

    users = UsersStack(
        app,
        "users",
        stack_name=f"geospatial-data-lake-users-{ENV}",
        env={"region": region, "account": account},
    )

    networking = NetworkingStack(
        app,
        "networking",
        stack_name="geospatial-data-lake-networking",
        env={"region": region, "account": account},
    )

    storage = StorageStack(
        app,
        "storage",
        stack_name=f"{ENV}-geospatial-data-lake-storage",
        env={"region": region, "account": account},
        deploy_env=ENV,
    )

    ProcessingStack(
        app,
        "processing",
        stack_name=f"{ENV}-geospatial-data-lake-processing",
        env={"region": region, "account": account},
        deploy_env=ENV,
        vpc=networking.datalake_vpc,
    )

    APIStack(
        app,
        "api",
        stack_name=f"{ENV}-geospatial-data-lake-api",
        env={"region": region, "account": account},
        deploy_env=ENV,
        datasets_table=storage.datasets_table,
        users_role=users.users_role,
    )

    StagingStack(
        app,
        "staging",
        stack_name=f"{ENV}-geospatial-data-lake-staging",
        env={"region": region, "account": account},
    )

    # tag all resources in stack
    core.Tag.add(app, "CostCentre", "100005")
    core.Tag.add(app, "ApplicationName", "geospatial-data-lake")
    core.Tag.add(app, "Owner", "Bill M. Nelson")
    core.Tag.add(app, ENVIRONMENT_TYPE_TAG_NAME, ENV)
    core.Tag.add(app, "SupportType", "Dev")
    core.Tag.add(app, "HoursOfOperation", "24x7")

    app.synth()


if __name__ == "__main__":
    main()
