#!/usr/bin/env python3

"""
CDK application entry point file.
"""
from os import environ

from aws_cdk import core

from datalake.api_stack import APIStack
from datalake.networking_stack import NetworkingStack
from datalake.processing_stack import ProcessingStack
from datalake.staging_stack import StagingStack
from datalake.storage_stack import StorageStack
from datalake.users_stack import UsersStack

ENVIRONMENT_TYPE_TAG_NAME = "EnvironmentType"
ENV = environ.get("DEPLOY_ENV", "dev")


def str2bool(value: str) -> bool:
    if value.upper() == "TRUE":
        return True
    if value.upper() == "FALSE":
        return False
    raise ValueError(f"Not a valid boolean: '{value}'")


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
        stack_name=f"geospatial-data-lake-networking-{ENV}",
        env={"region": region, "account": account},
        deploy_env=ENV,
        use_existing_vpc=str2bool(environ.get("DATALAKE_USE_EXISTING_VPC", "false")),
    )

    storage = StorageStack(
        app,
        "storage",
        stack_name=f"geospatial-data-lake-storage-{ENV}",
        env={"region": region, "account": account},
        deploy_env=ENV,
    )

    ProcessingStack(
        app,
        "processing",
        stack_name=f"geospatial-data-lake-processing-{ENV}",
        env={"region": region, "account": account},
        deploy_env=ENV,
        vpc=networking.datalake_vpc,
    )

    APIStack(
        app,
        "api",
        stack_name=f"geospatial-data-lake-api-{ENV}",
        env={"region": region, "account": account},
        datasets_table=storage.datasets_table,
        users_role=users.users_role,
    )

    StagingStack(
        app,
        "staging",
        stack_name=f"geospatial-data-lake-staging-{ENV}",
        env={"region": region, "account": account},
        deploy_env=ENV,
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
