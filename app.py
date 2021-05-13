#!/usr/bin/env python3

"""
CDK application entry point file.
"""
from aws_cdk.core import App, Tag

from backend.environment import ENV
from infrastructure.application_stack import Application
from infrastructure.constructs.batch_job_queue import APPLICATION_NAME, APPLICATION_NAME_TAG_NAME


def main() -> None:
    app = App()

    Application(app, f"{ENV}-datalake")

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
