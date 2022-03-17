"""
CDK application entry point file.
"""
from aws_cdk.core import App, Tag

from geostore.environment import environment_name
from infrastructure.application_stack import Application
from infrastructure.constructs.batch_job_queue import APPLICATION_NAME, APPLICATION_NAME_TAG_NAME
from infrastructure.logging_stack import Logging


def main() -> None:
    app = App()

    env_name = environment_name()
    Application(app, f"{env_name}-geostore")
    Logging(app, "geostore-logging")

    # tag all resources in stack
    Tag.add(app, "CostCentre", "100005")
    Tag.add(app, APPLICATION_NAME_TAG_NAME, APPLICATION_NAME)
    Tag.add(app, "Owner", "Bill M. Nelson")
    Tag.add(app, "EnvironmentType", env_name)
    Tag.add(app, "SupportType", "Dev")
    Tag.add(app, "HoursOfOperation", "24x7")

    app.synth()


if __name__ == "__main__":
    main()
