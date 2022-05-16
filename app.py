"""
CDK application entry point file.
"""
from aws_cdk import App, Tags

from geostore.environment import environment_name
from infrastructure.application_stack import Application
from infrastructure.constructs.batch_job_queue import APPLICATION_NAME, APPLICATION_NAME_TAG_NAME


def main() -> None:
    app = App()

    env_name = environment_name()
    Application(app, f"{env_name}-geostore")

    # tag all resources in stack
    Tags.of(app).add("CostCentre", "100005")
    Tags.of(app).add(APPLICATION_NAME_TAG_NAME, APPLICATION_NAME)
    Tags.of(app).add("Owner", "Bill M. Nelson")
    Tags.of(app).add("EnvironmentType", env_name)
    Tags.of(app).add("SupportType", "Dev")
    Tags.of(app).add("HoursOfOperation", "24x7")

    app.synth()


if __name__ == "__main__":
    main()
