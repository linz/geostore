from aws_cdk import (
    aws_dynamodb,
    aws_iam,
    aws_lambda_python,
    aws_s3,
    aws_sqs,
    aws_ssm,
    aws_stepfunctions,
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_stepfunctions import Wait, WaitTime
from aws_cdk.core import Construct, Duration, Tags

from backend.api_keys import SUCCESS_KEY
from backend.content_iterator.task import (
    ASSETS_TABLE_NAME_KEY,
    CONTENT_KEY,
    FIRST_ITEM_KEY,
    ITERATION_SIZE_KEY,
    NEXT_ITEM_KEY,
    RESULTS_TABLE_NAME_KEY,
)
from backend.environment import ENV_NAME_VARIABLE_NAME
from backend.import_status.get import IMPORT_DATASET_KEY
from backend.parameter_store import ParameterName
from backend.step_function import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    METADATA_UPLOAD_KEY,
    METADATA_URL_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)

from .batch_job_queue import BatchJobQueue
from .batch_submit_job_task import BatchSubmitJobTask
from .bundled_lambda_function import BundledLambdaFunction
from .common import grant_parameter_read_access
from .import_file_function import ImportFileFunction
from .lambda_config import LAMBDA_TIMEOUT
from .lambda_task import LambdaTask
from .s3_policy import ALLOW_DESCRIBE_ANY_S3_JOB
from .table import Table


class Processing(Construct):
    def __init__(
        self,
        scope: Construct,
        stack_id: str,
        *,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
        env_name: str,
        storage_bucket: aws_s3.Bucket,
        validation_results_table: Table,
    ) -> None:
        # pylint: disable=too-many-locals, too-many-statements

        super().__init__(scope, stack_id)

        ############################################################################################
        # PROCESSING ASSETS TABLE
        processing_assets_table = Table(
            self,
            f"{env_name}-processing-assets",
            env_name=env_name,
            parameter_name=ParameterName.PROCESSING_ASSETS_TABLE_NAME,
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
        )

        ############################################################################################
        # BATCH JOB DEPENDENCIES
        batch_job_queue = BatchJobQueue(
            self,
            "batch-job-queue",
            env_name=env_name,
            processing_assets_table=processing_assets_table,
        ).job_queue

        s3_read_only_access_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "AmazonS3ReadOnlyAccess"
        )

        ############################################################################################
        # UPDATE CATALOG UPDATE MESSAGE QUEUE

        dead_letter_queue = aws_sqs.Queue(
            self,
            "dead-letter-queue",
            visibility_timeout=LAMBDA_TIMEOUT,
        )

        self.message_queue = aws_sqs.Queue(
            self,
            "update-catalog-message-queue",
            visibility_timeout=LAMBDA_TIMEOUT,
            dead_letter_queue=aws_sqs.DeadLetterQueue(max_receive_count=3, queue=dead_letter_queue),
        )
        self.message_queue_name_parameter = aws_ssm.StringParameter(
            self,
            "update-catalog-message-queue-name",
            string_value=self.message_queue.queue_name,
            description=f"Update Catalog Message Queue Name for {env_name}",
            parameter_name=ParameterName.UPDATE_CATALOG_MESSAGE_QUEUE_NAME.value,
        )

        populate_catalog_lambda = BundledLambdaFunction(
            self,
            "populate-catalog-bundled-lambda-function",
            directory="populate_catalog",
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
            botocore_lambda_layer=botocore_lambda_layer,
        )

        self.message_queue.grant_consume_messages(populate_catalog_lambda)
        populate_catalog_lambda.add_event_source(
            SqsEventSource(self.message_queue, batch_size=1)  # type: ignore[arg-type]
        )

        ############################################################################################
        # STATE MACHINE TASKS

        check_stac_metadata_task = LambdaTask(
            self,
            "check-stac-metadata-task",
            directory="check_stac_metadata",
            botocore_lambda_layer=botocore_lambda_layer,
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
        )
        assert check_stac_metadata_task.lambda_function.role
        check_stac_metadata_task.lambda_function.role.add_managed_policy(
            policy=s3_read_only_access_policy
        )

        for table in [processing_assets_table, validation_results_table]:
            table.grant_read_write_data(check_stac_metadata_task.lambda_function)
            table.grant(
                check_stac_metadata_task.lambda_function,
                "dynamodb:DescribeTable",
            )

        content_iterator_task = LambdaTask(
            self,
            "content-iterator-task",
            directory="content_iterator",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path=f"$.{CONTENT_KEY}",
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
        )

        check_files_checksums_directory = "check_files_checksums"
        check_files_checksums_default_payload_object = {
            f"{DATASET_ID_KEY}.$": f"$.{DATASET_ID_KEY}",
            f"{VERSION_ID_KEY}.$": f"$.{VERSION_ID_KEY}",
            f"{METADATA_URL_KEY}.$": f"$.{METADATA_URL_KEY}",
            f"{FIRST_ITEM_KEY}.$": f"$.{CONTENT_KEY}.{FIRST_ITEM_KEY}",
            f"{ASSETS_TABLE_NAME_KEY}.$": f"$.{CONTENT_KEY}.{ASSETS_TABLE_NAME_KEY}",
            f"{RESULTS_TABLE_NAME_KEY}.$": f"$.{CONTENT_KEY}.{RESULTS_TABLE_NAME_KEY}",
        }
        check_files_checksums_single_task = BatchSubmitJobTask(
            self,
            "check-files-checksums-single-task",
            env_name=env_name,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=[
                "--dataset-id",
                f"Ref::{DATASET_ID_KEY}",
                "--version-id",
                f"Ref::{VERSION_ID_KEY}",
                "--first-item",
                f"Ref::{FIRST_ITEM_KEY}",
                "--assets-table-name",
                f"Ref::{ASSETS_TABLE_NAME_KEY}",
                "--results-table-name",
                f"Ref::{RESULTS_TABLE_NAME_KEY}",
            ],
        )
        array_size = int(
            aws_stepfunctions.JsonPath.number_at(f"$.{CONTENT_KEY}.{ITERATION_SIZE_KEY}")
        )
        check_files_checksums_array_task = BatchSubmitJobTask(
            self,
            "check-files-checksums-array-task",
            env_name=env_name,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=[
                "--dataset-id",
                f"Ref::{DATASET_ID_KEY}",
                "--version-id",
                f"Ref::{VERSION_ID_KEY}",
                "--first-item",
                f"Ref::{FIRST_ITEM_KEY}",
                "--assets-table-name",
                f"Ref::{ASSETS_TABLE_NAME_KEY}",
                "--results-table-name",
                f"Ref::{RESULTS_TABLE_NAME_KEY}",
            ],
            array_size=array_size,
        )

        for reader in [
            content_iterator_task.lambda_function,
            check_files_checksums_single_task.job_role,
            check_files_checksums_array_task.job_role,
        ]:
            processing_assets_table.grant_read_data(reader)  # type: ignore[arg-type]
            processing_assets_table.grant(
                reader, "dynamodb:DescribeTable"  # type: ignore[arg-type]
            )

        for writer in [
            check_files_checksums_single_task.job_role,
            check_files_checksums_array_task.job_role,
        ]:
            validation_results_table.grant_read_write_data(writer)  # type: ignore[arg-type]
            validation_results_table.grant(
                writer, "dynamodb:DescribeTable"  # type: ignore[arg-type]
            )

        validation_summary_task = LambdaTask(
            self,
            "validation-summary-task",
            directory="validation_summary",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path=f"$.{VALIDATION_KEY}",
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
        )
        validation_results_table.grant_read_data(validation_summary_task.lambda_function)
        validation_results_table.grant(
            validation_summary_task.lambda_function, "dynamodb:DescribeTable"
        )

        import_dataset_role = aws_iam.Role(
            self,
            "import-dataset",
            assumed_by=aws_iam.ServicePrincipal(  # type: ignore[arg-type]
                "batchoperations.s3.amazonaws.com"
            ),
        )

        import_asset_file_function = ImportFileFunction(
            self,
            directory="import_asset_file",
            invoker=import_dataset_role,
            env_name=env_name,
            botocore_lambda_layer=botocore_lambda_layer,
        )
        import_metadata_file_function = ImportFileFunction(
            self,
            directory="import_metadata_file",
            invoker=import_dataset_role,
            env_name=env_name,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        import_dataset_task = LambdaTask(
            self,
            "import-dataset-task",
            directory="import_dataset",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path=f"$.{IMPORT_DATASET_KEY}",
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
        )

        import_dataset_task.lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(
                resources=[import_dataset_role.role_arn],
                actions=["iam:PassRole"],
            ),
        )
        import_dataset_task.lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(resources=["*"], actions=["s3:CreateJob"])
        )

        for table in [processing_assets_table]:
            table.grant_read_data(import_dataset_task.lambda_function)
            table.grant(import_dataset_task.lambda_function, "dynamodb:DescribeTable")

        # Import status check
        wait_before_upload_status_check = Wait(
            self,
            "wait-before-upload-status-check",
            time=WaitTime.duration(Duration.seconds(10)),
        )
        upload_status_task = LambdaTask(
            self,
            "upload-status",
            directory="upload_status",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path="$.upload_status",
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
        )
        validation_results_table.grant_read_data(upload_status_task.lambda_function)
        validation_results_table.grant(upload_status_task.lambda_function, "dynamodb:DescribeTable")

        upload_status_task.lambda_function.add_to_role_policy(ALLOW_DESCRIBE_ANY_S3_JOB)

        # Parameters
        import_asset_file_function_arn_parameter = aws_ssm.StringParameter(
            self,
            "import asset file function arn",
            string_value=import_asset_file_function.function_arn,
            description=f"Import asset file function ARN for {env_name}",
            parameter_name=ParameterName.PROCESSING_IMPORT_ASSET_FILE_FUNCTION_TASK_ARN.value,
        )
        import_metadata_file_function_arn_parameter = aws_ssm.StringParameter(
            self,
            "import metadata file function arn",
            string_value=import_metadata_file_function.function_arn,
            description=f"Import metadata file function ARN for {env_name}",
            parameter_name=ParameterName.PROCESSING_IMPORT_METADATA_FILE_FUNCTION_TASK_ARN.value,
        )

        import_dataset_role_arn_parameter = aws_ssm.StringParameter(
            self,
            "import dataset role arn",
            string_value=import_dataset_role.role_arn,
            description=f"Import dataset role ARN for {env_name}",
            parameter_name=ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN.value,
        )

        update_dataset_catalog = LambdaTask(
            self,
            "update-dataset-catalog",
            directory="update_dataset_catalog",
            botocore_lambda_layer=botocore_lambda_layer,
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
        )

        for storage_writer in [
            import_dataset_role,
            import_dataset_task.lambda_function,
            import_asset_file_function,
            import_metadata_file_function,
            populate_catalog_lambda,
            update_dataset_catalog.lambda_function,
        ]:
            storage_bucket.grant_read_write(storage_writer)  # type: ignore[arg-type]

        grant_parameter_read_access(
            {
                import_asset_file_function_arn_parameter: [import_dataset_task.lambda_function],
                import_dataset_role_arn_parameter: [import_dataset_task.lambda_function],
                import_metadata_file_function_arn_parameter: [import_dataset_task.lambda_function],
                processing_assets_table.name_parameter: [
                    check_stac_metadata_task.lambda_function,
                    content_iterator_task.lambda_function,
                    import_dataset_task.lambda_function,
                ],
                validation_results_table.name_parameter: [
                    check_stac_metadata_task.lambda_function,
                    content_iterator_task.lambda_function,
                    validation_summary_task.lambda_function,
                    upload_status_task.lambda_function,
                ],
            }
        )

        success_task = aws_stepfunctions.Succeed(self, "success")
        upload_failure = aws_stepfunctions.Fail(self, "upload failure")
        validation_failure = aws_stepfunctions.Succeed(self, "validation failure")

        ############################################################################################
        # STATE MACHINE
        dataset_version_creation_definition = (
            check_stac_metadata_task.next(content_iterator_task)
            .next(
                aws_stepfunctions.Choice(  # type: ignore[arg-type]
                    self, "check_files_checksums_maybe_array"
                )
                .when(
                    aws_stepfunctions.Condition.number_equals(
                        f"$.{CONTENT_KEY}.{ITERATION_SIZE_KEY}", 1
                    ),
                    check_files_checksums_single_task.batch_submit_job,
                )
                .otherwise(check_files_checksums_array_task.batch_submit_job)
                .afterwards()
            )
            .next(
                aws_stepfunctions.Choice(self, "content_iteration_finished")
                .when(
                    aws_stepfunctions.Condition.number_equals(
                        f"$.{CONTENT_KEY}.{NEXT_ITEM_KEY}", -1
                    ),
                    validation_summary_task.next(
                        aws_stepfunctions.Choice(  # type: ignore[arg-type]
                            self, "validation_successful"
                        )
                        .when(
                            aws_stepfunctions.Condition.boolean_equals(
                                f"$.{VALIDATION_KEY}.{SUCCESS_KEY}", True
                            ),
                            import_dataset_task.next(
                                wait_before_upload_status_check  # type: ignore[arg-type]
                            )
                            .next(upload_status_task)
                            .next(
                                aws_stepfunctions.Choice(
                                    self, "import_completed"  # type: ignore[arg-type]
                                )
                                .when(
                                    aws_stepfunctions.Condition.and_(
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{ASSET_UPLOAD_KEY}.status", "Complete"
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{METADATA_UPLOAD_KEY}.status",
                                            "Complete",
                                        ),
                                    ),
                                    update_dataset_catalog.next(
                                        success_task  # type: ignore[arg-type]
                                    ),
                                )
                                .when(
                                    aws_stepfunctions.Condition.or_(
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{ASSET_UPLOAD_KEY}.status",
                                            "Cancelled",
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{ASSET_UPLOAD_KEY}.status", "Failed"
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{METADATA_UPLOAD_KEY}.status",
                                            "Cancelled",
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{METADATA_UPLOAD_KEY}.status",
                                            "Failed",
                                        ),
                                    ),
                                    upload_failure,  # type: ignore[arg-type]
                                )
                                .otherwise(
                                    wait_before_upload_status_check  # type: ignore[arg-type]
                                )
                            ),
                        )
                        .otherwise(validation_failure)  # type: ignore[arg-type]
                    ),
                )
                .otherwise(content_iterator_task)
            )
        )

        self.state_machine = aws_stepfunctions.StateMachine(
            self,
            f"{env_name}-dataset-version-creation",
            definition=dataset_version_creation_definition,  # type: ignore[arg-type]
        )

        self.state_machine_parameter = aws_ssm.StringParameter(
            self,
            "state machine arn",
            description=f"State machine ARN for {env_name}",
            parameter_name=ParameterName.PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN.value,  # pylint:disable=line-too-long
            string_value=self.state_machine.state_machine_arn,
        )

        Tags.of(self).add("ApplicationLayer", "processing")  # type: ignore[arg-type]
