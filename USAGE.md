# Using Geostore

The purpose of Geostore is to store geospatial datasets. This document should provide the technical
know-how to create and maintain such datasets.

The keywords "must", "must not", "required", "shall", "shall not", "should", "should not",
"recommended", "may", and "optional" in this document are to be interpreted as described in
[RFC 2119](https://tools.ietf.org/html/rfc2119).

## Prerequisites

### Geostore account and resource names

You'll need the following information to start using Geostore:

-  Geostore AWS account ID (`GEOSTORE_AWS_ACCOUNT_ID`)
-  Geostore user role name (`GEOSTORE_USER_ROLE_NAME`)

Replace the `GEOSTORE_…` strings in the following documentation with the actual values given by the
Geostore instance maintainer.

### Dataset source S3 bucket

Geostore needs to be able to read all the files in the dataset in the source bucket. This is
achieved using role assumption: you need to create a role which `GEOSTORE_AWS_ACCOUNT_ID` is allowed
to assume (see the policy below) and which has read access to the dataset files in your bucket. You
then pass the ARN of that role into the
[dataset version endpoint](#Dataset-Version-creation-request).

Technical note: Using role assumption means that it's easy for you to verify that the role you have
created has access to the right things, without having to ask the Geostore team to verify it for
you. Only the role assumption itself needs to be checked by the Geostore team.

Template trust policy on your role:

```json
{
   "Version": "2012-10-17",
   "Statement": [
      {
         "Action": "sts:AssumeRole",
         "Effect": "Allow",
         "Resource": "arn:aws:iam::<GEOSTORE_AWS_ACCOUNT_ID>:role/<GEOSTORE_USER_ROLE_NAME>"
      }
   ]
}
```

Template dataset source S3 bucket policy:

```json
{
   "Id": "<ID>",
   "Version": "2012-10-17",
   "Statement": [
      {
         "Sid": "<STATEMENT_ID>",
         "Action": ["s3:GetObject", "s3:GetObjectAcl", "s3:GetObjectTagging"],
         "Effect": "Allow",
         "Resource": "arn:aws:s3:::<YOUR_BUCKET>/<YOUR_DATASET>/*",
         "Principal": {
            "AWS": "arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<YOUR_ROLE_NAME>"
         }
      }
   ]
}
```

### Dataset format

A dataset consists of a set of files in Amazon Web Services Simple Storage Service (AWS S3). The
dataset consists of geospatial metadata files in
[SpatioTemporal Asset Catalogs (STAC)](https://stacspec.org/) format and data files, which are
called "assets" in STAC.

Geostore performs many checks on datasets. If any of the checks fail the dataset will not be
imported, so it's important to know what they are. The following list is a reference of all the
checks which are currently in place.

-  Every metadata file must follow the
   [STAC Collection Specification](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md).
-  Every metadata URL (in the
   [`links` property](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#link-object))
   must be an S3 URL of the form `s3://BUCKET_NAME/KEY`, for example,
   `s3://my-bucket/some-path/foo.tif`.
-  Every asset (in the
   [`assets` property](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#assets))
   must have:
   -  an S3
      [URL](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-object),
      as defined above
   -  a [checksum](https://github.com/stac-extensions/file#checksums) corresponding to the contents
      of the asset file
-  Every metadata and asset file must be in the same S3 bucket.
-  Every metadata and asset URL must be readable by Geostore.
-  A dataset _may_ refer to the same asset more than once. All references to the same asset must
   have the same multihash. That is, having a SHA-1 and a SHA-256 checksum for the same file will be
   considered invalid, even if both checksums are valid. This is to enable a simpler checksum
   validation.

## Authentication and authorization

Geostore allows read/write access for users authorized by SAML identity provider
(`GEOSTORE_SAML_IDENTITY_PROVIDER_ARN`) configured during deployment time (see
[README](README.md#aws-infrastructure-deployment-cdk-stack)).

Example of AWS service account authentication and authorization in to Geostore users role via Azure:

-  Log in to Geostore AWS account

   ```bash
   aws-azure-login --profile=li-geostore-prod
   ```

-  Set AWS profile for subsequent commands

   ```bash
   export AWS_PROFILE=li-geostore-prod
   ```

Example of assuming the Geostore roles from a LINZ AWS account:

-  Choose any LINZ AWS account to assume the role from

   ```bash
   export SOURCE_PROFILE=<YOUR_AWS_SOURCE_PROFILE> # e.g. li-small-apps-nonprod
   ```

-  Log in to any LINZ AWS account:

   ```bash
   aws-azure-login --profile="$SOURCE_PROFILE"
   ```

-  Assume either Geostore role:

   -  API user:

      ```bash
      aws sts assume-role --role-arn="$(aws iam get-role --role-name=api-users | jq --raw-output .Role.Arn)" --role-session-name="$USER"
      ```

   -  S3 user:

      ```bash
      aws sts assume-role --role-arn="$(aws iam get-role --role-name=s3-users | jq --raw-output .Role.Arn)" --role-session-name="$USER"
      ```

## Use

You can communicate with a Geostore instance using either the low-level application programming
interface (API) or the high-level command-line interface (CLI). The following documentation explains
how to work with both. For an example of working with Python, see the
`should_successfully_run_dataset_version_creation_process_with_multiple_assets` function in
`tests/test_processing_stack.py`.

### API introduction

The API endpoints include:

-  [Dataset space](https://ap-southeast-2.console.aws.amazon.com/lambda/home?region=ap-southeast-2#functions/datasets),
   to create, get, update or delete individual datasets, and to list all datasets.
-  [Dataset versions](https://ap-southeast-2.console.aws.amazon.com/lambda/home?region=ap-southeast-2#functions/dataset-versions),
   to create new versions of datasets. The S3 files which constitute the dataset are all linked to a
   specific dataset version.
-  [Import status](https://ap-southeast-2.console.aws.amazon.com/lambda/home?region=ap-southeast-2#functions/import-status),
   to get information about the status of dataset version import, including errors and issues.

These are implemented as AWS Lambda functions, which means they can be run ("invoked") either via
the AWS web interface (links above) or via any tool using the AWS API, such as the
[official AWS CLI](https://aws.amazon.com/cli/). AWS CLI API requests have the form
`aws lambda invoke --function-name=LAMBDA-FUNCTION-NAME --payload=JSON /dev/stdout`.

### CLI introduction

The CLI is a convenience function built on top of the API for easier use. Rather than using JSON as
the primary input and output format it takes only the minimal input necessary and prints only the
minimal new information. In cases of more complex output like the dataset version import status the
JSON response is output verbatim, since it can contain an arbitrary amount of relevant information.

To _install_ the Geostore CLI, run `pip3 install geostore`.

The general _synopsis_ is `geostore [GLOBAL_PARAMETER…] NOUN VERB [ACTION_PARAMETER…]`. `NOUN` is
the type of thing the command is operating on, for example `version` when dealing with dataset
versions. `VERB` is the action it is telling the system to take, for example `list` to show a
listing of the relevant objects, or `create` to create such an object. Verbs may have parameters in
the form of key/value pairs. `--KEY=VALUE` means the parameter is mandatory, and `[--KEY=VALUE]`
means the parameter is optional.

Run `geostore --help` to show the overall synopsis. `geostore NOUN --help` and
`geostore NOUN VERB --help` show subcommand synopsis.

### Dataset space

Synopsis: `geostore dataset VERB [PARAMETER…]`

#### Create

Synopsis: `geostore dataset create --title=TITLE --description=DESCRIPTION`

This creates a new dataset space and prints the new dataset ID on standard output when successful.

**Note:** it is important to choose an accurate and stable title. Changing the title is complex,
time-consuming, risky and lossy. If you need to change the title, choose between
[changing the dataset title by creating a copy of the latest dataset version](#Changing-the-dataset-title-by-creating-a-copy-of-the-latest-dataset-version)
and
[changing the dataset title by renaming and moving the files](#Changing-the-dataset-title-by-renaming-and-moving-the-files)
below.

CLI example:

```console
$ geostore dataset create --title=Auckland_2020 --description='Aerial imagery from April 2020'
01F9ZFRK12V0WFXJ94S0DHCP65
```

API example:

```console
$ aws lambda invoke --function-name=datasets --payload='{"http_method": "POST", "body": {"title": "Auckland_2020", "description": "Aerial imagery from April 2020"}}' /dev/stdout
{"status_code": 201, "body": {"created_at": "2021-05-26T21:17:47.758448+0000", "pk": "DATASET#01F6N8MSVEY2Y6EPFZ5XR0KFW1", "title": "Auckland_2020", "updated_at": "2021-05-26T21:17:47.758538+0000", "id": "01F6N8MSVEY2Y6EPFZ5XR0KFW1"}}
```

#### List

Synopsis: `geostore dataset list [--id=ID]`

Prints a listing of datasets, optionally filtered by the dataset ID.

Examples:

-  List all datasets using the CLI:

   ```console
   $ geostore dataset list
   Auckland_2020
   Wellington_2020
   ```

-  List all datasets using the API:
   ```console
   $ aws lambda invoke --function-name=datasets --payload='{"http_method": "GET", "body": {}}' /dev/stdout
   {"status_code": 200, "body": [{"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "title": "Auckland_2020", "updated_at": "2021-02-01T13:39:36.556583+0000"}]}
   ```
-  Filter to a single dataset using the CLI:

   ```console
   $ geostore dataset list --id=01F9ZFRK12V0WFXJ94S0DHCP65
   Auckland_2020
   ```

-  Filter to a single dataset using the API:
   ```console
   $ aws lambda invoke --function-name=datasets --payload='{"http_method": "GET", "body": {"id": "cb8a197e649211eb955843c1de66417d"}}' /dev/stdout
   {"status_code": 200, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "title": "Auckland_2020", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
   ```

#### Delete

Synopsis: `geostore dataset delete --id=ID`

`ID` is the dataset ID.

Deletes a dataset. Will only work if there are no versions in the dataset. This command does not
print anything when successful.

CLI example:

```console
$ geostore dataset delete --id=Auckland_2020
```

API example:

```console
$ aws lambda invoke --function-name=datasets --payload='{"http_method": "DELETE", "body": {"id": "cb8a197e649211eb955843c1de66417d"}}' /dev/stdout
{"status_code": 204, "body": {}}
```

### Dataset version

Synopsis: `geostore version VERB [PARAMETER…]`

#### Create

Synopsis: `geostore version create --dataset-id=ID --metadata-url=URL --s3-role-arn=ROLE_ARN`

Creates a new dataset version. It returns immediately, while the import process continues in AWS. It
prints the new dataset version ID and the ID of the import process on standard output in case of
success.

CLI example:

```console
$ geostore version create --dataset-id=01FKPEP0SQG4W2QF8KSQB6EJCD --metadata-url=s3://my-staging/Auckland_2020/catalog.json --s3-role-arn=arn:aws:iam::702361495692:role/s3-readers
2021-11-08T01-13-37-203Z_CJD6XKVJKS29ZXPA	arn:aws:states:ap-southeast-2:702361495692:execution:processingdatasetversioncreation55809360-7likTQJZBsBG:2021-11-08T01-13-37-203Z_CJD6XKVJKS29ZXPA
```

API example:

```console
$ aws lambda invoke --function-name=dataset-versions --payload='{"http_method": "POST","body": {"id": "cb8a197e649211eb955843c1de66417d","metadata_url": "s3://example-s3-url","s3_role_arn": "arn:aws:iam::1234567890:role/example-role"}}' /dev/stdout
{"status_code": 201, "body": {"dataset_version": "example_dataset_version_id", "execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}
```

#### Import process status

Synopsis: `geostore version status --execution-arn=EXECUTION_ARN`

`EXECUTION_ARN` is the import process ID printed by `geostore version create`.

This prints the current status of the dataset version import process started by
`geostore version create`.

CLI example:

```console
$ geostore version status --execution-arn=arn:aws:states:ap-southeast-2:702361495692:execution:processingdatasetversioncreation55809360-7likTQJZBsBG:2021-11-08T01-13-37-203Z_CJD6XKVJKS29ZXPA
{"step_function": {"status": "Succeeded"}, "validation": {"status": "Passed", "errors": []}, "metadata_upload": {"status": "Complete", "errors": {"failed_tasks": 0, "failure_reasons": []}}, "asset_upload": {"status": "Complete", "errors": {"failed_tasks": 0, "failure_reasons": []}}}
```

API example:

```console
$ aws lambda invoke --function-name=import-status --payload='{"http_method": "GET", "body": {"execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}' /dev/stdout
{"step_function": {"status": "Succeeded"}, "validation": {"status": "Passed", "errors": []}, "metadata_upload": {"status": "Complete", "errors": {"failed_tasks": 0, "failure_reasons": []}}, "asset_upload": {"status": "Complete", "errors": {"failed_tasks": 0, "failure_reasons": []}}}
```

### Receive Import Status updates by subscribing to our AWS SNS Topic

The ARN of our SNS Topic is
`arn:aws:sns:ap-southeast-2:<GEOSTORE_AWS_ACCOUNT_ID>:geostore-import-status` which you may choose
to subscribe to.

You may also choose to apply a subscription filter policy, which will filter notifications for a
specific dataset or specific statuses. Included in the example is all the valid statuses.

The Geostore will store a dataset in a top level directory with the same name as the dataset title
specified when importing it. You can filter SNS topics for a dataset by providing the title:

```json
{
   "dataset_title": ["Taranaki_2020"],
   "status": ["RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
}
```

Below is an example payload received when subscribed to our SNS Topic. The 'Message' field will
contain a JSON string with specific details regarding the Step Function Execution.

```json
{
   "Type": "Notification",
   "MessageId": "xxxx-xxxx-xxxx",
   "TopicArn": "arn:aws:sns:ap-southeast-2:<GEOSTORE_AWS_ACCOUNT_ID>:geostore-import-status",
   "Message": "{\"version\": \"0\", […]}",
   "Timestamp": "2021-07-07T01:49:33.471Z",
   "SignatureVersion": "1",
   "Signature": "xxxxx",
   "SigningCertURL": "https://example.com",
   "UnsubscribeURL": "https://example.com",
   "MessageAttributes": {
      "dataset_title": {
         "Type": "String",
         "Value": "01F9ZFRK12V0WFXJ94S0DHCP65"
      },
      "status": {
         "Type": "String",
         "Value": "SUCCEEDED"
      }
   }
}
```

Below is an example of the AWS EventBridge 'Step Functions Execution Status Change' payload. This
will be formatted as a string and associated with the 'Message' attribute in the above SNS payload.
See
[AWS EventBridge Payload Examples](https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html#cw-events-events)

```json
{
   "version": "0",
   "id": "3afc69d1-1291-6372-ae76-9a9c11ee35c4",
   "detail-type": "Step Functions Execution Status Change",
   "source": "aws.states",
   "account": "<GEOSTORE_AWS_ACCOUNT_ID>",
   "time": "2021-07-07T01:49:29Z",
   "region": "ap-southeast-2",
   "resources": ["arn:aws:states:example"],
   "detail": {
      "executionArn": "arn:aws:states:example",
      "stateMachineArn": "arn:aws:states:example",
      "name": "Aerial_Imagery_xxxx",
      "status": "SUCCEEDED",
      "startDate": 1625622391067,
      "stopDate": 1625622569782,
      "input": "{\"dataset_id\": \"01F9ZA9ZZZDM815S20EHXEAT40\", \"dataset_title\": \"test_1625622377\", \"version_id\": \"2021-07-07T01-46-30-787Z_9NJEAD3VXRCH5W05\", \"metadata_url\": \"s3://example/catalog.json\", \"s3_role_arn\": \"arn:aws:iam::715898075157:role/example\"}",
      "inputDetails": {
         "included": true
      },
      "output": "{\"dataset_id\":\"01F9ZA9ZZZDM815S20EHXEAT40\",\"dataset_title\":\"test_1625622377\",\"version_id\":\"2021-07-07T01-46-30-787Z_9NJEAD3VXRCH5W05\",\"metadata_url\":\"s3://example/catalog.json\",\"s3_role_arn\":\"arn:aws:iam::715898075157:role/example\",\"content\":{\"first_item\":\"0\",\"iteration_size\":1,\"next_item\":-1,\"assets_table_name\":\"example\",\"results_table_name\":\"example\"},\"validation\":{\"success\":true},\"import_dataset\":{\"asset_job_id\":\"e4ad8b0d-4358-4c42-bb0d-3577c96f7039\",\"metadata_job_id\":\"84a7b4fc-7d00-403c-a5fb-91257f406afb\"},\"upload_status\":{\"validation\":{\"status\":\"Passed\",\"errors\":[]},\"asset_upload\":{\"status\":\"Complete\",\"errors\":[]},\"metadata_upload\":{\"status\":\"Complete\",\"errors\":[]}},\"update_root_catalog\":{\"new_version_s3_location\":\"s3://linz-geostore/example/catalog.json\"}}",
      "outputDetails": {
         "included": true
      }
   }
}
```

Note: the output field will only be populated above when the Step Function has succeeded.

#### Changing the dataset title by creating a copy of the latest dataset version

This is the simplest way to change a dataset title, but there will be no explicit connection between
the new and old datasets. Anyone wishing to go back beyond the rename of a dataset needs to be aware
of this rename and has to either know or find the original dataset title.

1. [Create a new dataset](#Dataset-creation-request)
1. [Create a new dataset version](#Dataset-Version-creation-request) for the dataset created above,
   using a `metadata_url` pointing to the latest version of the original dataset.
1. [Wait for the import to finish](#Import-Status-Endpoint-Usage-Examples).
1. Optional: if the original dataset can be removed at this point (or sometime in the future),
   please let the Geostore product team know, and we'll arrange it.

#### Changing the dataset title by renaming and moving the files

This process is more cumbersome and time-consuming than the above, and results links to the old
dataset being broken rather than slowly phased out, but can be followed if necessary.

1. Send a request to the product team asking for a rename, specifying the current and new title of
   the dataset. The product team then takes care of the rest of the process:
   1. Schedule necessary downtime and notify users.
   1. Turn off external access to the whole or part of the system to avoid any conflicts.
   1. Run a manual rename of all the files in the relevant dataset.
   1. Use the `${ENV}-datasets` endpoint to rename the dataset in the database.
   1. Re-enable external access and notify users.
   1. Notify requester about the name change completion.

### On STAC IDs

-  The root catalog has the ID `root_catalog`.
-  The dataset catalogs have IDs consisting of the [dataset title](#dataset-creation-request), a
   hyphen, and a Universally Unique Lexicographically Sortable Identifier (ULID), for example
   `Wellington_2020`.
-  IDs within the dataset versions are unchanged.
