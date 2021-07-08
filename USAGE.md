# Using Geostore

The purpose of Geostore is to store geospatial datasets. This document should provide the technical
know-how to create and maintain such datasets.

The keywords "must", "must not", "required", "shall", "shall not", "should", "should not",
"recommended", "may", and "optional" in this document are to be interpreted as described in
[RFC 2119](https://tools.ietf.org/html/rfc2119).

# Prerequisites

## Geostore account and resource names

You'll need the following information to start using Geostore:

- Geostore AWS account ID (`GEOSTORE_AWS_ACCOUNT_ID`)
- Geostore user role name (`GEOSTORE_USER_ROLE_NAME`)

Replace the `GEOSTORE_…` strings in the following documentation with the actual values given by the
Geostore instance maintainer.

## Dataset source S3 bucket

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

## Dataset format

A dataset consists of a set of files in Amazon Web Services Simple Storage Service (AWS S3). The
dataset consists of geospatial metadata files in
[SpatioTemporal Asset Catalogs (STAC)](https://stacspec.org/) format and data files, which are
called "assets" in STAC.

Geostore performs many checks on datasets. If any of the checks fail the dataset will not be
imported, so it's important to know what they are. The following list is a reference of all the
checks which are currently in place.

- Every metadata file must follow the
  [STAC Collection Specification](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md).
- Every metadata URL (in the
  [`links` property](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#link-object))
  must be an S3 URL of the form `s3://BUCKET_NAME/KEY`, for example,
  `s3://my-bucket/some-path/foo.tif`.
- Every asset (in the
  [`assets` property](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#assets))
  must have:
  - an S3
    [URL](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-object),
    as defined above
  - a [checksum](https://github.com/stac-extensions/file#checksums) corresponding to the contents of
    the asset file
- Every metadata and asset file must be in the same S3 bucket.
- Every metadata and asset URL must be readable by Geostore.
- A dataset _may_ refer to the same asset more than once. All references to the same asset must have
  the same multihash. That is, having a SHA-1 and a SHA-256 checksum for the same file will be
  considered invalid, even if both checksums are valid. This is to enable a simpler checksum
  validation.

# Authentication and authorization

Geostore allows read/write access for users authorized by SAML identity provider
(`GEOSTORE_SAML_IDENTITY_PROVIDER_ARN`) configured during deployment time (see
[README](README.md#aws-infrastructure-deployment-cdk-stack)).

Example of AWS service account authentication and authorization in to Geostore users role via Azure:

- Log in to Geostore AWS account using Geostore users role

  ```bash
  aws-azure-login --profile geostore-users
  ```

- Set Geostore AWS profile for subsequent commands
  ```bash
  export AWS_PROFILE geostore-users
  ```

# Endpoints

There are several end user interaction points in Geostore:

- A
  [dataset space endpoint](https://ap-southeast-2.console.aws.amazon.com/lambda/home?region=ap-southeast-2#functions/datasets),
  to create, get, update or delete individual datasets, and to list all datasets
- A
  [dataset versions endpoint](https://ap-southeast-2.console.aws.amazon.com/lambda/home?region=ap-southeast-2#functions/dataset-versions),
  to create new versions of datasets. The S3 files which constitute the dataset are all linked to a
  specific dataset version.
- An
  [import status endpoint](https://ap-southeast-2.console.aws.amazon.com/lambda/home?region=ap-southeast-2#functions/import-status),
  to get information about the status of dataset version import, including errors and issues

These are implemented as AWS Lambda functions, which means they can be run ("invoked") either via
the AWS web interface (links above) or via any tool using the AWS API, such as the commands below.

## Endpoint Request Format

```bash
aws lambda invoke --function-name GEOSTORE-LAMBDA-FUNCTION-ENDPOINT-NAME \
    --payload 'REQUEST-PAYLOAD-JSON' /dev/stdout
```

## Dataset Space Endpoint Usage Examples

### Dataset creation request

```console
$ aws lambda invoke --function-name datasets \
    --payload '{"http_method": "POST", "body": {"title": "Auckland_2020", "description": "Aerial Imagery from APR 2020"}}' /dev/stdout

{"status_code": 201, "body": {"created_at": "2021-05-26T21:17:47.758448+0000", "pk": "DATASET#01F6N8MSVEY2Y6EPFZ5XR0KFW1", "title": "Auckland_2020", "updated_at": "2021-05-26T21:17:47.758538+0000", "id": "01F6N8MSVEY2Y6EPFZ5XR0KFW1"}}
```

Please note that it is important to choose an accurate and stable title. Changing the title is
complex, time-consuming, risky and lossy. If you need to change the title, choose between
[changing the dataset title by creating a copy of the latest dataset version](#Changing-the-dataset-title-by-creating-a-copy-of-the-latest-dataset-version)
and
[changing the dataset title by renaming and moving the files](#Changing-the-dataset-title-by-renaming-and-moving-the-files)
below.

### All datasets listing request

```console
$ aws lambda invoke --function-name datasets \
    --payload '{"http_method": "GET", "body": {}}' /dev/stdout

{"status_code": 200, "body": [{"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "title": "Auckland_2020", "updated_at": "2021-02-01T13:39:36.556583+0000"}]}
```

### Single dataset listing request

```console
$ aws lambda invoke --function-name datasets \
    --payload '{"http_method": "GET", "body": {"id": "cb8a197e649211eb955843c1de66417d"}}' \
    /dev/stdout

{"status_code": 200, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "title": "Auckland_2020", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
```

### Dataset delete request

```console
$ aws lambda invoke --function-name datasets \
    --payload '{"http_method": "DELETE", "body": {"id": "cb8a197e649211eb955843c1de66417d"}}' \
    /dev/stdout

{"status_code": 204, "body": {}}
```

### Changing the dataset title by creating a copy of the latest dataset version

This is the simplest way to change a dataset title, but there will be no explicit connection between
the new and old datasets. Anyone wishing to go back beyond the rename of a dataset needs to be aware
of this rename and has to either know or find the original dataset title.

1. [Create a new dataset](#Dataset-creation-request)
1. [Create a new dataset version](#Dataset-Version-creation-request) for the dataset created above,
   using a `metadata_url` pointing to the latest version of the original dataset.
1. [Wait for the import to finish](#Import-Status-Endpoint-Usage-Examples).
1. Optional: if the original dataset can be removed at this point (or sometime in the future),
   please let the Geostore product team know, and we'll arrange it.

### Changing the dataset title by renaming and moving the files

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

## Dataset Version Endpoint Usage Examples

### Dataset Version creation request

```console
$ aws lambda invoke --function-name dataset-versions \
   --payload '{
     "http_method": "POST",
     "body": {
       "id": "cb8a197e649211eb955843c1de66417d",
       "metadata_url": "s3://example-s3-url",
       "s3_role_arn": "arn:aws:iam::1234567890:role/example-role"
     }
   }' \
   /dev/stdout

{"status_code": 201, "body": {"dataset_version": "example_dataset_version_id", "execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}
```

## Import Status Endpoint Usage Examples

### Get Import Status request

```console
$ aws lambda invoke --function-name import-status \
   --payload '{"http_method": "GET", "body": {"execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}' \
   /dev/stdout

{"status_code": 200, "body": {"validation":{ "status": "SUCCEEDED"}, "metadata_upload":{"status": "Pending", "errors":[]}, "asset_upload":{"status": "Pending", "errors":[]}}}
```
