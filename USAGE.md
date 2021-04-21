# Using the Geospatial Data Lake

The purpose of GDL is to store geospatial datasets. This document should provide the technical
know-how to create and maintain such datasets.

The keywords "must", "must not", "required", "shall", "shall not", "should", "should not",
"recommended", "may", and "optional" in this document are to be interpreted as described in
[RFC 2119](https://tools.ietf.org/html/rfc2119).

# Prerequisites

## Data Lake account and resource names

Following information must be provided by Data Lake instance maintainer in order to start using it:

- Data Lake AWS account ID (`DATALAKE_AWS_ACCOUNT_ID`)
- Data Lake user role name (`DATALAKE_USER_ROLE_NAME`)
- Environment name (`ENV`, typically "prod")

## Dataset source S3 bucket

To import data in to the Data Lake, dataset source S3 bucket must be readable by Data Lake.

Example dataset source S3 bucket policy:

```
{
    "Version": "2012-10-17",
    "Id": "data-lake-policy",
    "Statement": [
        {
            "Sid": "data-lake-readonly",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<DATALAKE_AWS_ACCOUNT_ID>:root"
            },
            "Action": [
                "s3:GetObject",
                "s3:GetObjectAcl",
                "s3:GetObjectTagging"
            ],
            "Resource": "arn:aws:s3:::<YOUR_BUCKET>/<YOUR_DATASET>/*"
        }
    ]
}
```

## Dataset format

A dataset consists of a set of files in Amazon Web Services Simple Storage Service (AWS S3). The
dataset consists of geospatial metadata files in
[SpatioTemporal Asset Catalogs (STAC)](https://stacspec.org/) format and data files, which are
called "assets" in STAC.

The GDL performs many checks on datasets. If any of the checks fail the dataset will not be
imported, so it's important to know what they are. The following list is a reference of all the
checks which are currently in place.

- Every metadata file must follow the
  [STAC Collection Specification](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md).
- Every metadata URL (in the
  [`links` property](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#link-object))
  must be an S3 URL of the form `s3://BUCKET_NAME/KEY`, for example,
  `s3://my-bucket/some-path/foo.tif`.
- Every asset (in the
  [`item_assets` property](https://github.com/radiantearth/stac-spec/blob/master/extensions/item-assets/README.md))
  must have:
  - an S3
    [URL](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-object),
    as defined above
  - a
    [multihash](https://github.com/radiantearth/stac-spec/blob/master/extensions/checksum/README.md)
    corresponding to the contents of the asset file
- Every metadata and asset file must be in the same S3 bucket.
- Every metadata and asset URL must be readable by the GDL.
- A dataset _may_ refer to the same asset more than once. All references to the same asset must have
  the same multihash. That is, having a SHA-1 and a SHA-256 checksum for the same file will be
  considered invalid, even if both checksums are valid. This is to enable a simpler checksum
  validation.

# Authentication and authorization

Data Lake allows read/write access for users authorized by SAML identity provider
(`DATALAKE_SAML_IDENTITY_PROVIDER_ARN`) configured during deployment time (see
[README](README.md#aws-infrastructure-deployment-cdk-stack)).

Example of AWS service account authentication and authorization in to Data Lake users role via
Azure:

- Log in to Data Lake AWS account using Data Lake users role

  ```bash
  aws-azure-login --profile data-lake-users
  ```

- Set Data Lake AWS profile for subsequent commands
  ```bash
  export AWS_PROFILE data-lake-users
  ```

# Endpoints

There are several end user interaction points in GDL:

- A [dataset space endpoint](TODO), to create, get, update or delete individual datasets, and to
  list all datasets
- A [dataset versions endpoint](TODO), to create new versions of datasets. The S3 files which
  constitute the dataset are all linked to a specific dataset version.
- An [import status endpoint](TODO), to get information about the status of dataset version import,
  including errors and issues

These are implemented as AWS Lambda functions, which means they can be run ("invoked") either via
the AWS web interface (links above) or via any tool using the AWS API, such as the commands below.

## Endpoint Request Format

```bash
aws lambda invoke \
    --function-name '<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>' \
    --payload '<REQUEST-PAYLOAD-JSON>'
/dev/stdout
```

## Dataset Space Endpoint Usage Examples

- Example of Dataset creation request

  ```console
  $ aws lambda invoke \
      --function-name "${ENV}-datasets" \
      --payload '{"httpMethod": "POST", "body": {"title": "Auckland 2020"}}' \
      /dev/stdout

  {"statusCode": 201, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "title": "Auckland 2020", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
  ```

- Example of all Datasets listing request

  ```console
  $ aws lambda invoke \
      --function-name "${ENV}-datasets" \
      --payload '{"httpMethod": "GET", "body": {}}' \
      /dev/stdout

  {"statusCode": 200, "body": [{"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "title": "Auckland 2020", "updated_at": "2021-02-01T13:39:36.556583+0000"}]}
  ```

- Example of single Dataset listing request

  ```console
  $ aws lambda invoke \
      --function-name "${ENV}-datasets" \
      --payload '{"httpMethod": "GET", "body": {"id": "cb8a197e649211eb955843c1de66417d"}}' \
      /dev/stdout

  {"statusCode": 200, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "title": "Auckland 2020", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
  ```

- Example of Dataset delete request

  ```console
  $ aws lambda invoke \
      --function-name "${ENV}-datasets" \
      --payload '{"httpMethod": "DELETE", "body": {"id": "cb8a197e649211eb955843c1de66417d"}}' \
      /dev/stdout

  {"statusCode": 204, "body": {}}
  ```

## Dataset Version Endpoint Usage Examples

- Example of Dataset Version creation request

  ```console
  $ aws lambda invoke \
     --function-name "${ENV}-dataset-versions" \
     --payload '{"httpMethod": "POST", "body": {"id": "cb8a197e649211eb955843c1de66417d", "metadata-url": "s3://example-s3-url"}}' \
     /dev/stdout

  {"statusCode": 201, "body": {"dataset_version": "example_dataset_version_id", "execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}
  ```

## Import Status Endpoint Usage Examples

- Example of get Import Status request

  ```console
  $ aws lambda invoke \
     --function-name "${ENV}-import-status" \
     --payload '{"httpMethod": "GET", "body": {"execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}' \
     /dev/stdout

  {"statusCode": 200, "body": {"validation":{ "status": "SUCCEEDED"}, "metadata upload":{"status": "Pending", "errors":[]}, "asset upload":{"status": "Pending", "errors":[]}}}
  ```
