# Using the Geospatial Data Lake

The purpose of GDL is to store geospatial datasets. This document should provide the technical know-how to create and maintain such datasets.

The keywords "must", "must not", "required", "shall", "shall not", "should", "should not", "recommended",  "may", and "optional" in this document are to be interpreted as described in [RFC 2119](https://tools.ietf.org/html/rfc2119).

## Prerequisites
Currently, Data Lake allows read/write access for all users authenticated via `DATALAKE_SAML_IDENTITY_PROVIDER_ARN` during deployment time (see [README](README.md#aws-infrastructure-deployment-cdk-stack)).

The product team will provide you the following information to enable to you to start using the Data Lake:

Also, following information must be provided by Data Lake instance maintainer in order to star using it:

- Data Lake AWS account ID (`DATALAKE_AWS_ACCOUNT_ID`)
- Data Lake user role name (`DATALAKE_USER_ROLE_NAME`)
- Data Lake lambda function endpoint names (`DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME`)

### Data Maintainers
To import data in to the Data Lake, you will need a 'staging' S3 bucket with your data in it. You will need to give permissions to Data Lake AWS account to read your data.

Example bucket policy:

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
            "Resource": "arn:aws:s3:::<YOUR_STAGING_BUCKET>/<YOUR_DATASET>/*"
        }
    ]
}
```

## Credentials

Temporary access credentials for Data Lake can be requested by running the following commands while authenticated using user's home AWS account credentials:

```bash
export DATALAKE_AWS_ACCOUNT_ID=<DATALAKE-AWS-ACCOUNT-ID>
export DATALAKE_USER_ROLE_NAME=<DATALAKE-USER-ROLE-NAME>

credentials="$(aws sts assume-role \
    --role-arn "arn:aws:iam::${DATALAKE_AWS_ACCOUNT_ID}:role/${DATALAKE_USER_ROLE_NAME}" \
    --role-session-name datalake-user)"

export AWS_ACCESS_KEY_ID=$(jq -r ".Credentials[\"AccessKeyId\"]" <<< "$credentials")
export AWS_SECRET_ACCESS_KEY=$(jq -r ".Credentials[\"SecretAccessKey\"]" <<< "$credentials")
export AWS_SESSION_TOKEN=$(jq -r ".Credentials[\"SessionToken\"]" <<< "$credentials")
```

## Dataset format

A dataset consists of a set of files in Amazon Web Services Simple Storage Service (AWS S3). The dataset consists of geospatial metadata files in [SpatioTemporal Asset Catalogs (STAC)](https://stacspec.org/) format and data files, which are called "assets" in STAC.

The GDL performs many checks on datasets. If any of the checks fail the dataset will not be imported, so it's important to know what they are. The following list is a reference of all the checks which are currently in place.

- Every metadata file must follow the [STAC Collection Specification](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md).
- Every metadata URL (in the [`links` property](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#link-object)) must be an S3 URL of the form `s3://BUCKET_NAME/KEY`, for example, `s3://my-bucket/some-path/foo.tif`.
- Every asset (in the [`item_assets` property](https://github.com/radiantearth/stac-spec/blob/master/extensions/item-assets/README.md)) must have:
   - an S3 [URL](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#asset-object), as defined above
   - a [multihash](https://github.com/radiantearth/stac-spec/blob/master/extensions/checksum/README.md) corresponding to the contents of the asset file
- Every metadata and asset must be in the same S3 "folder." That is, every metadata and asset URL must have the same content up to the last slash. For example, a having a root metadata URL `s3://bucket/folder/collection.json` and an asset URL `s3://bucket/folder/subfolder/1.tif` is invalid.
- Every metadata and asset URL must be readable by the GDL.
- A dataset *may* refer to the same asset more than once. All references to the same asset must have the same multihash. That is, having a SHA-1 and a SHA-256 checksum for the same file will be considered invalid, even if both checksums are valid. This is to enable a simpler checksum validation.

## Data Lake Endpoints Usage

There are two end user interaction points in GDL:

- A [dataset space endpoint](TODO), to create, get, update or delete individual datasets, and to list all datasets
- A [dataset versions endpoint](TODO), to create new versions of datasets. The S3 files which constitute the dataset are all linked to a specific dataset version.

Both of these are implemented as AWS Lambda functions, which means they can be run ("invoked") either via the AWS web interface (links above) or via any tool using the AWS API, such as the commands below.

### Endpoint Request Format

```bash
export DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME=<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>

aws lambda invoke \
    --function-name "$DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME" \
    --invocation-type RequestResponse \
    --payload '<REQUEST-PAYLOAD-JSON>'
/dev/stdout
```

### Dataset Space Endpoint Usage Examples

- Set Dataset Space Endpont Lambda function name

   ```bash
   export DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME=<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>
   ```
- Example of Dataset creation request

   ```console
   $ aws lambda invoke \
       --function-name "$DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME" \
       --invocation-type RequestResponse \
       --payload '{"httpMethod": "POST", "body": {"type": "RASTER", "title": "Auckland 2020", "owning_group": "A_XYZ_XYZ"}}' \
       /dev/stdout

   {"statusCode": 201, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "owning_group": "A_XYZ_XYZ", "title": "Auckland 2020", "type": "RASTER", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
   ```
- Example of all Datasets listing request

   ```console
   $ aws lambda invoke \
       --function-name "$DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME" \
       --invocation-type RequestResponse \
       --payload '{"httpMethod": "GET", "body": {}}' \
       /dev/stdout

   {"statusCode": 200, "body": [{"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "owning_group": "A_XYZ_XYZ", "title": "Auckland 2020", "type": "RASTER", "updated_at": "2021-02-01T13:39:36.556583+0000"}]}
   ```
- Example of single Dataset listing request

   ```console
   $ aws lambda invoke \
       --function-name "$DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME" \
       --invocation-type RequestResponse \
       --payload '{"httpMethod": "GET", "body": {"id": "cb8a197e649211eb955843c1de66417d", "type": "RASTER"}}' \
       /dev/stdout

   {"statusCode": 200, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "owning_group": "A_XYZ_XYZ", "title": "Auckland 2020", "type": "RASTER", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
   ```
- Example of Dataset delete request

   ```console
   $ aws lambda invoke \
       --function-name datasets-endpoint \
       --invocation-type RequestResponse \
       --payload '{"httpMethod": "DELETE", "body": {"id": "cb8a197e649211eb955843c1de66417d", "type": "RASTER"}}' \
       /dev/stdout

   {"statusCode": 204, "body": {}}
   ```

## Dataset Version Endpoint Usage Examples

- Set Dataset Space Endpoint Lambda function name

   ```bash
   export DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME=<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>
   ```
- Example of Dataset Version creation request

   ```console
   $ aws lambda invoke \
      --function-name $DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME \
      --invocation-type RequestResponse \
      --payload '{"httpMethod": "POST", "body": {"id": "cb8a197e649211eb955843c1de66417d", "type": "RASTER", "metadata-url": "s3://example-s3-url"}}' \
      /dev/stdout

   {"statusCode": 201, "body": {"dataset_version": "example_dataset_version_id", "execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}
   ```

## Import Status Endpoint Usage Examples

- Set Import Status Endpoint Lambda function name

   ```bash
   export DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME=<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>
   ```
- Example of get Import Status request

   ```console
   $ aws lambda invoke \
      --function-name $DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME \
      --invocation-type RequestResponse \
      --payload '{"httpMethod": "GET", "body": {"execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}' \
      /dev/stdout

   {"statusCode": 200, "body": {"validation":{ "status": "SUCCEEDED"}, "upload":{"status": "Pending", "errors":[]}}}
   ```
