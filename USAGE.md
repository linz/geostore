# Getting Access to Data Lake
## Prerequisites
Currently, the Data Lake allows read/write access for AWS accounts specified in `DATALAKE_USERS_AWS_ACCOUNTS_IDS` (see [README](README.md#aws-infrastructure-deployment-cdk-stack)). To add your AWS account to this list, contact the Data Lake product team. 

The product team will provide you the following information to enable to you to start using the Data Lake:

* Data Lake AWS account ID (`DATALAKE_AWS_ACCOUNT_ID`)
* Data Lake user role name (`DATALAKE_USER_ROLE_NAME`)
* Data Lake lambda function endpoint names (`DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME`)

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
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::<YOUR_STAGING_BUCKET>/<YOUR_DATASET>/*"
        }
    ]
}
```

## Credentials
Temporary access credentials for Data Lake can be requested by running the following commands while
authenticated using user's home AWS account credentials:
```
export DATALAKE_AWS_ACCOUNT_ID=<DATALAKE-AWS-ACCOUNT-ID>
export DATALAKE_USER_ROLE_NAME=<DATALAKE-USER-ROLE-NAME>

credentials="$(aws sts assume-role \
    --role-arn "arn:aws:iam::${DATALAKE_AWS_ACCOUNT_ID}:role/${DATALAKE_USER_ROLE_NAME}" \
    --role-session-name datalake-user)"

export AWS_ACCESS_KEY_ID=$(echo $credentials | jq -r ".Credentials[\"AccessKeyId\"]")
export AWS_SECRET_ACCESS_KEY=$(echo $credentials | jq -r ".Credentials[\"SecretAccessKey\"]")
export AWS_SESSION_TOKEN=$(echo $credentials | jq -r ".Credentials[\"SessionToken\"]")
```

# Data Lake Lambda Endpoints Usage
## Endpoint Request Format
```
export DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME=<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>

aws lambda invoke \
    --function-name  $DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME \
    --invocation-type RequestResponse \
    --payload '<REQUEST-PAYLOAD-JSON>'
/dev/stdout
```

## Dataset Space Endpoint Usage Examples
* Set Dataset Space Endpoint Lambda function name
    ```
    export DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME=<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>
    ```
* Example of Dataset creation request
    ```
    $ aws lambda invoke \
        --function-name $DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME \
        --invocation-type RequestResponse \
        --payload '{"httpMethod": "POST", "body": {"type": "RASTER", "title": "Auckland 2020", "owning_group": "A_XYZ_XYZ"}}' \
    /dev/stdout

    {"statusCode": 201, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "owning_group": "A_XYZ_XYZ", "title": "Auckland 2020", "type": "RASTER", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
    ```
* Example of all Datasets listing request
    ```
    $ aws lambda invoke \
        --function-name $DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME \
        --invocation-type RequestResponse \
        --payload '{"httpMethod": "GET", "body": {}}' \
    /dev/stdout

    {"statusCode": 200, "body": [{"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "owning_group": "A_XYZ_XYZ", "title": "Auckland 2020", "type": "RASTER", "updated_at": "2021-02-01T13:39:36.556583+0000"}]}
    ```
* Example of single Dataset listing request
    ```
    $ aws lambda invoke \
        --function-name $DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME \
        --invocation-type RequestResponse \
        --payload '{"httpMethod": "GET", "body": {"id": "cb8a197e649211eb955843c1de66417d", "type": "RASTER"}}' \
    /dev/stdout

    {"statusCode": 200, "body": {"created_at": "2021-02-01T13:38:40.776333+0000", "id": "cb8a197e649211eb955843c1de66417d", "owning_group": "A_XYZ_XYZ", "title": "Auckland 2020", "type": "RASTER", "updated_at": "2021-02-01T13:39:36.556583+0000"}}
    ```
* Example of Dataset delete request
    ```
    $ aws lambda invoke \
        --function-name datasets-endpoint \
        --invocation-type RequestResponse \
        --payload '{"httpMethod": "DELETE", "body": {"id": "cb8a197e649211eb955843c1de66417d", "type": "RASTER"}}' \
    /dev/stdout

    {"statusCode": 204, "body": {}}
    ```

## Dataset Version Endpoint Usage Examples
* Set Dataset Space Endpoint Lambda function name
    ```
    export DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME=<DATALAKE-LAMBDA-FUNCTION-ENDPOINT-NAME>
    ```
* Example of Dataset Version creation request
    ```
    $ aws lambda invoke \
        --function-name $DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME \
        --invocation-type RequestResponse \
        --payload '{"httpMethod": "POST", "body": {"id": "example_dataset_id", "type": "RASTER", "metadata-url": "s3://example-s3-url"}}' \
    /dev/stdout

    {"statusCode": 201, "body": {"dataset_version": "example_dataset_version_id", "execution_arn": "arn:aws:batch:ap-southeast-2:xxxx:job/example-arn"}}
    ```
  