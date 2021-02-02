# Getting Access to Data Lake
## Prerequisites
Currently, Data Lake allows read/write access for all users from all AWS accounts (users home
accounts) specified in `DATALAKE_USERS_AWS_ACCOUNTS_IDS` during deployment time
(see [README](README.md#aws-infrastructure-deployment-cdk-stack)).

Also, following information must be provided by Data Lake instance maintainer in order to star using
it:

* Data Lake AWS account ID (`DATALAKE_AWS_ACCOUNT_ID`)
* Data Lake user role name (`DATALAKE_USER_ROLE_NAME`)
* Data Lake lambda function endpoint names (`DATALAKE_LAMBDA_FUNCTION_ENDPOINT_NAME`)

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
* Set Dataset Space Endpont Lambda function name
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
