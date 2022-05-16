#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail

S3_ROLE_NAME="$1"

RAW_POLICIES=$( (aws iam list-role-policies --role-name="$S3_ROLE_NAME") | jq -c -r '.PolicyNames[]')
for POLICY in "${RAW_POLICIES[@]}"; do
    aws iam delete-role-policy --role-name="$S3_ROLE_NAME" --policy-name="$POLICY"
done
aws iam delete-role --role-name="$S3_ROLE_NAME"
