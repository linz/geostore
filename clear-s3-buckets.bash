#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail

if [[ "$#" -eq 0 ]]
then
    cat >&2 << 'EOF'
./clear-s3-buckets.bash BUCKET [BUCKET…]

Deletes *all* versions of *all* files in *all* given buckets. Only to be used in case of emergency!
EOF
    exit 1
fi

read -n1 -p "THIS WILL DELETE EVERYTHING IN BUCKETS ${*}! Press Ctrl-c to cancel or anything else to continue: " -r

delete_objects() {
    count="$(jq length <<< "$1")"

    if [[ "$count" -eq 0 ]]
    then
        echo "No objects found; skipping" >&2
        return
    fi

    echo "Removing objects"
    for index in $(seq 0 $(("$count" - 1)))
    do
        keys+=("$(jq --raw-output ".[${index}].Key" <<< "$1")")
        version_ids+=("$(jq --raw-output ".[${index}].VersionId" <<< "$1")")
    done
    parallel --group aws s3api delete-object --bucket="$bucket" --key="{1}" --version-id="{2}" ::: "${keys[@]}" :::+ "${version_ids[@]}"
}

for bucket
do
    versions="$(aws s3api list-object-versions --bucket="$bucket" | jq .Versions)"
    delete_objects "$versions"

    markers="$(aws s3api list-object-versions --bucket="$bucket" | jq .DeleteMarkers)"
    delete_objects "$markers"
done
