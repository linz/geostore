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

    echo "Removing ${count} objects"
    jq --raw-output '.[] | [.Key, .VersionId] | @tsv' <<< "$1" | parallel --colsep='\t' --group aws s3api delete-object --bucket="$bucket" --key='{1}' --version-id='{2}'
}

for bucket
do
    versions="$(aws s3api list-object-versions --bucket="$bucket" | jq .Versions)"
    delete_objects "$versions"

    markers="$(aws s3api list-object-versions --bucket="$bucket" | jq .DeleteMarkers)"
    delete_objects "$markers"
done
