#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail
shopt -s failglob inherit_errexit

if [[ $# -eq 0 ]]; then
    cat >&2 <<'EOF'
Synopsis: ./generate-requirements-files.bash PATH [PATH…]

Example: ./generate-requirements-files.bash geostore/poetry.txt

Creates pip formatted requirements files (including dependencies and hashes) at each PATH with the package derived from the filename.

This is used to work around Dependabot not knowing which package is the "main" one in a requirements file.
EOF
    exit 1
fi

for path; do
    package_name="$(basename "${path%.txt}")"
    pip-compile --allow-unsafe --generate-hashes --no-annotate --no-header --output-file="$path" --upgrade <(echo "$package_name")
done
