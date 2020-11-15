#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset

if [[ "$#" -lt 1 ]]
then
    cat >&2 <<'EOF'
Synopsis: ./bundle.bash DIRECTORY
Example: ./bundle.bash datasets
EOF
    exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

work_dir="$(mktemp --directory)"

python -m venv "${work_dir}/.venv"
# shellcheck source=/dev/null
. "${work_dir}/.venv/bin/activate"
python -m pip install --upgrade pip
python -m pip install poetry

all_requirements_file="${work_dir}/all-requirements.txt"
endpoint_requirements_file="${work_dir}/endpoint-requirements.txt"

# Get endpoint-specific requirements file
poetry export --output="$all_requirements_file" --without-hashes
grep --file="${script_dir}/${1}/requirements.txt" "$all_requirements_file" > "$endpoint_requirements_file"

pip install --requirement="$endpoint_requirements_file" --target=/asset-output
mkdir --parents /asset-output/endpoints/datasets
cp --archive --update --verbose "${script_dir}/"*.py /asset-output/endpoints
cp --archive --update --verbose "${script_dir}/${1}/"*.py /asset-output/endpoints/datasets
