#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail

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
python -m pip install --quiet --cache-dir="$work_dir" --upgrade pip
python -m pip install --quiet --cache-dir="$work_dir" poetry wheel

asset_root='/asset-output'
task_directory="$(basename "$1")"
requirements_file="${work_dir}/requirements.txt"
# `--without-hashes` works around https://github.com/python-poetry/poetry/issues/1584
poetry export --extras="$task_directory" --without-hashes | grep --invert-match '^botocore==' > "$requirements_file"
pip install --quiet --cache-dir="$work_dir" --no-deps --requirement="$requirements_file" --target="$asset_root"

mkdir --parents "${asset_root}/geostore/${1}"
cp --archive --update "${script_dir}/"*.py "${asset_root}/geostore/"
cp --archive --update "${script_dir}/${1}" "${asset_root}/geostore/"
