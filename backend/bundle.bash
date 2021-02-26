#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset

if [[ "$#" -lt 1 ]]
then
    cat >&2 <<'EOF'
Synopsis: ./bundle.bash DIRECTORY
Example: ./bundle.bash processing/datasets
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

asset_root='/asset-output'
task_directory="$(basename "$1")"
requirements_file="${work_dir}/requirements.txt"
# `--without-hashes` works around https://github.com/python-poetry/poetry/issues/1584
poetry export --extras="$task_directory" --without-hashes > "$requirements_file"
pip install --requirement="$requirements_file" --target="$asset_root"

mkdir --parents "${asset_root}/${1}"
task_parent_directory=$(dirname "$1")
cp --archive --update --verbose "${script_dir}/${task_parent_directory}/"*.py "${asset_root}/${task_parent_directory}"
cp --archive --update --verbose "${script_dir}/${1}/"*.py "${asset_root}/${1}"
