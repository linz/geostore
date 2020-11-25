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

asset_root='/asset-output'
# `--without-hashes` works around https://github.com/python-poetry/poetry/issues/1584
pip install --requirement=<(poetry export --extras="${1}-endpoint" --without-hashes) "--target=${asset_root}"
mkdir --parents "${asset_root}/endpoints/datasets"
cp --archive --update --verbose "${script_dir}/"*.py "${asset_root}/endpoints"
cp --archive --update --verbose "${script_dir}/${1}/"*.py "${asset_root}/endpoints/datasets"
