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
asset_root='/asset-output'

if [ -f "${script_dir}/${1}/requirements.txt" ]; then
    python -m venv "${work_dir}/.venv"
    # shellcheck source=/dev/null
    . "${work_dir}/.venv/bin/activate"
    python -m pip install --upgrade pip

    pip install --requirement="${script_dir}/${1}/requirements.txt" --target=${asset_root}
fi

mkdir --parents "${asset_root}/processing/${1}"
cp --archive --update "${script_dir}/"*.py "${asset_root}/processing"
cp --archive --update "${script_dir}/${1}/"*.py "${asset_root}/processing/${1}"
