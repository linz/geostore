#!/usr/bin/env bash

set -o errexit -o noclobber -o nounset -o pipefail

usage() {
    cat >&2 << 'EOF'
Usage:

. activate-dev-env.bash
EOF
}

if ! (return 0)
then
    usage
    exit 2
fi

script_dir="$(dirname "${BASH_SOURCE[0]}")"

if type nvm &> /dev/null
then
    nvm use
fi
PATH="${script_dir}/node_modules/.bin:${PATH}"

if ! diff <(node --version | cut --delimiter=. --fields=1-2 | tr --delete v) <(cut --delimiter=. --fields=1-2 "${script_dir}/.nvmrc")
then
    # shellcheck disable=SC2016
    echo 'Wrong major/minor version of Node.js detected. Please run `nvm install` to update Node.js and then reset the dev env.' >&2
    exit 3
fi

set +o errexit +o nounset
if [[ -e "${script_dir}/.venv/bin/activate" ]]
then
    # shellcheck source=/dev/null
    . "${script_dir}/.venv/bin/activate"
fi

if ! diff <(python <<< 'import platform; print(platform.python_version())' | cut --delimiter=. --fields=1-2) <(cut --delimiter=. --fields=1-2 "${script_dir}/.python-version")
then
    # shellcheck disable=SC2016
    echo 'Wrong major/minor version of Python detected. Please run `pyenv install` to update Python and then reset the dev env.' >&2
    exit 4
fi
