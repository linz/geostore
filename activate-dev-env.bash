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

nvm use
PATH="${script_dir}/node_modules/.bin:${PATH}"

if ! diff <(node --version | tr --delete v) .nvmrc
then
    # shellcheck disable=SC2016
    echo 'Please run `nvm install` to update Node.js and then reset the dev env.' >&2
    exit 3
fi

set +o errexit +o nounset
# shellcheck source=/dev/null
. .venv/bin/activate

if ! diff <(python <<< 'import platform; print(platform.python_version())') .python-version
then
    # shellcheck disable=SC2016
    echo 'Please run `pyenv install` to update Python and then reset the dev env.' >&2
    exit 4
fi
