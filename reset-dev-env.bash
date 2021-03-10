#!/usr/bin/env bash

set -o errexit

usage() {
    cat >&2 << 'EOF'
./reset-dev-env.bash --all
./reset-dev-env.bash [--delete] [--hooks] [--node] [--python] [--submodule]
./reset-dev-env.bash --help

`--all` implies `--delete --hooks --node --python --submodule`.
EOF
}

arguments="$(getopt --options '' \
    --longoptions all,delete,help,hooks,node,python,submodule --name "$0" -- "$@")"
eval set -- "$arguments"
unset arguments

while true
do
    case "$1" in
        --all)
            delete=1
            hooks=1
            node=1
            python=1
            submodule=1
            shift
            ;;
        --delete)
            delete=1
            shift
            ;;
        --help)
            usage
            exit
            ;;
        --hooks)
            hooks=1
            shift
            ;;
        --node)
            node=1
            shift
            ;;
        --python)
            python=1
            shift
            ;;
        --submodule)
            submodule=1
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            printf 'Not implemented: %q\n' "$1" >&2
            exit 1
            ;;
    esac
done

if [[ -z "${hooks-}" ]] \
    && [[ -z "${node-}" ]] \
    && [[ -z "${python-}" ]] \
    && [[ -z "${submodule-}" ]]
then
    usage
    exit 1
fi

cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ -n "${delete-}" ]]
then
    echo "Cleaning Git repository"
    # TODO: Remove next line after fixing https://github.com/linz/geospatial-data-lake/issues/253
    rm --force --recursive cdk.out
    git clean -d --exclude='.idea' --force -x
fi

if [[ -n "${submodule-}" ]]
then
    echo "Updating submodules"
    git submodule update --init
fi

if [[ -n "${node-}" ]]
then
    if [[ -n "${delete-}" ]]
    then
        echo "Removing Node.js packages"
        rm --force --recursive ./node_modules
    fi

    echo "Installing Node.js packages"
    npm ci
fi

if [[ -n "${python-}" ]]
then
    if [[ -n "${delete-}" ]]
    then
        echo "Removing Python packages"
        rm --force --recursive ./.venv
    fi

    echo "Installing Python packages"
    poetry install --extras='cdk check_files_checksums check_stac_metadata content_iterator datasets dataset_versions import_dataset import_status' --remove-untracked
fi

if [[ -n "${hooks-}" ]]
then
    echo "Installing Git hooks"

    # shellcheck source=/dev/null
    . .venv/bin/activate

    pre-commit install --hook-type=commit-msg --overwrite
    pre-commit install --hook-type=pre-commit --overwrite
fi
