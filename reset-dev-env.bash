#!/usr/bin/env bash

set -o errexit

cd "$(dirname "${BASH_SOURCE[0]}")"

npm ci

poetry install --extras='cdk datasets-endpoint'

# shellcheck source=/dev/null
. .venv/bin/activate

pre-commit install --hook-type=commit-msg --overwrite
pre-commit install --hook-type=pre-commit --overwrite
