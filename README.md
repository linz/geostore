[![GitHub Actions Status](https://github.com/linz/geospatial-data-lake/workflows/Build/badge.svg)](https://github.com/linz/geospatial-data-lake/actions) ![](https://img.shields.io/badge/WIP-Work%20In%20Progress-orange)

# Geospatial Data Lake
Central storage, management and access for important geospatial datasets
Developed by [Land Information New Zealand](https://github.com/linz)


## Dependencies Installation
### Python Virtual Environment (for Python CLI and AWS CDK)
* Create and activate a Python virtual environment

    ```bash
    $ python3 -m venv .venv
    $ source .venv/bin/activate
    ```
* Upgrade pip

    ```bash
    $ pip install --upgrade pip
    ```
* [Install Poetry](https://python-poetry.org/docs/#installation)
* Install the dependencies:

    ```bash
    $ poetry install --extras='cdk datasets-endpoint'
    ```

### AWS CDK Environment (AWS Infrastructure)
* Install NVM (use latest version)

    ```bash
    $ curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v<LATEST-VERSION>/install.sh | bash
    ```
* Enable NVM

    ```bash
    $ export NVM_DIR="$HOME/.nvm"
    $ [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
    $ [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
    ```
* Install latest LTS Node version

    ```bash
    $ nvm install --lts
    ```
* Install latest AWS CDK version

    ```bash
    $ npm install
    ```


## AWS Infrastructure Deployment (CDK Stack)
* Get AWS credentials (see: https://www.npmjs.com/package/aws-azure-login)

    ```bash
    $ ./node_modules/.bin/aws-azure-login -p <geospatial-data-lake-nonprod|geospatial-data-lake-prod>
    ```
* Deploy CDK stack

    ```bash
    $ cd infra
    $ export ENVIRONMENT_TYPE=dev|nonprod|prod
    $ ../node_modules/.bin/cdk --profile <geospatial-data-lake-nonprod|geospatial-data-lake-prod> bootstrap aws://unknown-account/ap-southeast-2
    $ ../node_modules/.bin/cdk deploy --profile <geospatial-data-lake-nonprod|geospatial-data-lake-prod> geospatial-data-lake
    ```


## Development
* Install Git hooks

    ```bash
    $ pre-commit install --hook-type=commit-msg --overwrite
    $ pre-commit install --hook-type=pre-commit --overwrite
    ```
   You will need to run this whenever upgrading the Python minor version, such as 3.8 to 3.9, to avoid messages like
   
   > /usr/bin/env: ‘python3.8’: No such file or directory
* Install automatic Pylint code checks for your editor or run it by hand

    ```
    $ pylint <DIRECTORY-PATH>
    ```
* Install automatic Black code formatting for your editor or run it by hand

     ```
     $ black . --check --diff
     ```

To add a development-only package: `poetry add --dev PACKAGE='*'`

To add a production package:

1. Install the package using `poetry add --optional PACKAGE='*'`.
1. Put the package in alphabetical order within the list.
1. Mention the package in the relevant lists in `[tool.poetry.extras]`.
