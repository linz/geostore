[![GitHub Actions Status](https://github.com/linz/geospatial-data-lake/workflows/Build/badge.svg)](https://github.com/linz/geospatial-data-lake/actions) ![](https://img.shields.io/badge/WIP-Work%20In%20Progress-orange)

# Geospatial Data Lake
Central storage, management and access for important geospatial datasets
Developed by [Land Information New Zealand](https://github.com/linz)


# Dependencies Installation
## Python Virtual Environment (for Python CLI and AWS CDK)
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
$ poetry install
```

## AWS CDK Environment (AWS Infrastructure)
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
$ npm install -g aws-cdk
```


## AWS Infrastructure Deployment (CDK Stack)
* Get AWS credentials (see: https://www.npmjs.com/package/aws-azure-login)

```bash
$ aws-azure-login -p <geospatial-data-lake-nonprod|geospatial-data-lake-prod>
```

* Deploy CDK stack

```bash
$ export ENVIRONMENT_TYPE=dev|nonprod|prod
$ cdk --profile <geospatial-data-lake-nonprod|geospatial-data-lake-prod> bootstrap aws://unknown-account/ap-southeast-2
$ cdk deploy --profile <geospatial-data-lake-nonprod|geospatial-data-lake-prod> geospatial-data-lake
```


## Development
* Install commit-msg git hook

```bash
$ pre-commit install --hook-type=commit-msg --overwrite
$ pre-commit install --hook-type=pre-commit --overwrite
```

* Install automatic Pylint code checks for your editor or run it by hand
```
$ pylint <DIRECTORY-PATH>
```

* Install automatic Black code formatting for your editor or run it by hand
```
$ black . --check --diff
```
