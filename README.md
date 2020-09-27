[![GitHub Actions Status](https://github.com/linz/geospatial-data-lake/workflows/Build/badge.svg)](https://github.com/linz/geospatial-data-lake/actions)

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

* Upgrade pip and install the required dependencies

```bash
$ pip install --upgrade pip
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


## Python CLI Installation
* Install Python dependencies

```bash
$ pip install -r requirements.txt
```

* Install Data Lake

```bash
$ python ./setup.py install
```


## AWS Infrastructure Deployment (CDK Stack)
* Install Python CDK dependencies

```bash
$ pip install -r infra/requirements.txt
```

* Get AWS credentials (see: https://www.npmjs.com/package/aws-azure-login)

```bash
$ aws-azure-login -p <linz-data-lake-nonprod|linz-data-lake-prod>
```

* Deploy CDK stack

```bash
$ cdk deploy --profile <linz-data-lake-nonprod|linz-data-lake-prod> <data-lake-raster-nonprod|data-lake-raster-prod>
```


## Development
* Install Python development dependencies

```bash
$ pip install -r requirements-dev.txt
```

* Install commit-msg git hook

```bash
$ pre-commit install --hook-type commit-msg
```

* Install automatic Pylint code checks for your editor or run it by hand
```
$ pylint <DIRECTORY-PATH>
```

* Install automatic Black code formatting for your editor or run it by hand
```
$ black . --check --diff
```
