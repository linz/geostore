[![GitHub Actions Status](https://github.com/linz/geospatial-data-lake/workflows/Build/badge.svg)](https://github.com/linz/geospatial-data-lake/actions) ![](https://img.shields.io/badge/WIP-Work%20In%20Progress-orange)

# Geospatial Data Lake
Central storage, management and access for important geospatial datasets
Developed by [Land Information New Zealand](https://github.com/linz)

## Development setup

One-time setup, assuming you are in the project directory:

1. Install and configure Docker:
   1. Install the package: `sudo apt install docker.io`
   1. Add yourself to the "docker" group: `sudo usermod --append --groups docker "$USER"`
   1. Either log out and back in, or run `newgrp docker` to enable the new group for yourself in the current terminal.
1. [Install and enable `pyenv`](https://github.com/pyenv/pyenv#installation):
    1. Install Python build environment:

        ```bash
        sudo apt-get update
        sudo apt-get install --no-install-recommends build-essential curl libbz2-dev libffi-dev liblzma-dev libncurses5-dev libreadline-dev libsqlite3-dev libssl-dev libxml2-dev libxmlsec1-dev llvm make tk-dev wget xz-utils zlib1g-dev
        ```
   1. `curl https://pyenv.run | bash`
   1. Add the following to ~/.bashrc (wraps the upstream instructions to not do anything if `pyenv` is not installed):

       ```bash
       # Pyenv <https://github.com/pyenv/pyenv>
       if [[ -e "${HOME}/.pyenv" ]]
       then
           PATH="${HOME}/.pyenv/bin:${PATH}"
           eval "$(pyenv init -)"
           eval "$(pyenv virtualenv-init -)"
       fi
       ```
   1. Restart your shell: `exec "$SHELL"`.
   1. Install the Python version used in this project: `pyenv install`.
   1. Restart your shell again: `exec "$SHELL"`.
   1. Verify setup: `diff <(python <<< 'import platform; print(platform.python_version())') .python-version` - should produce no output.
1. [Install and enable Poetry](https://python-poetry.org/docs/#installation):
   1. Install:

       ```bash
       curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
       ```
   1. Add the following to ~/.bashrc:

       ```bash
       # Poetry <https://python-poetry.org/>
       if [[ -e "${HOME}/.poetry" ]]
       then
           PATH="${HOME}/.poetry/bin:${PATH}"
       fi
       ```
   1. Restart your shell: `exec "$SHELL"`.
   1. Verify setup: `poetry --version`.
1. [Install and enable `nvm`](https://github.com/nvm-sh/nvm#installing-and-updating):
   1. Install (change the version number if you want to install a different one):

       ```bash
       curl https://raw.githubusercontent.com/nvm-sh/nvm/v0.37.2/install.sh | bash
       ```
   1. Add the following to ~/.bashrc (wraps the upstream instructions to not do anything if `nvm` is not installed):

       ```bash
       if [[ -d "${HOME}/.nvm" ]]
       then
           export NVM_DIR="${HOME}/.nvm"
           # shellcheck source=/dev/null
           [[ -s "${NVM_DIR}/nvm.sh" ]] && . "${NVM_DIR}/nvm.sh"
           # shellcheck source=/dev/null
           [[ -s "${NVM_DIR}/bash_completion" ]] && . "${NVM_DIR}/bash_completion"
       fi
       ```
   1. Restart your shell: `exec "$SHELL"`.
   1. Verify setup: `nvm --version`.
1. [Install latest `npm` LTS](https://github.com/nvm-sh/nvm#long-term-support): `nvm install --lts`
1. Run `./reset-dev-env.bash` to install packages.
1. Enable the virtualenv: `. .venv/bin/activate`.
1. Enable Node.js executables:
   1. Add the executables directory to your path in ~/.bashrc (replace the project path with the path to this directory):

       ```bash
       if [[ -d "${HOME}/dev/geospatial-data-lake" ]]
       then
           PATH="${HOME}/dev/geospatial-data-lake/node_modules/.bin:${PATH}"
       fi
       ```
   1. Verify setup: `cdk --version`

Re-run `./reset-dev-env.bash` when packages change.

Re-run `. .venv/bin/activate` in each shell.


## AWS Infrastructure Deployment (CDK Stack)

1. [Configure AWS](https://confluence.linz.govt.nz/display/GEOD/Login+to+AWS+Service+Accounts+via+Azure+in+Command+Line)
1. Get AWS credentials (see: https://www.npmjs.com/package/aws-azure-login)

    ```bash
    aws-azure-login --profile=<AWS-PROFILE-NAME>
    ```
1. Environment variables
* **DEPLOY_ENV:** set deployment environment. Recommended values: prod, nonprod, ci, dev or any
    string without spaces. Default: dev.
* **DATALAKE_USE_EXISTING_VPC:** determine if networking stack will use existing VPC in target AWS
    account or will create a new one. Existing VPC must to be used must contain following tags -
    "ApplicationName": "geospatial-data-lake", "ApplicationLayer": "networking".
    Allowed values: true, false. Default: false.
1. Bootstrap CDK (only once per profile)

    ```bash
    cdk --profile=<AWS-PROFILE-NAME> bootstrap aws://unknown-account/ap-southeast-2
    ```
1. Deploy CDK stack

    ```bash
    cdk --profile=<AWS-PROFILE-NAME> deploy --all
    ```

If you `export AWS_PROFILE=<AWS-PROFILE-NAME>` you won't need the `--profile=<AWS-PROFILE-NAME>` arguments above.

## Development

To add a development-only package: `poetry add --dev PACKAGE='*'`

To add a production package:

1. Install the package using `poetry add --optional PACKAGE='*'`.
1. Put the package in alphabetical order within the list.
1. Mention the package in the relevant lists in `[tool.poetry.extras]`.
   - When adding a new "extra", make sure to install it in `reset-dev-env.bash`.

### Upgrading CI runner

[`jobs.<job_id>.runs-on`](https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-syntax-for-github-actions#jobsjob_idruns-on) in .github sets the runner type per job. We should make sure all of these use the latest specific ("ubuntu-YY.MM" as opposed to "ubuntu-latest") Ubuntu LTS version, to make sure the version changes only when we're ready for it.

### Development patterns

- Commit package upgrades separately from package installs/removals. That is, if you want to run `poetry update`, make sure any existing changes to poetry.lock and pyproject.toml are already committed. Beware that `poetry lock` will also upgrade packages by default; see next point.

   Rationale: Keeping upgrades and other packages changes apart is useful when reading/bisecting history.
- When there's a merge conflict in poetry.lock, first check whether either or both commits contain a package upgrade:
   - If neither of them do, simply `git checkout --ours -- poetry.lock && poetry lock --no-update`.
   - If one of them does, check out that file (`git checkout --ours -- poetry.lock` or `git checkout --theirs -- poetry.lock`) and run `poetry lock --no-update` to regenerate `poetry.lock` with the current package versions.
   - If both of them do, manually merge `poetry.lock` and run `poetry lock --no-update`.

   Rationale: This should avoid accidentally down- or upgrading when resolving a merge conflict.
- Update the code coverage minimum in pyproject.toml on branches which increase it.

   Rationale: By updating this continuously we avoid missing test regressions in new branches.
