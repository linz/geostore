# Geostore

[![Deploy](https://github.com/linz/geostore/actions/workflows/deploy.yml/badge.svg)](https://github.com/linz/geostore/actions/workflows/deploy.yml)
[![CodeQL Analysis](https://github.com/linz/geostore/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/linz/geostore/actions/workflows/codeql-analysis.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Coverage: 100%](https://img.shields.io/badge/Coverage-100%25-brightgreen.svg)](https://pytest.org/)
[![hadolint: passing](https://img.shields.io/badge/hadolint-passing-brightgreen)](https://github.com/hadolint/hadolint)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg)](https://github.com/prettier/prettier)
[![pylint: passing](https://img.shields.io/badge/pylint-passing-brightgreen)](https://www.pylint.org/)
[![Python: 3.8](https://img.shields.io/badge/Python-3.8-blue)](https://www.python.org/)
[![shellcheck: passing](https://img.shields.io/badge/shellcheck-passing-brightgreen)](https://www.shellcheck.net/)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)

LINZ central storage, management and access solution for important geospatial datasets. Developed by
[Land Information New Zealand](https://github.com/linz).

# Prerequisites

## Geostore VPC

A Geostore VPC must exist in your AWS account before deploying this application. AT LINZ, VPCs are
managed internally by the IT team. If you are deploying this application outside LINZ, you will need
to create a VPC with the following tags:

- "ApplicationName": "geostore"
- "ApplicationLayer": "networking"

You can achieve this by adding the `networking_stack` (`infrastructure/networking_stack.py)` into
`app.py` before deployment as a dependency of `application_stack`
(`infrastructure/application_stack.py`).

## Verify infrastructure settings

This infrastructure by default includes some Toitū Te Whenua-/LINZ-specific parts, controlled by
settings in cdk.json. To disable these, simply remove the context entries or set them to `false`.
The settings are:

- `enableLDSAccess`: if true, gives the LINZ Data Service/Koordinates access to the storage bucket.

# Development setup

One-time setup, assuming you are in the project directory:

1. Install and configure Docker:
   1. Install the package: `sudo apt install docker.io`
   1. Add yourself to the "docker" group: `sudo usermod --append --groups docker "$USER"`
   1. Either log out and back in, or run `newgrp docker` to enable the new group for yourself in the
      current terminal.
1. [Install and enable `pyenv`](https://github.com/pyenv/pyenv#installation):

   1. Install Python build environment:

      ```bash
      sudo apt-get update
      sudo apt-get install --no-install-recommends build-essential curl libbz2-dev libffi-dev liblzma-dev libncurses5-dev libreadline-dev libsqlite3-dev libssl-dev libxml2-dev libxmlsec1-dev llvm make tk-dev wget xz-utils zlib1g-dev
      ```

   1. `curl https://pyenv.run | bash`
   1. Add the following to ~/.bashrc (wraps the upstream instructions to not do anything if `pyenv`
      is not installed):

      ```bash
      # Pyenv <https://github.com/pyenv/pyenv>
      if [[ -e "${HOME}/.pyenv" ]]
      then
          PATH="${HOME}/.pyenv/bin:${PATH}"
          eval "$(pyenv init --path)"
          eval "$(pyenv init -)"
          eval "$(pyenv virtualenv-init -)"
      fi
      ```

   1. Restart your shell: `exec "$SHELL"`.
   1. Install the Python version used in this project: `pyenv install`.
   1. Restart your shell again: `exec "$SHELL"`.
   1. Verify setup:
      `diff <(python <<< 'import platform; print(platform.python_version())') .python-version` -
      should produce no output.

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

   1. Add the following to ~/.bashrc (wraps the upstream instructions to not do anything if `nvm` is
      not installed):

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

   1. Add the executables directory to your path in ~/.bashrc (replace the project path with the
      path to this directory):

      ```bash
      if [[ -d "${HOME}/dev/geostore" ]]
      then
          PATH="${HOME}/dev/geostore/node_modules/.bin:${PATH}"
      fi
      ```

   1. Verify setup: `cdk --version`

1. Optional: Enable [Dependabot alerts by email](https://github.com/settings/notifications). (This
   is optional since it currently can't be set per repository or organisation, so it affects any
   repos where you have access to Dependabot alerts.)

Re-run `./reset-dev-env.bash` when packages change.

Re-run `. .venv/bin/activate` in each shell.

# AWS Infrastructure deployment

1. [Configure AWS](https://confluence.linz.govt.nz/display/GEOD/Login+to+AWS+Service+Accounts+via+Azure+in+Command+Line)
1. Get AWS credentials (see: https://www.npmjs.com/package/aws-azure-login) for 12 hours:

   ```bash
   aws-azure-login --no-prompt --profile=<AWS-PROFILE-NAME>
   ```

1. Environment variables

   - **`GEOSTORE_ENV_NAME`:** set deployment environment. For your personal development stack: set
     GEOSTORE_ENV_NAME to your username.

     ```bash
     export GEOSTORE_ENV_NAME="$USER"
     ```

     Other values used by CI pipelines include: prod, nonprod, ci, dev or any string without spaces.
     Default: test.

   * **`RESOURCE_REMOVAL_POLICY`:** determines if resources containing user content like Geostore
     Storage S3 bucket or application database tables will be preserved even if they are removed
     from stack or stack is deleted. Supported values:
     - DESTROY: destroy resource when removed from stack or stack is deleted (default)
     - RETAIN: retain orphaned resource when removed from stack or stack is deleted

   - **`GEOSTORE_SAML_IDENTITY_PROVIDER_ARN`:** SAML identity provider AWS ARN.

1. Bootstrap CDK (only once per profile)

   ```bash
   cdk --profile=<AWS-PROFILE-NAME> bootstrap aws://unknown-account/ap-southeast-2
   ```

1. Deploy CDK stack

   ```bash
   cdk --profile=<AWS-PROFILE-NAME> deploy --all
   ```

   Once comfortable with CDK you can add `--require-approval=never` above to deploy
   non-interactively.

If you `export AWS_PROFILE=<AWS-PROFILE-NAME>` you won't need the `--profile=<AWS-PROFILE-NAME>`
arguments above.

# Development

## Adding or updating Python dependencies

To add a development-only package: `poetry add --dev PACKAGE='*'`

To add a production package:

1. Install the package using `poetry add --optional PACKAGE='*'`.
1. Put the package in alphabetical order within the list.
1. Mention the package in the relevant lists in `[tool.poetry.extras]`.
   - When adding a new "extra", make sure to install it in `reset-dev-env.bash`.

- Make sure to update packages separately from adding packages. Basically, follow this process
  before running `poetry add`, and do the equivalent when updating Node.js packages or changing
  Docker base images:

  1.  Check out a new branch on top of origin/master:
      `git checkout -b update-python-packages origin/master`.
  1.  Update the Python packages: `poetry update`. The rest of the steps are only necessary if this
      step changes poetry.lock. Otherwise you can just change back to the original branch and delete
      "update-python-packages".
  1.  Commit, push and create pull request.
  1.  Check out the branch where you originally wanted to run `poetry add`.
  1.  Rebase the branch onto the package update branch: `git rebase update-python-packages`.

  At this point any `poetry add` commands should not result in any package updates other than those
  necessary to fulfil the new packages' dependencies.

  Rationale: Keeping upgrades and other packages changes apart is useful when reading/bisecting
  history. It also makes code review easier.

- When there's a merge conflict in poetry.lock, first check whether either or both commits contain a
  package upgrade:

  - If neither of them do, simply `git checkout --ours -- poetry.lock && poetry lock --no-update`.
  - If one of them does, check out that file (`git checkout --ours -- poetry.lock` or
    `git checkout --theirs -- poetry.lock`) and run `poetry lock --no-update` to regenerate
    `poetry.lock` with the current package versions.
  - If both of them do, manually merge `poetry.lock` and run `poetry lock --no-update`.

  Rationale: This should avoid accidentally down- or upgrading when resolving a merge conflict.

- Update the code coverage minimum in pyproject.toml and the badge above on branches which increase
  it.

  Rationale: By updating this continuously we avoid missing test regressions in new branches.

## Running tests

To launch full test suite: `pytest tests/`

## Debugging

To start debugging at a specific line, insert `import ipdb; ipdb.set_trace()`.

To debug a test run, add `--capture=no` to the `pytest` arguments. You can also automatically start
debugging at a test failure point with `--pdb --pdbcls=IPython.terminal.debugger:Pdb`.

## Upgrading CI runner

[`jobs.<job_id>.runs-on`](https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-syntax-for-github-actions#jobsjob_idruns-on)
in .github sets the runner type per job. We should make sure all of these use the latest specific
("ubuntu-YY.MM" as opposed to "ubuntu-latest") Ubuntu LTS version, to make sure the version changes
only when we're ready for it.

## GitHub Actions cache clearing

To throw away the current cache (for example in case of a cache corruption), simply change the
[`CACHE_SEED` repository "secret"](https://github.com/linz/geostore/settings/secrets/actions/CACHE_SEED),
for example to the current timestamp (`date +%s`). Subsequent jobs will then ignore the existing
cache.
