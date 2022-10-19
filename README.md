# Geostore

[![Deploy](https://github.com/linz/geostore/actions/workflows/deploy.yml/badge.svg)](https://github.com/linz/geostore/actions/workflows/deploy.yml)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/linz/geostore.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/linz/geostore/alerts/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/linz/geostore.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/linz/geostore/context:python)
[![CodeQL Analysis](https://github.com/linz/geostore/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/linz/geostore/actions/workflows/codeql-analysis.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Coverage: 100% branches](https://img.shields.io/badge/Coverage-100%25%20branches-brightgreen.svg)](https://pytest.org/)
[![Dependabot Status](https://badgen.net/badge/Dependabot/enabled?labelColor=2e3a44&color=blue)](https://github.com/linz/geostore/network/updates)
[![hadolint: passing](https://img.shields.io/badge/hadolint-passing-brightgreen)](https://github.com/hadolint/hadolint)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Kodiak](https://badgen.net/badge/Kodiak/enabled?labelColor=2e3a44&color=F39938)](https://kodiakhq.com/)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg)](https://github.com/prettier/prettier)
[![pylint: passing](https://img.shields.io/badge/pylint-passing-brightgreen)](https://www.pylint.org/)
[![Python: 3.9](https://img.shields.io/badge/Python-3.9-blue)](https://www.python.org/)
[![shellcheck: passing](https://img.shields.io/badge/shellcheck-passing-brightgreen)](https://www.shellcheck.net/)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)

Central storage, management and access solution for important geospatial datasets. Developed by
[Toitū Te Whenua Land Information New Zealand](https://github.com/linz).

## Prerequisites

### Geostore VPC

A Geostore VPC must exist in your AWS account before deploying this application. At Toitū Te Whenua
LINZ, VPCs are managed internally by the IT team. If you are deploying this application outside
Toitū Te Whenua LINZ, you will need to create a VPC with the following tags:

-  "ApplicationName": "geostore"
-  "ApplicationLayer": "networking"

You can achieve this by adding the `networking_stack` (`infrastructure/networking_stack.py)` into
`app.py` before deployment as a dependency of `application_stack`
(`infrastructure/application_stack.py`).

### Verify infrastructure settings

This infrastructure by default includes some Toitū Te Whenua LINZ-specific parts, controlled by
settings in cdk.json. To disable these, simply remove the context entries or set them to `false`.
The settings are:

-  `enableLDSAccess`: if true, gives Toitū Te Whenua LINZ Data Service/Koordinates read access to
   the storage bucket.
-  `enableOpenTopographyAccess`: if true, gives OpenTopography read access to the storage bucket.

## Development setup

One-time setup which generally assumes that you're in the project directory.

### Common

1. [Install Docker](https://docs.docker.com/engine/install/ubuntu/)
2. Configure Docker:
   1. Add yourself to the "docker" group: `sudo usermod --append --groups=docker "$USER"`
   2. Log out and back in to enable the new group
3. Set up an AWS Azure login shortcut like this in your .bashrc:

   ```shell
   aws-azure-login() {
       docker run --interactive --rm --tty --volume="${HOME}/.aws:/root/.aws" sportradar/aws-azure-login:2021062807125386530a "$@"
   }
   ```

### Ubuntu

1. Install [`nvm`](https://github.com/nvm-sh/nvm#installing-and-updating):
   ```bash
   cd "$(mktemp --directory)"
   wget https://raw.githubusercontent.com/nvm-sh/nvm/master/install.sh
   echo 'b674516f001d331c517be63c1baeaf71de6cbb6d68a44112bf2cff39a6bc246a install.sh' | sha256sum --check && bash install.sh
   ```
2. Install [Poetry](https://python-poetry.org/docs/master/#installation):
   ```bash
   cd "$(mktemp --directory)"
   wget https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py
   echo 'b35d059be6f343ac1f05ae56e8eaaaebb34da8c92424ee00133821d7f11e3a9c install-poetry.py' | sha256sum --check && python3 install-poetry.py
   ```
3. Install [Pyenv](https://github.com/pyenv/pyenv#installation):
   ```bash
   sudo apt-get update
   sudo apt-get install --no-install-recommends build-essential curl libbz2-dev libffi-dev liblzma-dev libncurses5-dev libreadline-dev libsqlite3-dev libssl-dev libxml2-dev libxmlsec1-dev llvm make tk-dev wget xz-utils zlib1g-dev
   cd "$(mktemp --directory)"
   wget https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer
   echo '3aa49f2b3b77556272a80a01fe44d46733f4862dbbbc956002dc944c428bebd8 pyenv-installer' | sha256sum --check && bash pyenv-installer
   ```
4. Enable the above by adding the following to your `~/.bashrc`:

   ```bash
   if [[ -e "${HOME}/.local/bin" ]]
   then
       PATH="${HOME}/.local/bin:${PATH}"
   fi

   # nvm <https://github.com/nvm-sh/nvm>
   if [[ -d "${HOME}/.nvm" ]]
   then
       export NVM_DIR="${HOME}/.nvm"
       # shellcheck source=/dev/null
       [[ -s "${NVM_DIR}/nvm.sh" ]] && . "${NVM_DIR}/nvm.sh"
       # shellcheck source=/dev/null
       [[ -s "${NVM_DIR}/bash_completion" ]] && . "${NVM_DIR}/bash_completion"
   fi

   # Pyenv <https://github.com/pyenv/pyenv>
   if [[ -e "${HOME}/.pyenv" ]]
   then
       PATH="${HOME}/.pyenv/bin:${PATH}"
       eval "$(pyenv init --path)"
       eval "$(pyenv init -)"
       eval "$(pyenv virtualenv-init -)"
   fi
   ```

5. Configure Docker:
   1. Add yourself to the "docker" group: `sudo usermod --append --groups=docker "$USER"`
   1. Log out and back in to enable the new group
6. [Install project Node.js](https://github.com/nvm-sh/nvm#long-term-support): `nvm install`
7. Run `./reset-dev-env.bash --all` to install packages.
8. Enable the dev environment: `. activate-dev-env.bash`.
9. Optional: Enable [Dependabot alerts by email](https://github.com/settings/notifications). (This
   is optional since it currently can't be set per repository or organisation, so it affects any
   repos where you have access to Dependabot alerts.)

Re-run `./reset-dev-env.bash` when packages change. One easy way to use it pretty much seamlessly is
to run it before every workday, with a crontab entry like this template:

```crontab
HOME='/home/USERNAME'
0 2 * * 1-5 export PATH="${HOME}/.pyenv/shims:${HOME}/.pyenv/bin:${HOME}/.poetry/bin:/root/bin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/run/current-system/sw/bin" && cd "PATH_TO_GEOSTORE" && ./reset-dev-env.bash --all
```

Replace `USERNAME` and `PATH_TO_GEOSTORE` with your values, resulting in something like this:

```crontab
HOME='/home/jdoe'
0 2 * * 1-5 export PATH="${HOME}/.pyenv/shims:${HOME}/.pyenv/bin:${HOME}/.poetry/bin:/root/bin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/run/current-system/sw/bin" && cd "${HOME}/dev/geostore" && ./reset-dev-env.bash --all
```

Re-run `. activate-dev-env.bash` in each shell.

### Nix

1. Run `nix-shell`.
1. Optional: [Install and configure `lorri`](https://github.com/nix-community/lorri) and run
   `direnv allow .` to load the Nix shell whenever you `cd` into the project.

Restart your `nix-shell` when packages change.

When setting up the project SDK point it to `.run/python`, which is a symlink to the latest Nix
shell Python executable.

### Optional

Enable [Dependabot alerts by email](https://github.com/settings/notifications). (This is optional
since it currently can't be set per repository or organisation, so it affects any repos where you
have access to Dependabot alerts.)

## AWS Infrastructure deployment

1. Configure a
   [named AWS profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html)
   with permission to deploy stacks

   -  [Instructions for Toitū Te Whenua LINZ employees](https://confluence.linz.govt.nz/display/GEOD/Login+to+AWS+Service+Accounts+via+Azure+in+Command+Line)

1. Environment variables

   -  **`GEOSTORE_ENV_NAME`:** set deployment environment. For your personal development stack: set
      GEOSTORE_ENV_NAME to your username.

      ```bash
      export GEOSTORE_ENV_NAME="$USER"
      ```

      Other values used by CI pipelines include: prod, nonprod, ci, dev or any string without
      spaces. Default: test.

   -  **`AWS_DEFAULT_REGION`:** The region to deploy to. For practical reasons this is the nearest
      region.

      ```bash
      export AWS_DEFAULT_REGION=ap-southeast-2
      ```

   -  **`RESOURCE_REMOVAL_POLICY`:** determines if resources containing user content like Geostore
      Storage S3 bucket or application database tables will be preserved even if they are removed
      from stack or stack is deleted. Supported values:

      -  DESTROY: destroy resource when removed from stack or stack is deleted (default)
      -  RETAIN: retain orphaned resource when removed from stack or stack is deleted

   -  **`GEOSTORE_SAML_IDENTITY_PROVIDER_ARN`:** SAML identity provider AWS ARN.

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

## Development

### Third party updates

When Dependabot updates any Python dependencies in pip requirements files (`*.txt`), make sure to
run `./generate-requirements-files.bash` with the relevant path to update the version of all its
dependencies. Sometimes this will _revert_ the file to the previous state, which means that specific
dependency update is _not_ compatible with the rest of the packages in the same file. For example,
say `geostore/pip.txt` lists a package `foo`, which depends on `bar~=1.0`. This information is not
part of the requirements file, so Dependabot might update `bar` to version 2.0, not being aware that
it's incompatible with the current version of `foo`. `generate-requirements-files.bash` effectively
re-checks this, creating a file with a compatible set of dependencies, which may mean reverting the
update done by Dependabot. In this case, simply close the Dependabot PR.

We're using poetry2nix to generate a Nix derivation from the `poetry.lock` file, to allow people to
develop this project with either Nix or Poetry[^1]. Sometimes package updates will break the Nix
shell, usually because Python packages don't list all their build dependencies. These need to be set
up as a `poetry2nix` _override._ First try upgrading nixpkgs using `./bump-nixpkgs.bash` and
re-running `nix-shell`; maybe the latest stable poetry2nix already has an override for this package.
If not, you either have to work one out yourself (see
[upstream overrides](https://github.com/nix-community/poetry2nix/tree/master/overrides)) or
[report it](https://github.com/nix-community/poetry2nix/issues).

### Adding or updating Python dependencies

To add a development-only package: `poetry add --dev --lock PACKAGE='*'`

To add a production package:

1. Add the package using `poetry add --lock --optional PACKAGE='*'`.
1. Put the package in alphabetical order within the list.
1. Mention the package in the relevant lists in `[tool.poetry.extras]`.

-  Make sure to update packages separately from adding packages. Basically, follow this process
   before running `poetry add`, and do the equivalent when updating Node.js packages or changing
   Docker base images:

   1. Check out a new branch on top of origin/master:
      `git checkout -b update-python-packages origin/master`.
   1. Update the Python packages: `poetry update --lock`. The rest of the steps are only necessary
      if this step changes poetry.lock. Otherwise you can just change back to the original branch
      and delete "update-python-packages".
   1. Commit, push and create pull request.
   1. Check out the branch where you originally wanted to run `poetry add`.
   1. Rebase the branch onto the package update branch: `git rebase update-python-packages`.

   At this point any `poetry add` commands should not result in any package updates other than those
   necessary to fulfil the new packages' dependencies.

   Rationale: Keeping upgrades and other packages changes apart is useful when reading/bisecting
   history. It also makes code review easier.

-  When there's a merge conflict in poetry.lock, first check whether either or both commits contain
   a package upgrade:

   -  If neither of them do, simply `git checkout --ours -- poetry.lock && poetry lock --no-update`.
   -  If one of them does, check out that file (`git checkout --ours -- poetry.lock` or
      `git checkout --theirs -- poetry.lock`) and run `poetry lock --no-update` to regenerate
      `poetry.lock` with the current package versions.
   -  If both of them do, manually merge `poetry.lock` and run `poetry lock --no-update`.

   Rationale: This should avoid accidentally down- or upgrading when resolving a merge conflict.

-  Update the code coverage minimum in pyproject.toml and the badge above on branches which increase
   it.

   Rationale: By updating this continuously we avoid missing test regressions in new branches.

### Upgrading Python version

To minimise the chance of discrepancies between environments it is important to run the same (or as
close as possible) version of Python in the development environment, in the pipeline, and in
deployed instances. At the moment the available versions are constrained by the following:

-  The
   [Ubuntu packages](https://packages.ubuntu.com/search?keywords=python3&searchon=names&exact=1&suite=all&section=all)
   used in the [Dockerfile](geostore/Dockerfile)
-  The [AWS base images](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html) used as
   [Lambda runtimes](infrastructure/constructs/lambda_config.py)
-  The [pyenv versions](https://github.com/pyenv/pyenv) used for
   [local development](.python-version)
-  The [supported Poetry versions](https://python-poetry.org/docs/#system-requirements) used for all
   [dependencies](pyproject.toml)

When updating Python versions you have to check that all of the above can be kept at the same minor
version, and ideally at the same patch level.

### Running tests

Prerequisites:

-  Authenticated to a profile which has access to a deployed Geostore.

To launch full test suite, run `pytest`.

### Debugging

To start debugging at a specific line, insert `import ipdb; ipdb.set_trace()`.

To debug a test run, add `--capture=no` to the `pytest` arguments. You can also automatically start
debugging at a test failure point with `--pdb --pdbcls=IPython.terminal.debugger:Pdb`.

### Upgrading CI runner

[`jobs.<job_id>.runs-on`](https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-syntax-for-github-actions#jobsjob_idruns-on)
in .github sets the runner type per job. We should make sure all of these use the latest specific
("ubuntu-YY.MM" as opposed to "ubuntu-latest") Ubuntu LTS version, to make sure the version changes
only when we're ready for it.

### GitHub Actions cache clearing

To throw away the current cache (for example in case of a cache corruption), simply change the
[`CACHE_SEED` repository "secret"](https://github.com/linz/geostore/settings/secrets/actions/CACHE_SEED),
for example to the current timestamp (`date +%s`). Subsequent jobs will then ignore the existing
cache.

## Manual admin

### Delete dataset with versions

To do this, you'll need the dataset title and ID.

Once a dataset has some files in it, it's much harder to delete. This is intentional, to avoid
accidental loss of important and costly data. The following should be a complete set of actions to
delete a dataset, with template values in `UPPERCASE`. _Note the trailing slashes to make sure we
limit the commands to the specific dataset!_

1. Remove reference to the dataset from the
   [top-level `catalog.json`](https://s3.console.aws.amazon.com/s3/object/linz-geostore?region=ap-southeast-2&prefix=catalog.json):
   1. Download the file: `aws s3 cp s3://linz-geostore/catalog.json .`
   1. Manually edit the file and delete the object with the deleted dataset
   1. Re-upload the file: `aws s3 cp catalog.json s3://linz-geostore/catalog.json`
1. Remove the dataset from DynamoDB:
   1. Run `geostore dataset delete --id=DATASET_ID`
1. Delete dataset files from S3:
   1. Run `aws s3 rm --recursive s3://linz-geostore/DATASET_TITLE/`.
   1. Ask AWS support to remove the delete markers returned by
      `aws s3api list-object-versions --bucket=linz-geostore --prefix=DATASET_TITLE/ | jq .DeleteMarkers`.

[^1]:
    When using Nix, make sure to remove the `.venv` directory. Mixing Nix and Poetry leads to weird
    behaviour.
