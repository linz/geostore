# Coding Conventions

## Software Stack

- [Python](https://www.python.org/) - Application code
- [AWS CDK](https://aws.amazon.com/cdk/) - AWS infrastructure provisioning
- [GitHub](https://github.com/) - Source code distribution and integration pipeline
- [ZenHub](https://app.zenhub.com/) - Issue tracking

## Software Development Conventions

### General

- submit Pull Request for all changes, wait for CI/CD passing and review
- use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) style commit messages
- always provide scope for commit message type (ex.: `fix(infra): fixed xyz, feat(cli): new xyz)`
- use preferably 80 characters line limit (max 120)
- development code lives in `master` branch
- release code and tags lives in release branches
- [Semantic Versioning](https://semver.org/) is used for software versioning
- release branch naming convention is `release-<MAJOR>.<MINOR>` (ex.: `release-1.3`)
- release tag naming convention is `<MAJOR>.<MINOR>.<PATCH>` (ex.: `1.3.1`)

### Python Development Conventions

- code must be checked by [Pylint](https://www.pylint.org/) and issue free
- code must be formatted by [Black](https://github.com/psf/black)


