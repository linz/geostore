# Source Code Layout

```
├── cli             - command line interface code
├── infra           - AWS infrastructure code
├── .github         - CI/CD code
│   └── workflows
│       └── ci.yml
│       └── cd.yml
├── LICENSE
└── README.md
```


# Software Stack
* Python 3 - CLI
* [AWS Python CDK](https://aws.amazon.com/cdk/) - AWS infrastructure
    provisioning
* [Localstack](https://www.pylint.org/) - local AWS development environment


# Software Development Conventions
## General
* submit Pull Request for all changes, wait for CI/CD passing and review
* use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
    style commit messages
* always provide scope for commit message type (ex.: `fix(infra): fixed xyz,
    feat(cli): new xyz)`
* use preferably 80 characters line limit (max 120)
* development code lives in `master` branch
* release code and tags lives in release branches
* [Semantic Versioning](https://semver.org/) is used for software versioning
* release branch naming convention is `release-<MAJOR>.<MINOR>` (ex.: `release-1.3`)
* release tag naming convention is `<MAJOR>.<MINOR>.<PATCH>` (ex.: `1.3.1`)

# Python Development Conventions
* code must be checked by [Pylint](https://www.pylint.org/) and
    issue free
* code must be formatted by [Black](https://github.com/psf/black)


