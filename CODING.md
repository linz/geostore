# Coding Conventions

## Software Stack

- [Python](https://www.python.org/) - Application code
- [AWS CDK](https://aws.amazon.com/cdk/) - AWS infrastructure provisioning
- [GitHub](https://github.com/) - Source code distribution and integration pipeline
- [ZenHub](https://app.zenhub.com/) - Issue tracking

## Software Development Conventions

- Any new code should first go on a branch
- Submit pull request for a branch, wait for a review and passing pipeline before merging
- Release code and tags lives in release branches
- Use [Semantic Versioning](https://semver.org/)
- Release branch naming convention is `release-<MAJOR>.<MINOR>` (ex.: `release-1.3`)
- Release tag naming convention is `<MAJOR>.<MINOR>.<PATCH>` (ex.: `1.3.1`)
- The formatting of code, configuration and commits are enforced by Git hooks
   - Use [Conventional Commits](https://www.conventionalcommits.org/) style commit messages
