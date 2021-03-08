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

## Code review checklist

This document is meant to give general hints for code reviewers. It should not be considered a complete set of things to consider, and should not include anything which is currently being validated automatically.

### Dockerfiles

- Don't pin `apt` package versions.

  [Rationale](https://github.com/linz/geospatial-data-lake/pull/171#discussion_r553474554):

   - Distributions should only get patch-level upgrades, making breakage unlikely.
   - Manually upgrading these packages in a timely fashion is a fair bit of overhead.
   - To be on the safe side we'd have to lock the versions of the dependencies in addition to the top-level packages, which is even more overhead. Apt AFAIK does not have a lockfile mechanism like NPM and Poetry.
   - [Dependabot does not yet patch Dockerfile package installs](https://github.com/dependabot/dependabot-core/issues/2129). This would fix all of the issues above, probably flipping this recommendation.

### Testing

- Make sure the test name says what the code should do, not what it should *not* do.

  Bad example: `should not [action] when [state]`.

  Rationale: There are an infinite number of ways not to do the action. Should it skip the action, throw an exception, change the state or it put the action into a queue for reprocessing? Stating positively what the code should do makes it easier to compare the test name to its implementation to judge whether the action is appropriate and that the name is accurate.

- Use precise action wording.

  Precise examples:

   - `should return HTTP 200 when creating item`
   - `should log success message when creating item`
   - `should return ID when creating item`

  Vague examples:

   - `should fail when [state]` is not helpful, because there are an infinite number of ways in which code could "fail". A better name says something about how the code *handles* the failure, such as `should return error message when …` or `should throw FooError when …`.
   - `should succeed when [state]` has the same issue, even though there are typically only a few application-specific (for example, HTTP 200 response) or language-specific (for example, returning without throwing an exception) ways the code could reasonably be said to "succeed". This also often ends up hiding the fact that more than one thing indicates success, and each of them should probably be tested in isolation (for example, the precise examples above, each of which are side effects of the same action).

  Rationale: Precise names help with verifying that the test does what it says it should do, makes it easier to search through the tests for similar ones to use as a template for a new one, and makes it faster to understand what's gone wrong when an old test fails in CI.

### AWS Lambda

- To speed up our lambdas, boto3 clients and other large pieces of setup should be initialised outside the main lambda handler. See [Best practices for working with AWS Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html) for more tips.
- Lambda runtime has been configured to 60 seconds. Higher than the minimum runtime of 3 seconds and lower than maximum of 15 minutes. This can be increased if a Lambda can be demonstrated to require more runtime than 60 seconds.

### Code reuse

- Group code by relatedness, not by reuse. Basically, putting a bunch of stuff in a single file should be justified by more than "they are all being used in more than one place". Putting all the utility code in one place creates clutter, makes it harder to find reusable pieces, and makes it impossible to create minimal sets of code to deploy to different services.

   Counter-example: conftest.py is a pytest-specific file for fixtures. We don't want to change our configuration simply to allow splitting this up.
