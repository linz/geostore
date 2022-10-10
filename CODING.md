# Coding Conventions

## Software Stack

-  [Python](https://www.python.org/) - Application code
-  [AWS CDK](https://aws.amazon.com/cdk/) - AWS infrastructure provisioning
-  [GitHub](https://github.com/) - Source code distribution and integration pipeline
-  [ZenHub](https://app.zenhub.com/) - Issue tracking

## Software Development Conventions

-  Any new code should first go on a branch
-  Assign tickets to yourself when you start work
-  Submit pull request for a branch, assign a reviewer, then wait for a review and passing pipeline
   before merging
-  Release code and tags lives in release branches
-  Use [Semantic Versioning](https://semver.org/)
-  Release branch naming convention is `release-<MAJOR>.<MINOR>` (ex.: `release-1.3`)
-  Release tag naming convention is `<MAJOR>.<MINOR>.<PATCH>` (ex.: `1.3.1`)
-  The formatting of code, configuration and commits are enforced by Git hooks
   -  Use [Conventional Commits](https://www.conventionalcommits.org/) style commit messages
   -  It's recommended to use a similar branch style, like `feat/create-oidc-providers`, to make
      branches easy to navigate
-  Link to third party issues you've had to work around, so that others understand it and can
   simplify it when the issue is resolved
-  Refactoring should be on separate branches from other changes
-  Add the "automerge" label to your PR if you're confident the pipeline will pass (since the
   deployment update test takes a while)

## Periodic work

At the start of january, run the "Update copyright year in license file" workflow manually. This
used to be scheduled to run automatically, but we're working around a
[bug](https://github.com/orgs/community/discussions/32197) for now.

## How to test a full import

Follow this guide step by step to import a dataset from your personal test stack.

1. Set up properties:

   ```shell
   export GEOSTORE_AWS_ACCOUNT_ID=632223577832 # Use 715898075157 in production
   export GEOSTORE_USER_ROLE_NAME="nonprod-api-users" # Use "api-users" in production
   ```

1. Set up properties of the source account and resources you're about to create there:

   ```shell
   export AWS_PROFILE='PERSONAL_PROFILE_ID'
   source_account_id='SOURCE_ACCOUNT_ID'
   policy_name="geostore-assumption-by-${GEOSTORE_USER_ROLE_NAME}"
   role_name="geostore-${GEOSTORE_USER_ROLE_NAME}"
   bucket_name='SOURCE_BUCKET_NAME'
   metadata_url="s3://${bucket_name}/PATH_TO_COLLECTION_JSON"
   ```

1. Log in to your personal profile as an admin user:

   ```shell
   aws-azure-login --no-prompt --profile="$AWS_PROFILE"
   ```

1. Create the assumption [policy](https://console.aws.amazon.com/iamv2/home?#/policies$customer):

   ```shell
   policy_arn="$(
       aws iam create-policy \
           --policy-name="${policy_name}" \
           --policy-document="{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Resource\": \"arn:aws:iam::${GEOSTORE_AWS_ACCOUNT_ID}:role/${GEOSTORE_USER_ROLE_NAME}\", \"Action\": \"sts:AssumeRole\"}]}" \
       | jq --raw-output .Policy.Arn
   )"
   ```

1. Create the assumed [role](https://console.aws.amazon.com/iamv2/home?#/roles):

   ```shell
   role_arn="$(
       aws iam create-role \
           --role-name="${role_name}" \
           --assume-role-policy-document="{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Principal\": {\"AWS\": \"arn:aws:iam::${GEOSTORE_AWS_ACCOUNT_ID}:root\"}, \"Action\": \"sts:AssumeRole\", \"Condition\": {}}]}" \
       | jq --raw-output .Role.Arn
   )"
   ```

1. Attach the policy to the role:

   ```shell
   aws iam attach-role-policy \
       --role-name="${role_name}" \
       --policy-arn="${policy_arn}"
   ```

1. Allow the assumed role to access your [S3 bucket](https://s3.console.aws.amazon.com/s3/home)
   (**warning:** this will _overwrite_ any existing bucket policy, not add to it):

   ```shell
   aws s3api put-bucket-policy \
       --bucket="${bucket_name}" \
       --policy="{\"Id\": \"Policy$(date +%s)\", \"Version\": \"2012-10-17\", \"Statement\": [{\"Sid\": \"Stmt$(date +%s)\", \"Action\": [\"s3:GetObject\", \"s3:GetObjectAcl\", \"s3:GetObjectTagging\"], \"Effect\": \"Allow\", \"Resource\": \"arn:aws:s3:::${bucket_name}/*\", \"Principal\": {\"AWS\": [\"${role_arn}\"]}}]}"
   ```

1. Log in to any LINZ AWS account:

   ```shell
   aws-azure-login --no-prompt --profile="$AWS_PROFILE"
   ```

1. Assume the API users role:

   ```shell
   aws sts assume-role --role-arn="$(aws iam get-role --role-name=nonprod-api-users | jq --raw-output .Role.Arn)" --role-session-name="$USER"
   ```

1. Create a new dataset:

   ```shell
   dataset_id="$(
       aws lambda invoke \
           --function-name="nonprod-datasets" \
           --payload "{\"http_method\": \"POST\", \"body\": {\"title\": \"test_$(date +%s)\", \"description\": \"Description\"}}" \
           /dev/stdout \
       | jq --raw-output '.body.id // empty'
   )"
   ```

1. Create a dataset version:

   ```shell
   execution_arn="$(
       aws lambda invoke \
           --function-name="nonprod-dataset-versions" \
           --payload "{\"http_method\": \"POST\", \"body\": {\"id\": \"${dataset_id}\", \"metadata_url\": \"${metadata_url}\", \"s3_role_arn\": \"${role_arn}\"}}" \
           /dev/stdout \
       | jq --raw-output '.body.execution_arn // empty'
   )"
   ```

1. Poll for the import to finish:

   ```shell
   aws lambda invoke \
       --function-name="nonprod-import-status" \
       --payload "{\"http_method\": \"GET\", \"body\": {\"execution_arn\": \"${execution_arn}\"}}" \
       /dev/stdout
   ```

To clean up:

```shell
aws iam detach-role-policy --role-name="${role_name}" --policy-arn="${policy_arn}"
aws iam delete-policy --policy-arn="${policy_arn}"
aws iam delete-role --role-name="${role_name}"
```

## Code review

### Typical workflow

1. Author:
   1. Make sure the PR title is in Conventional Commits format.
   1. Double-check that the PR can't be split.
   1. Resolve any _conflicts._
   1. Double-check the PR template to see if any more details can be filled in.
   1. Add the "automerge" label if it's _urgent_ or you're _confident_ it'll pass the CI pipeline.
      This triggers a longer-running production upgrade workflow.
   1. Click "Ready for review" if it's a _draft_ PR.
   1. Add at least one reviewer.
   1. Notify the reviewers separately if it's an _urgent_ fix.
1. Reviewer:
   1. Add comments, typically using the "Start a review" button to group an entire review together.
      Comments should be actionable, either posing a question or suggesting a change. Kudos is
      probably better served by other means (shout-outs FTW!), to keep the review quick and easy to
      read.
   1. Finish the review.
      -  The review comment is optional, and only really needed for overall or non-code comments,
         such as suggesting commit message or PR detail changes.
      -  "Comment" if _all_ your comments are optional, and you'd be OK with another reviewer
         approving unconditionally and merging without addressing any of them.
      -  "Approve" the PR if there are no comments. This will result in an auto-merge if the PR has
         the "automerge" label and the pipeline passes.
      -  "Request changes" if any of your comments definitely require changing the PR contents. This
         means you have to later approve the PR for it to actually merge.
1. Author (if the PR is not approved yet):
   1. Address each comment, either by changing the PR or responding to the reviewer. Beware that
      GitHub will _hide_ parts of long conversations, and you might have to press a link repeatedly
      to see all comments.
   1. Re-request review, to notify the reviewers that you've finished addressing their comments.
1. Reviewer:
   1. Resolve conversations which don't need any further action. If a comment has not been fully
      addressed yet, you might need to point out why. For example, a comment might apply in several
      places, and only some of them have been resolved.
   1. Continue as above.

### Checklist

This document is meant to give general hints for code reviewers. It should not be considered a
complete set of things to consider, and should not include anything which is currently being
validated automatically.

#### Time zones

-  Only use time zone-aware datetimes.

   Bad examples:
   [`datetime.utcnow()` and `datetime.utcfromtimestamp()` return datetimes without a time zone](https://blog.ganssle.io/articles/2019/11/utcnow.html),
   despite `utc` in the name.

   Good examples: `datetime.now(tz=timezone.utc)` and
   `datetime.fromtimestamp(timestamp, tz=timezone.utc)`.

   Rationale: When doing anything with time zone-naive datetimes they are considered to be in the
   same time zone as the local machine clock, which is not what you'd ever want when programming for
   another system.

#### Dockerfiles

-  Don't pin `apt` package versions.

   [Rationale](https://github.com/linz/geostore/pull/171#discussion_r553474554):

   -  Distributions should only get patch-level upgrades, making breakage unlikely.
   -  Manually upgrading these packages in a timely fashion is a fair bit of overhead.
   -  To be on the safe side we'd have to lock the versions of the dependencies in addition to the
      top-level packages, which is even more overhead. Apt AFAIK does not have a lockfile mechanism
      like NPM and Poetry.
   -  [Dependabot does not yet patch Dockerfile package installs](https://github.com/dependabot/dependabot-core/issues/2129).
      This would fix all of the issues above, probably flipping this recommendation.

#### Testing

-  Make sure the test name says what the code should do, not what it should _not_ do.

   Bad example: `should not [action] when [state]`.

   Rationale: There are an infinite number of ways not to do the action. Should it skip the action,
   throw an exception, change the state or it put the action into a queue for reprocessing? Stating
   positively what the code should do makes it easier to compare the test name to its implementation
   to judge whether the action is appropriate and that the name is accurate.

-  Use precise action wording.

   Precise examples:

   -  `should return HTTP 200 when creating item`
   -  `should log success message when creating item`
   -  `should return ID when creating item`

   Vague examples:

   -  `should fail when [state]` is not helpful, because there are an infinite number of ways in
      which code could "fail". A better name says something about how the code _handles_ the
      failure, such as `should return error message when …` or `should throw FooError when …`.
   -  `should succeed when [state]` has the same issue, even though there are typically only a few
      application-specific (for example, HTTP 200 response) or language-specific (for example,
      returning without throwing an exception) ways the code could reasonably be said to "succeed".
      This also often ends up hiding the fact that more than one thing indicates success, and each
      of them should probably be tested in isolation (for example, the precise examples above, each
      of which are side effects of the same action).

   Rationale: Precise names help with verifying that the test does what it says it should do, makes
   it easier to search through the tests for similar ones to use as a template for a new one, and
   makes it faster to understand what's gone wrong when an old test fails in CI.

-  To reproduce pipeline non-infrastructure test runs, make sure to
   `unset AWS_DEFAULT_REGION AWS_PROFILE` and `mv ~/.aws{,.orig}` (undo with `mv ~/.aws{.orig,}`)
   first.

#### AWS Lambda

-  To speed up our lambdas, boto3 clients and other large pieces of setup should be initialised
   outside the main lambda handler. See
   [Best practices for working with AWS Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)
   for more tips.
-  Lambda runtime has been configured to 60 seconds. Higher than the minimum runtime of 3 seconds
   and lower than maximum of 15 minutes. This can be increased if a Lambda can be demonstrated to
   require more runtime than 60 seconds.
-  Only code which is used by multiple Lambda jobs should be in the top-level `geostore` directory.
   Rationale: Different subsets of the code are deployed to each job, so every piece of code in the
   top-level Python files adds a bit more risk of production failure. Counter-example: Some things
   belong together, even if only a subset of them are used in multiple Lambda jobs. For example,
   constants for standard STAC keys and values.

#### Imports

-  Imports within a top-level directory should use relative imports. Rationale:
   -  Lambda jobs are deployed without the top-level `geostore` directory, so any attempt to
      `from geostore[…]` or `import geostore[…]` is going to fail in AWS.
   -  For consistency, we should do the same as the above elsewhere.
   -  Relative imports are trivially distinguished from imports from third party packages, and are
      grouped accordingly by `isort`.

#### Command-line interfaces

-  Mandatory parameters should be named. This way commands are self-documenting, and users don't
   have to remember the exact sequence.

   Bad example: `geostore credentials create 1000 'Jane Doe'`

   Good example: `geostore credentials create --account-id=1000 --comment='Jane Doe'`

   In [Typer](https://typer.tiangolo.com/) you can use a default value of literally `Option(...)` to
   make a mandatory parameter an option.

-  Progress and error messages should be printed to standard error.
-  Output should be scriptable by default. That means single values should be printed without any
   decoration such as quotes or a title, table data should be printed as tab-separated lines, and
   object data should be printed as JSON.
-  Output may be human-readable when the receiver is a terminal rather than another program. This
   sacrifices some programming ease (because the terminal output is no longer identical to the piped
   output) to enable more user-friendly formatting such as titles for single values, table headings
   and aligned table columns, and pretty-printed JSON, any of which could be coloured.

#### Documentation

-  We should try to use portable
   [Markdown syntax](https://daringfireball.net/projects/markdown/syntax). This isn't always easy -
   even mainstream renderers like GitHub, Stack Overflow, and JetBrains IDEs disagree about subtle
   details, which are usually only discoverable by trial and error. Rationale: As an open source
   project, we should focus on portability where it's cheap, and only add non-portable features when
   it's really valuable. Basically we need to balance readability and ease of editing across
   different platforms.

   Bad example: ASCII art is generally neither self-documenting nor reproducible. That is, if you
   wanted to change a single character in a non-trivial ASCII art diagram you have several problems.
   Which ASCII art editor was used? Since it's not self-documenting there's no way to know unless
   you use precious documentation space for this meta-documentation. And what was the source of the
   ASCII art? Many of them let you create a diagram which is then exported to ASCII art, but going
   the other way is generally not possible. Bit rot is also a concern - what if the editor has
   changed fundamentally or no longer exists? And finally, a lot of editors can't be trivially
   automated, so any kind of updating might require manual work every time.

   Good example: A [DOT file](https://graphviz.org/doc/info/lang.html) is a relatively simple
   format, can be converted into various image formats, and the conversion is trivial to automate.
   Including the result of such a conversion in Markdown is also trivial.

#### Code reuse

-  Group code by relatedness, not by reuse. Basically, putting a bunch of stuff in a single file
   should be justified by more than "they are all being used in more than one place". Putting all
   the utility code in one place creates clutter, makes it harder to find reusable pieces, and makes
   it impossible to create minimal sets of code to deploy to different services.

   Counter-example: conftest.py is a pytest-specific file for fixtures. We don't want to change our
   configuration simply to allow splitting this up.

#### Dependabot

-  When Dependabot updates any Python dependencies in pip/requirements.txt formatted files, make
   sure to run `./generate-requirements-files.bash` with the relevant path to update the version of
   all its dependencies.
