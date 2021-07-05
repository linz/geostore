from subprocess import PIPE, Popen

with Popen(["git", "rev-parse", "--abbrev-ref", "HEAD"], stdout=PIPE) as branch_command:
    GIT_BRANCH = branch_command.communicate()[0].decode().strip()

with Popen(["git", "rev-parse", "--short", "HEAD"], stdout=PIPE) as commit_command:
    GIT_COMMIT = commit_command.communicate()[0].decode().strip()

with Popen(["git", "describe", "--tags", "--exact-match"], stdout=PIPE) as tag_command:
    GIT_TAG = tag_command.communicate()[0].decode().strip()
if not GIT_TAG:
    GIT_TAG = "UNRELEASED"
