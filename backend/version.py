import subprocess

GIT_BRANCH = (
    subprocess.Popen(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], shell=False, stdout=subprocess.PIPE
    )
    .communicate()[0]
    .decode()
    .strip()
)
GIT_COMMIT = (
    subprocess.Popen(["git", "rev-parse", "--short", "HEAD"], shell=False, stdout=subprocess.PIPE)
    .communicate()[0]
    .decode()
    .strip()
)

GIT_TAG = (
    subprocess.Popen(
        ["git", "describe", "--tags", "--exact-match"],
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    .communicate()[0]
    .decode()
    .strip()
)
if not GIT_TAG:
    GIT_TAG = "UNRELEASED"
