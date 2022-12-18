from git import Repo  # type: ignore

repo = Repo(search_parent_directories=True)

try:
    GIT_BRANCH = repo.active_branch.name
except TypeError:
    GIT_BRANCH = repo.head.object.hexsha

GIT_COMMIT = repo.git.rev_parse(repo.head, short=True)
GIT_TAG = next((tag for tag in repo.tags if tag.commit == repo.head.commit), "UNRELEASED")
