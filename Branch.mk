DOCKER_OVERRIDE_BASE_REPOSITORY ?= ghcr.io/ytsaurus/ytsaurus
DOCKER_OVERRIDE_BASE_IMAGE ?= stable-23.2.1-relwithdebinfo

##@ Releases:

YT_VERSION ?= 23.2

# map current branch upstream "@{u}" to various branches
BRANCH_STABLE_BASE = $(shell git rev-parse --symbolic-full-name @{u} | sed -E 's#(refs/remotes/[^/]+)/([^/]+)/(.*)/stable/([^/]+)(.*)#\1/stable/\4#')
REMOTE_BOOTSTRAP_BRANCH = $(shell git rev-parse --symbolic-full-name @{u} | sed -E 's#(refs/remotes/[^/]+)/([^/]+)/(.*)/stable/([^/]+)(.*)#\1/\2/patches/bootstrap/stable/\4#')
LOCAL_BOOTSTRAP_BRANCH = $(shell git rev-parse --symbolic-full-name @{u} | sed -E 's#(refs/remotes/[^/]+)/([^/]+)/(.*)/stable/([^/]+)(.*)#\2/patches/bootstrap/stable/\4#')
PUBLIC_RELEASE_BRANCH = $(shell git rev-parse --symbolic-full-name @{u} | sed -E 's#(refs/remotes/[^/]+)/([^/]+)/(.*)/stable/([^/]+)(.*)#\2/releases/public/stable/\4#')
PRIVATE_RELEASE_BRANCH = $(shell git rev-parse --symbolic-full-name @{u} | sed -E 's#(refs/remotes/[^/]+)/([^/]+)/(.*)/stable/([^/]+)(.*)#\2/releases/private/stable/\4#')
BRANCH_FRAGMENT_PUBLIC_PREFIXES = $(shell git rev-parse --symbolic-full-name @{u} | sed -E 's#(refs/remotes/[^/]+)/([^/]+)/(.*)/stable/([^/]+)(.*)#\1/\2/patches/public/stable/\4#')
BRANCH_FRAGMENT_PRIVATE_PREFIXES = $(shell git rev-parse --symbolic-full-name @{u} | sed -E 's#(refs/remotes/[^/]+)/([^/]+)/(.*)/stable/([^/]+)(.*)#\1/\2/patches/private/stable/\4#')
BRANCH_PUBLIC_FRAGMENTS=$(shell git for-each-ref --sort="-authordate" "--format=%(refname)" ${BRANCH_FRAGMENT_PUBLIC_PREFIXES})
BRANCH_PRIVATE_FRAGMENTS=$(shell git for-each-ref --sort="-authordate" "--format=%(refname)" ${REMOTE_BOOTSTRAP_BRANCH}  ${BRANCH_FRAGMENT_PRIVATE_PREFIXES})

checkout-bootstrap-branch: ## Checkout bootstrap branch for the current release.
	git diff --quiet
	git diff --quiet --cached
	git checkout ${LOCAL_BOOTSTRAP_BRANCH}

checkout-public-release-branch: ## Checkout public release branch for the current release.
	git diff --quiet
	git diff --quiet --cached
	git checkout ${PUBLIC_RELEASE_BRANCH}

checkout-release-branch: ## Checkout (private) release branch for the current release.
	git diff --quiet
	git diff --quiet --cached
	git checkout ${PRIVATE_RELEASE_BRANCH}

rebuild-public-release-branch: checkout-public-release-branch ## Rebuild public release branch by resetting it to the corresponding stable branch and cherry-picking fragments from mirror-ytsaurus/patches/public/stable/version/*.
	git reset --hard ${BRANCH_STABLE_BASE}
	if [ ! -z "${BRANCH_PUBLIC_FRAGMENTS}" ]; then git cherry-pick -x --keep-redundant-commits ${BRANCH_PUBLIC_FRAGMENTS} --not HEAD; fi

rebuild-private-release-branch: checkout-release-branch ## Rebuild private release branch by resetting it to the current public release branch and cherry-picking fragments from mirror-ytsaurus/patches/private/stable/version/*.
	git reset --hard ${PUBLIC_RELEASE_BRANCH}
	if [ ! -z "${BRANCH_PRIVATE_FRAGMENTS}" ]; then git cherry-pick -x --keep-redundant-commits ${BRANCH_PRIVATE_FRAGMENTS} --not HEAD; fi

rebuild-release-branch: rebuild-public-release-branch rebuild-private-release-branch ## Rebuild release branch by first rebuilding the public release branch on top of the corresponding stable branch and then rebuilding the private branch on top of the resulting public release branch.

push-public-release-branch: ## Push public release branch.
	git push -f origin ${PUBLIC_RELEASE_BRANCH}

push-private-release-branch: TAG_NAME=tracto-${YT_VERSION}-$(shell git show -s --pretty=%cs-%H ${PRIVATE_RELEASE_BRANCH})
push-private-release-branch: ## Push private release branch with corresponding tag.
	git tag -a ${TAG_NAME} -m "YTsaurus tracto ${YT_VERSION} release" ${PRIVATE_RELEASE_BRANCH}
	git push -f origin ${PRIVATE_RELEASE_BRANCH}
	git push origin ${TAG_NAME}

push-release-branches: push-public-release-branch push-private-release-branch ## Push public and private release branches with corresponding tags. Should start CI.

release-image: DOCKER_IMAGE_TAG=tracto-${YT_VERSION}-$(shell git show -s --pretty=%cs-%H)
release-image: DOCKER_OVERRIDE_BASE_REPOSITORY=ghcr.io/ytsaurus/ytsaurus-nightly
# It is best to change this image to the most recent completed 23.2 build every release.
release-image: DOCKER_OVERRIDE_BASE_IMAGE=dev-23.2-2024-07-08-367bed7c0d74d5cec0def1499c02e72890d97ce2-relwithdebinfo
release-image: docker-ytsaurus-override ## Build release docker image and push to nemax registry.
