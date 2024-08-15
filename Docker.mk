## Docker registry for local builds.
DOCKER_REGISTRY ?= localhost:${REGISTRY_LOCAL_PORT}

## Docker image path.
DOCKER_REPOSITORY ?= ${USER}

## Docker image suffix.
DOCKER_IMAGE_SUFFIX ?= -local

## Target docker image tag, default {branch}-{date}-{commit}.
DOCKER_IMAGE_TAG ?= $(shell git branch --show-current | tr / -)-$(shell git show -s --pretty=%cs-%H)${DOCKER_IMAGE_SUFFIX}

DOCKER_OVERRIDE_BASE_REPOSITORY ?=
DOCKER_OVERRIDE_BASE_IMAGE ?=

ifeq (${YAPACKAGE_MODE}, push)
  YAPACKAGE_FLAGS += --docker-push
endif
ifneq (${DOCKER_REGISTRY},)
  YAPACKAGE_FLAGS += --docker-registry ${DOCKER_REGISTRY}
endif
ifneq (${DOCKER_REPOSITORY},)
  YAPACKAGE_FLAGS += --docker-repository ${DOCKER_REPOSITORY}
endif
ifneq (${DOCKER_IMAGE_TAG},)
  YAPACKAGE_FLAGS += --custom-version ${DOCKER_IMAGE_TAG}
endif

ifneq (${DOCKER_OVERRIDE_BASE_REPOSITORY},)
  DOCKER_OVERRIDE_FLAGS += --docker-build-arg BASE_REPOSITORY=${DOCKER_OVERRIDE_BASE_REPOSITORY}
endif
ifneq (${DOCKER_OVERRIDE_BASE_IMAGE},)
  DOCKER_OVERRIDE_FLAGS += --docker-build-arg BASE_IMAGE=${DOCKER_OVERRIDE_BASE_IMAGE}
endif

##@ Docker:

docker-ytsaurus: ## Build release docker image.
	$(YATOOL) package ${YAPACKAGE_FLAGS} yt/docker/ya-build/ytsaurus/package.json
	@cat packages.json

docker-ytsaurus-override: ## Override ytsaurus server in docker image.
	$(YATOOL) package ${YAPACKAGE_FLAGS} ${DOCKER_OVERRIDE_FLAGS} yt/docker/ya-build/ytsaurus-server-override/package.json
	@cat packages.json

# /usr/include/pythonX/pyconfig.h cannot include ARCH/pythonX/pyconfig.h without -I/usr/include
# Add symlink /usr/include/pythonX/ARCH/pythonX -> /usr/include/ARCH/pythonX
hack-local-python: I=$(shell python3-config --includes | sed -n 's/^-I\(\S\+\) .*/\1/p')
hack-local-python: A=$(shell dpkg-architecture -q DEB_BUILD_MULTIARCH)
hack-local-python: ## Fix for USE_LOCAL_PYTHON in multiarch distro for docker build.
	if [ ! -e ${I}/${A} ]; then sudo mkdir -p ${I}/${A} && sudo ln -s ../../${A}/$(notdir ${I}) ${I}/${A}; fi

# https://distribution.github.io/distribution/about/configuration/

## Port for local docker registry.
REGISTRY_LOCAL_PORT ?= 5000
REGISTRY_LOCAL_NAME ?= ${USER}-registry-localhost-${REGISTRY_LOCAL_PORT}

docker-run-local-registry: ## Run local docker registry.
	docker run --name ${REGISTRY_LOCAL_NAME} -d --restart=always \
		--mount type=volume,src=${REGISTRY_LOCAL_NAME},dst=/var/lib/registry \
		-p "127.0.0.1:${REGISTRY_LOCAL_PORT}:5000" \
		registry:2

docker-rm-local-registry: ## Remove local docker registry.
	docker rm -f ${REGISTRY_LOCAL_NAME}
	docker volume rm -f ${REGISTRY_LOCAL_NAME}

clean-docker:
	docker container prune --force
	docker volume prune --force --all
	docker image prune --force --all
	docker builder prune --force --all
	docker system prune --force --all

export CONTAINERD_NAMESPACE=k8s.io
clean-containerd-containers:
	ctr task ls -q | xargs -r ctr task rm -f
	ctr container ls -q | xargs -r ctr container rm

clean-containerd-images:
	ctr image prune --all
