.PHONY: build
build:
	@echo "--- build"
	mvn clean package

.PHONY: test
test:
	@echo "--- test"
	mvn clean test

.PHONY: release
release:
	@echo "--- release"
	mvn -D revision=$(REVISION) clean package

.PHONY: docker-base
docker-base:
	@echo "--- docker-base"
	docker build \
	    --tag data/flight-spark-source/base \
	    --file containers/base.Dockerfile \
	    --platform linux/amd64 \
	    --progress plain \
	    .

.PHONY: docker-latest
docker-latest:
	@echo "--- docker-latest"
	docker build \
	    --tag data/flight-spark-source/latest \
	    --file containers/latest.Dockerfile \
	    --platform linux/amd64 \
	    --progress plain \
	    --build-arg BUILDKITE_BRANCH=${BUILDKITE_BRANCH} \
	    .

.PHONY: docker-stable
docker-stable:
	@echo "--- docker-stable"
	docker build \
	    --tag data/flight-spark-source/stable \
	    --file containers/stable.Dockerfile \
	    --platform linux/amd64 \
	    --progress plain \
	    --build-arg BUILDKITE_BRANCH=${BUILDKITE_BRANCH} \
	    .
