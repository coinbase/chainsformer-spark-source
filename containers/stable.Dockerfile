# The base image was built by https://codeflow.cbhq.net/#/data/flight-spark-source/commits/7a6c617119101193637097214b291a8ffa3e8544
FROM 652969937640.dkr.ecr.us-east-1.amazonaws.com/data/flight-spark-source/base@sha256:3305cb25122329c61784e5c183b6b6b21540c863a5a7a462b3bae655a572819a AS build_stage
WORKDIR /app
RUN mkdir assets
COPY . ./
# This builds a zip file containing:
# 1. flight-spark-source-1.0-YYYY-MM-DD-HASH-shaded.jar
# 2. flight-spark-source-1.0-stable-shaded.jar, only if the active branch is master.
RUN export BRANCH=${BUILDKITE_BRANCH} \
    && if [ -z "${BRANCH}" ]; then \
        export BRANCH=$(git show -s --pretty=%D HEAD | awk -F '[ ,]' '{print $3}'); \
    fi \
    && export REVISION=$(git show --no-patch --no-notes --pretty='%cs-%h' HEAD) \
    && make release REVISION=${REVISION} \
    && export TARGET=flight-spark-source-1.0-${REVISION}-shaded.jar \
    && cp target/${TARGET} assets/${TARGET} \
    && echo -n "HEAD: " && git show -s --pretty=%D HEAD \
    && echo "BRANCH: ${BRANCH}" \
    && echo "REVISION: ${REVISION}" \
    && echo "ASSETS: ${TARGET}" \
    && if [ "${BRANCH}" = "master" ] || [ "${BRANCH}" = "origin/master" ]; then \
        export STABLE_TARGET=flight-spark-source-1.0-stable-shaded.jar \
        && cp target/${TARGET} assets/${STABLE_TARGET} \
        && echo "ASSETS: ${STABLE_TARGET}"; \
    fi \
    && zip -j assets.zip assets/*

FROM scratch AS release_stage
WORKDIR /app
COPY --from=build_stage /app/assets.zip .
