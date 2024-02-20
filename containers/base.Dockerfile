FROM 652969937640.dkr.ecr.us-east-1.amazonaws.com/containers/ubuntu:20-04-lts

WORKDIR /app
RUN apt-get update && apt-get install -y build-essential curl git maven zip

# Find the JDK version used by the current Databricks runtime:
# 1. Look for "sparkVersion" in pynest, e.g. "12.2.x-scala2.12"
# 2. Find its release notes at https://docs.databricks.com/release-notes/runtime/releases.html
# 3. Search for "zulu", which states the JDK version, e.g. "8.68.0.21-CA-linux64"
# 4. Copy link address from https://www.azul.com/downloads/?version=java-8-lts&os=linux&architecture=x86-64-bit&package=jdk&show-old-builds=true#zulu
# 5. Checkum the downloaded file: shasum -a 256 /path/to/file
RUN curl https://cdn.azul.com/zulu/bin/zulu8.68.0.21-ca-jdk8.0.362-linux_amd64.deb > /tmp/jdk.deb \
    && export JDK_DEB_CHECKSUM=4893b9ac353d2ed7619e7bb2c33fd12189ea771c7fc3243789db90fc0e9719d5 \
    && echo "${JDK_DEB_CHECKSUM} /tmp/jdk.deb" | sha256sum -c \
    && apt-get install -y -f /tmp/jdk.deb

ENV JAVA_HOME=/usr/lib/jvm/zulu-8-amd64
RUN java -version
