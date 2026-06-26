FROM airbyte/java-connector-base:2.0.1

ENV APPLICATION=destination-starrocks
WORKDIR /airbyte

# The Gradle `application` plugin distribution: build with `:destination-starrocks:distTar`.
COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar
RUN tar xf ${APPLICATION}.tar --strip-components=1 && rm ${APPLICATION}.tar

# Airbyte's workload launcher execs $AIRBYTE_ENTRYPOINT (/airbyte/base.sh) DIRECTLY and ignores the
# Docker ENTRYPOINT, so base.sh must be executable. (A plain `docker run` uses ENTRYPOINT and masks this.)
RUN chmod +x /airbyte/base.sh

# base.sh maps Airbyte's positional ops (spec/check/discover/write) to these commands, appending
# --config/--catalog/--state. The Bulk-Load CDK expects the operation as a long flag (--spec, etc.).
ENV AIRBYTE_SPEC_CMD="/airbyte/bin/destination-starrocks --spec" \
    AIRBYTE_CHECK_CMD="/airbyte/bin/destination-starrocks --check" \
    AIRBYTE_DISCOVER_CMD="/airbyte/bin/destination-starrocks --discover" \
    AIRBYTE_READ_CMD="/airbyte/bin/destination-starrocks --read" \
    AIRBYTE_WRITE_CMD="/airbyte/bin/destination-starrocks --write"

# Invoke via bash so base.sh does not need the execute bit.
ENTRYPOINT ["/airbyte/base.sh"]

LABEL io.airbyte.version=2.0.22
LABEL io.airbyte.name=airbyte/destination-starrocks
