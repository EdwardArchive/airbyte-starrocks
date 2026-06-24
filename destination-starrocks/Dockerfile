# NOTE: Docker packaging is not yet verified in a docker environment (issue #7 follow-up).
# The CDK build distribution is produced by the `application` plugin via `:destination-starrocks:distTar`.
FROM airbyte/java-connector-base:2.0.1

WORKDIR /airbyte
ENV APPLICATION=destination-starrocks

COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar
RUN tar xf ${APPLICATION}.tar --strip-components=1 && rm -rf ${APPLICATION}.tar

LABEL io.airbyte.version=2.0.0
LABEL io.airbyte.name=airbyte/destination-starrocks
