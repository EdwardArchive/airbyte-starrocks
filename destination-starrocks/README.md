# Destination StarRocks (Kotlin, Bulk-Load CDK)

StarRocks destination connector, rewritten on Airbyte's **Bulk-Load CDK** (dataflow `Aggregate`)
in Kotlin. See epic [#6](https://github.com/EdwardArchive/airbyte-starrocks/issues/6) and
`STARROCKS-VERSION-COMPATIBILITY.md` for the StarRocks version feature gating.

- **Data plane:** HTTP Stream Load (`http_port`, default 8030).
- **Control plane:** MySQL protocol (`port`, default 9030) for DDL and `SELECT current_version()`.

## Requirements

- **JDK 21** (the CDK and connector are compiled/run for Java 21). Build with `JAVA_HOME` pointing
  at a JDK 21 install, e.g.:
  ```
  JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :destination-starrocks:run --args="--spec"
  ```

## Connector CLI

The CDK takes the operation as a long flag (`--spec`, `--check`, `--discover`, `--write`), e.g.:

```
./gradlew :destination-starrocks:run --args="--spec"
./gradlew :destination-starrocks:run --args="--check --config secrets/config.json"
```

## Build / test

```
./gradlew :destination-starrocks:build      # compile + tests
./gradlew :destination-starrocks:test       # unit tests
```

Integration tests (Testcontainers StarRocks) and the acceptance suite are added in issue #13.
