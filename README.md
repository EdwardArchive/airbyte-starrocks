# Airbyte StarRocks Destination

An [Airbyte](https://airbyte.com/) **destination connector** for
[StarRocks](https://www.starrocks.io/), rewritten in **Kotlin** on Airbyte's **Bulk-Load CDK**
(dataflow `Aggregate`). It writes Airbyte records to StarRocks tables using HTTP **Stream Load** for
the data plane and the **MySQL protocol** for the control plane (DDL and version detection), and
supports full-refresh, incremental, and Change Data Capture (CDC) syncs.

> **Status:** `alpha` · community support · **OSS custom-image** connector — you build the Docker
> image and register it as a custom destination. Not available on Airbyte Cloud.

## How it works

- **Data plane** — HTTP **Stream Load** (`http_port`, default `8030`) for high-throughput bulk load.
- **Control plane** — **MySQL protocol** (`port`, default `9030`) for DDL and
  `SELECT current_version()`.
- **Version-adaptive** — the connector detects the cluster version at `check` and write start and
  gates behavior on it (rejects clusters below the floor, opts into higher-version capabilities).
  See [STARROCKS-VERSION-COMPATIBILITY.md](STARROCKS-VERSION-COMPATIBILITY.md).

## Features

| Capability                          | Supported                                              |
| ----------------------------------- | ------------------------------------------------------ |
| Full Refresh — Overwrite / Append   | O  (DUPLICATE KEY table)                                |
| Incremental — Append                | O  (DUPLICATE KEY table)                                |
| Incremental — Append + Dedup        | O  (PRIMARY KEY table, load-time upsert via `__op`)     |
| Change Data Capture (CDC)           | O  (hard or soft delete)                                |
| Namespaces                          | O  (stream namespace → StarRocks database)              |
| SSL/TLS                             | O  (control plane only; Stream Load stays HTTP)         |
| SSH tunnel (bastion)                | O  (use the `SQL` load method when tunneling)           |
| Load method                         | `Stream Load` (HTTP) or `SQL` (batched JDBC)            |
| Load format                         | `CSV`, or `JSON` with `gzip` / `zstd` compression       |
| Airbyte Cloud                       | X  (OSS custom-image only)                               |

See the [setup & reference guide](docs/integrations/destinations/starrocks.md) for the full feature
matrix, connection parameters, sync-mode guidance, type mapping, and output schema.

## Requirements

- **Build:** JDK **21** (the CDK and connector compile/run for Java 21).
- **Target cluster:** StarRocks **shared-data**, version **3.3.x – 4.1.x** (the connector rejects
  older clusters at the connection check; 4.1.x is the tested ceiling). Higher versions improve
  throughput/operational efficiency rather than unlocking new connector features.
- **Network:** access from the Airbyte worker to **both** the query port (`9030`) and the HTTP port
  (`8030`), or an SSH bastion that can reach them.

## Quick start

Build and run the connector locally with Gradle (point `JAVA_HOME` at a JDK 21 install):

```bash
./gradlew :destination-starrocks:build      # compile + unit tests
./gradlew :destination-starrocks:test       # unit tests only

# The CDK takes the operation as a long flag (--spec, --check, --discover, --write):
./gradlew :destination-starrocks:run --args="--spec"
./gradlew :destination-starrocks:run --args="--check --config secrets/config.json"
```

To deploy it into Airbyte, build the connector image, push it to a registry your Airbyte deployment
can pull from, and register it as a custom destination (definition ID
`5c4d966a-19ff-45d8-9687-876ad0f5d0d9`, current image tag `2.0.23`). The
[setup guide](docs/integrations/destinations/starrocks.md) walks through every step, including the
`supports_refreshes` flag the Bulk-Load CDK requires.

## Documentation

| Document | Description |
| -------- | ----------- |
| [docs/integrations/destinations/starrocks.md](docs/integrations/destinations/starrocks.md) | **Setup & reference guide** — install, connection parameters, sync modes (incl. Cursor vs CDC), CDC, load format, SSH tunnel, type mapping, changelog. |
| [STARROCKS-VERSION-COMPATIBILITY.md](STARROCKS-VERSION-COMPATIBILITY.md) | **Version feature matrix** — what each shared-data version unlocks and the gating rules. |
| [destination-starrocks/README.md](destination-starrocks/README.md) | **Developer notes** — Gradle build/test and the connector CLI. |

## Repository layout

```
destination-starrocks/        # the connector (Kotlin, Bulk-Load CDK)
  src/main/kotlin/...          #   spec, check, write, Stream Load client, version gate, SSH tunnel
  metadata.yaml               #   Airbyte connector metadata (definitionId, image tag)
  Dockerfile
docs/                         # user-facing setup & reference guide
STARROCKS-VERSION-COMPATIBILITY.md
```

## License

Apache License 2.0.
