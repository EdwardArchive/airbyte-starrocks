# StarRocks

This page guides you through setting up the StarRocks destination connector.

[StarRocks](https://www.starrocks.io/) is a high-performance analytical database. This
connector writes Airbyte records to StarRocks tables using HTTP **Stream Load** for the data
plane and the MySQL protocol for the control plane (DDL and version detection). It supports
full-refresh and incremental syncs, including Change Data Capture (CDC).

This is an **open-source, custom-image** connector: you build it as a Docker image, push it to a
registry your Airbyte deployment can pull from, and register it as a custom destination. It is not
available on Airbyte Cloud.

## Features

| Feature                        | Supported | Notes                                                      |
| ------------------------------ | :-------: | ---------------------------------------------------------- |
| Full Refresh - Overwrite       |     O     | DUPLICATE KEY table, swapped in atomically                 |
| Full Refresh - Append          |     O     | DUPLICATE KEY table                                        |
| Incremental - Append           |     O     | DUPLICATE KEY table                                        |
| Incremental - Append + Dedup   |     O     | PRIMARY KEY table, load-time upsert via `__op`             |
| Change Data Capture (CDC)      |     O     | Hard or soft delete (see CDC deletion mode)                |
| Namespaces                     |     O     | Stream namespace maps to a StarRocks database              |
| Replicate Incremental Deletes  |     O     | Via CDC soft delete (`_ab_cdc_deleted_at`)                 |
| SSL/TLS connection             |     O     | Control plane (MySQL protocol) only; Stream Load stays HTTP |
| Airbyte Cloud                  |     X     | OSS custom-image connector only                            |

## Prerequisites

- A running StarRocks cluster, version **3.3.x or newer** (the connector rejects older clusters at
  connection check; the tested ceiling is **4.1.x**).
- Network access from the Airbyte worker to **both** ports:
  - the **query port** (MySQL protocol, default `9030`) for DDL and `SELECT current_version()`, and
  - the **HTTP port** (FE HTTP, default `8030`) for Stream Load.
- A StarRocks user with privileges to create databases/tables and to INSERT (Stream Load) into the
  target database.
- A Docker registry reachable by your Airbyte deployment, since this connector is registered as a
  custom image.

## Setup guide

### Step 1: Prepare the StarRocks user and database

Connect to StarRocks over the MySQL protocol (port `9030`) and ensure the sync user can create
objects and load data. For example:

```sql
CREATE USER 'airbyte'@'%' IDENTIFIED BY '{password}';
GRANT CREATE DATABASE ON *.* TO 'airbyte'@'%';
GRANT ALL ON DATABASE {database} TO 'airbyte'@'%';
```

The connector creates the target database (`CREATE DATABASE IF NOT EXISTS`) and the per-stream
tables itself, so the user mainly needs create and load (INSERT) privileges on the target database.
If you leave the **Database** field blank, the connector uses a database named `default`.

### Step 2: Build and push the connector image

From the repository root, build the destination Docker image and push it to your registry:

```bash
# tag should match metadata.yaml: airbyte/destination-starrocks:2.0.22
docker build -t {your-registry}/destination-starrocks:2.0.22 destination-starrocks
docker push {your-registry}/destination-starrocks:2.0.22
```

### Step 3: Register the custom destination in Airbyte

In the Airbyte UI, go to **Settings -> Destinations -> + New connector** (custom connector) and
provide:

- **Docker repository:** `{your-registry}/destination-starrocks`
- **Docker image tag:** `2.0.22`
- **Connector definition ID:** `5c4d966a-19ff-45d8-9687-876ad0f5d0d9`

> Note: a raw custom-image registration does not read `metadata.yaml`. Because this is a Bulk-Load
> CDK destination, the platform must have `supports_refreshes` enabled for the actor definition,
> otherwise the CDK fails before the first write with a missing `generationId`. Set this flag in the
> platform `actor_definition_version` for the custom destination.

### Step 4: Configure the connection

Create a new destination of type **StarRocks** and fill in the connection parameters below. The
connection **Check** validates both planes end to end: it connects over the MySQL protocol, reads
`current_version()`, applies version gating, and then round-trips one row through Stream Load
(create throwaway table, load one row, drop it). A passing check therefore implies a working sync.

### Step 5 (optional): Enable SSL/TLS

SSL applies to the **control plane only** (the MySQL/JDBC connection on port `9030`). Set **SSL** to
true and pick an **SSL Mode**:

- `required` — encrypt the connection but do not verify the server certificate (use for self-signed
  certificates).
- `verify_ca` / `verify_identity` — verify the certificate against the JVM trust store.

The Stream Load data plane (HTTP port `8030`) stays plain HTTP even on TLS-enabled clusters, because
StarRocks' FE HTTP port is not TLS.

## Connection parameters

| Parameter           | Required | Default       | Description                                                                                              |
| ------------------- | :------: | ------------- | -------------------------------------------------------------------------------------------------------- |
| `host`              |   Yes    | —             | FE host of the StarRocks cluster.                                                                         |
| `port`              |   Yes    | `9030`        | MySQL-protocol port for queries/DDL and version detection.                                                |
| `http_port`         |   Yes    | `8030`        | FE HTTP port for Stream Load (data plane).                                                                |
| `username`          |   Yes    | `root`        | Username to access the database.                                                                          |
| `password`          |    No    | —             | Password for the username (stored as a secret).                                                          |
| `database`          |   Yes    | `default`     | Target database. Blank falls back to `default`.                                                           |
| `ssl`               |    No    | `false`       | Use SSL for the MySQL-protocol (control-plane) connection. Does not affect Stream Load.                   |
| `ssl_mode`          |    No    | `required`    | Certificate verification when SSL is on: `required`, `verify_ca`, or `verify_identity`.                   |
| `enable_json`       |    No    | `false`       | Store object fields as the StarRocks `JSON` type instead of a JSON-encoded `STRING`.                     |
| `cdc_deletion_mode` |    No    | `Hard delete` | CDC delete handling for deduped streams: `Hard delete` or `Soft delete`.                                  |
| `load_format`       |    No    | `CSV`         | Stream Load body format: `CSV` (no options) or `JSON` (with a nested `compression` option).               |
| `compression`       |    No    | `none`        | Under the `JSON` load format only: `none`, `gzip`, or `zstd` for the Stream Load request body.            |

## Supported sync modes

| Sync mode                       | Supported | Table model                                   |
| ------------------------------- | :-------: | --------------------------------------------- |
| Full Refresh - Overwrite        |     O     | DUPLICATE KEY                                 |
| Full Refresh - Append           |     O     | DUPLICATE KEY                                 |
| Incremental - Append            |     O     | DUPLICATE KEY                                 |
| Incremental - Append + Dedup    |     O     | PRIMARY KEY                                   |

- **Append / Overwrite** streams write to **DUPLICATE KEY** tables keyed on the per-record unique
  `_airbyte_raw_id`. A DUPLICATE key is not a uniqueness constraint, so append never collapses
  business-duplicate records.
- **Append + Dedup** (and CDC) streams write to **PRIMARY KEY** tables. Deduplication happens at
  load time: each row carries a trailing `__op` column (`0` = upsert, `1` = delete) that StarRocks
  applies against the primary key. PRIMARY KEY columns are forced `NOT NULL` and are immutable
  (never altered) after the table is created.

### Namespaces

A stream's namespace maps to a StarRocks database. Streams without a namespace use the configured
**Database**.

## Change Data Capture (CDC)

For CDC sources synced with Append + Dedup, the **CDC Deletion Mode** controls how deletes land:

- **Hard delete** (default): the row is removed from the destination (`__op=1`).
- **Soft delete**: the row is kept and marked via the `_ab_cdc_deleted_at` column (loaded as
  `__op=0`). Query live rows with `WHERE _ab_cdc_deleted_at IS NULL`.

> Note: `__op` is a reserved column name in StarRocks 3.3.6+. If a source stream already has an
> `__op` column it must be renamed upstream to avoid a collision.

## Load format and compression

The **Load Format** parameter is a choice between two Stream Load body formats:

- **CSV** (default): compact and high-throughput. Caveats:
  - A literal `\N` string is stored as `NULL` (it is StarRocks' CSV null marker, even inside an
    enclosure).
  - Very large `BIGINT` / `DECIMAL` values can lose precision.
- **JSON**: avoids the CSV escaping edge cases above, and preserves full integer/decimal precision
  by emitting numbers as JSON strings (a JSON number would be parsed as a double and overflow).
  JSON additionally supports request-body **compression**:
  - `none`, `gzip`, or `zstd` (`zstd` compresses better at similar or lower CPU cost).
  - Compression requires a cluster **>= 3.3.2** and is honored by StarRocks for JSON Stream Load
    only. If compression is requested on an older cluster, the connection check fails.

Stream Load batches use a content-addressed label (a hash of the batch body), so retrying the same
batch is idempotent: StarRocks rejects the re-load as "Label Already Exists", which the connector
treats as success to prevent duplicate rows.

## Output schema

### Airbyte metadata columns

Every table includes these four Airbyte metadata columns (all `NOT NULL`):

| Column                    | StarRocks type |
| ------------------------- | -------------- |
| `_airbyte_raw_id`         | STRING         |
| `_airbyte_extracted_at`   | DATETIME       |
| `_airbyte_meta`           | STRING         |
| `_airbyte_generation_id`  | BIGINT         |

### Type mapping

Airbyte schema types map to StarRocks SQL column types as follows:

| Airbyte type                       | StarRocks type      |
| ---------------------------------- | ------------------- |
| Boolean                            | BOOLEAN             |
| Integer                            | BIGINT              |
| Number                             | DECIMAL(38, 9)      |
| Date                               | DATE                |
| Timestamp (with/without timezone)  | DATETIME            |
| Time (with/without timezone)       | STRING              |
| String                             | STRING              |
| Array                              | STRING              |
| Union                              | STRING              |
| Object                             | JSON or STRING      |

Notes:

- **Object** fields map to the StarRocks `JSON` type when **Enable JSON** is on, otherwise to a
  JSON-encoded `STRING`. Enable JSON for sources with rich JSON columns (for example Postgres
  `jsonb`).
- Timestamps are normalized to UTC and written in `yyyy-MM-dd HH:mm:ss` format (no `T`, no offset).

### Out-of-range value handling

Values that fall outside the range of their target StarRocks column are set to `NULL` and recorded
in `_airbyte_meta` with a `DESTINATION_FIELD_SIZE_LIMITATION` change, rather than failing the load.
This applies to:

| Target column   | Accepted range                                  |
| --------------- | ----------------------------------------------- |
| BIGINT          | signed 64-bit (the Long range)                  |
| DECIMAL(38, 9)  | absolute value < 10^29 (29 integer digits)      |
| DATE            | 0000-01-01 .. 9999-12-31                        |
| DATETIME        | 0000-01-01 00:00:00 .. 9999-12-31 23:59:59      |

## StarRocks version compatibility

The connector detects the cluster version with `SELECT current_version()` (at connection check and
again at write start) and gates behavior on it:

- **Floor:** shared-data **3.3.x**. Older clusters are rejected at the connection check.
- **Ceiling (tested):** **4.1.x**.
- **JSON request-body compression:** requires **>= 3.3.2**.

Higher versions improve throughput and operational efficiency (for example Merge Commit at 3.4+ and
PK-load parallelism at 4.1) rather than unlocking new connector features. See
`STARROCKS-VERSION-COMPATIBILITY.md` in the repository root for the full feature matrix.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request                                            | Subject                                                          |
| ------- | ---------- | ------------------------------------------------------- | ---------------------------------------------------------------- |
| 2.0.22  | 2026-06-26 | [#66](https://github.com/EdwardArchive/airbyte-starrocks/pull/66) | Model `load_format` as a oneOf so CSV cannot select compression  |
|         |            | [#65](https://github.com/EdwardArchive/airbyte-starrocks/pull/65) | Add zstd Stream Load compression alongside gzip                  |
|         |            | [#64](https://github.com/EdwardArchive/airbyte-starrocks/pull/64) | Version-adaptive Stream Load (floor 3.3, gzip JSON compression)  |
|         |            | [#63](https://github.com/EdwardArchive/airbyte-starrocks/pull/63) | Nullify out-of-range values via StarrocksValueCoercer            |
|         |            | [#62](https://github.com/EdwardArchive/airbyte-starrocks/pull/62) | `ssl_mode` option for JDBC certificate verification              |
|         |            | [#61](https://github.com/EdwardArchive/airbyte-starrocks/pull/61) | Add `load_format` toggle (CSV default / JSON)                    |

</details>
