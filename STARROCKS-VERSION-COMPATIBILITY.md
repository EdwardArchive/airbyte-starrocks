# StarRocks version compatibility — destination-starrocks (Stream Load / Primary Key / CDC)

> This document covers the StarRocks version gating of the **Kotlin Bulk-Load CDK connector** (this
> repo's `main`). The gating is enforced by `StarrocksVersionGate` and validated at `check`. The
> setup guide (`docs/integrations/destinations/starrocks.md`) references this matrix.
>
> **Assumption:** the target cluster runs in **shared-data mode** (FE + CN, object storage). Feature
> gating therefore follows the **shared-data support matrix**, not the general primary-key docs —
> some PK features landed later in shared-data.

## 0. Summary

- The connector's foundation (**PRIMARY KEY tables + `__op` upsert/delete + `merge_condition`**) is
  **fully available on shared-data 3.3.11+**. Treat that as the **baseline**.
- Higher versions improve **throughput / operational convenience rather than adding features**
  (3.4 Merge Commit → 3.5 automatic mode detection → 4.1 parallel PK load).
- Instead of trusting flags blindly, the connector **detects the version**
  (`SELECT current_version()`), **fails fast / warns** on unsupported combinations, and aligns its
  behavior using this document.

## 1. Version detection

The connector already connects to **9030 (MySQL protocol)** for DDL. On that same connection:

```sql
SELECT current_version();   -- e.g. "3.3.11", "3.5.4", or "4.1.1"
```

It reads the version at `check` / startup and compares it against the matrix below.

- If an unsupported combination is enabled, **abort with a clear error**
  (e.g. `merge_condition requires shared-data >= 3.1; column-mode partial + condition requires >= 3.3.11; detected 3.3.5`).
- If a feature is below the shared-data bar, **disable it and log a warning**.

> Why: version mismatches surface at runtime as cryptic Stream Load 4xx errors that are hard to
> debug. Validating once over 9030 before loading is well worth the cost.

## 2. Version compatibility matrix (shared-data)

Legend: ✅ available · ⚠️ conditional / caution · ❌ unsupported (needs a higher version)

| Feature (connector use) | shared-data since | **3.3.11** | **3.5.x** | **4.1.1** |
|---|---|:---:|:---:|:---:|
| PRIMARY KEY tables (#3) | v3.1.0 | ✅ | ✅ | ✅ |
| `__op` upsert/delete (#4) | v3.1.0 (with PK) | ✅ | ✅ | ✅ |
| `merge_condition` full-upsert (#5 order-safety) | v3.1.0 (**Greater(≥) only**) | ✅ | ✅ | ✅ |
| partial update — row mode | v3.1.0 | ✅ | ✅ | ✅ |
| partial update — column mode | v3.3.1 | ✅ | ✅ | ✅ |
| column-mode partial **+ conditional combo** | v3.3.11 | ✅ | ✅ | ✅ (most robust) |
| request-body compression (gzip/lz4_frame/zstd, etc.) | v3.3.2 | ✅ | ✅ | ✅ |
| persistent index = `CLOUD_NATIVE` (object store) | v3.3.2 / default v3.3.8 | ⚠️ ≥3.3.8 recommended | ✅ | ✅ |
| **Merge Commit** (merge concurrent small loads) | v3.4.0 | ❌ | ✅ | ✅ |
| partial-update **automatic mode detection** | v3.5.8 / 4.0.2 | ❌ (manual) | ✅ ≥3.5.8 | ✅ |
| **parallel PK load** (publish / index / conditional) | v4.1.0 | ❌ | ❌ | ✅ |

### One line per version

- **3.3.11 (baseline):** the full connector feature set *works*, but without Merge Commit / auto-mode
  / parallel load it is the least efficient for CDC loads. The lower bound when 3.3 is already
  installed.
- **3.5.x (current stable, recommended):** all of 3.3 + **Merge Commit** (eases version pressure from
  small batches) + **automatic mode detection**. The stability/efficiency sweet spot.
  ⚠️ **JDK 17+ required from 3.5.0**.
- **4.1.1 (latest):** + **parallel PK load** for the highest throughput ceiling, and the most robust
  column-mode + conditional. Bleeding edge.

## 3. Caveats that need "logic", not just a flag (all versions)

Constraints to honor regardless of version — adding a spec field is not enough.

1. **`merge_condition` supports only the 'Greater (≥)' operator.**
   The ordering column for #5 (`dedup_ordering_column`) must therefore be **monotonically
   increasing**. A monotonic sequence derived from the CDC binlog position
   (`_ab_cdc_log_file` + `_ab_cdc_log_pos`) is ideal. A plain `updated_at` timestamp can break on
   ties / clock skew, so it is subject to **validation / warning**.

2. **`merge_condition` does not apply to DELETE (`__op=1`)**, plus there are restrictions on mixing
   condition columns within one batch. Load **conditional upserts and deletes in separate batches**;
   otherwise an out-of-order delete can remove a row that was later re-inserted.

3. **`__op` is a reserved column name in v3.3.6+.** If a source already has an `__op` column it
   collides and must be renamed upstream.

## 4. Recommended targets

- **Stability first:** **3.5.x** — full feature set + Merge Commit + auto mode, production-stable.
- **Throughput first:** **4.1.1** — parallel PK load + the most robust column-mode + conditional.
- **Lower bound:** **3.3.11** — below this the column-mode + conditional combo is incomplete, so it is
  not supported.

## 5. Operational notes

- **JDK 17+**: required when upgrading clusters to 3.5.0+ (a cluster-side requirement).
- **persistent index default**: shared-data PK on 3.3.8+ defaults to `CLOUD_NATIVE`.
- **Merge Commit** (3.4+): enabled via headers such as `enable_merge_commit`. It reduces version
  explosion / compaction pressure under Airbyte's frequent small batches. Not available on 3.3.x, so
  substitute with batch size / frequency tuning there.

## 6. Sources

- Stream Load SQL reference — https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/STREAM_LOAD/
- Load to Primary Key tables — https://docs.starrocks.io/docs/loading/Load_to_Primary_Key_tables/
- Feature Support: Shared-data Clusters — https://docs.starrocks.io/docs/deployment/shared_data/feature-support-shared-data/
- Feature Support: Loading/Unloading — https://docs.starrocks.io/docs/loading/loading_introduction/feature-support-loading-and-unloading/
- Release notes — 3.3 / 3.4 / 3.5 / 4.0 / 4.1 (docs.starrocks.io/releasenotes/)
- Related PRs: column-mode partial in shared-data (#46516), shared-data conditional partial (#56132),
  CLOUD_NATIVE default (#52209), `__op` reserved name (#52621), 4.1 Lake column-mode conditional (#71961)
