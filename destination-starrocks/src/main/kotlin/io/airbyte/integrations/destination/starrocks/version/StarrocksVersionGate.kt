/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.version

/**
 * StarRocks (shared-data) version feature gating — see repo `STARROCKS-VERSION-COMPATIBILITY.md`.
 *
 * Detected from `SELECT current_version()` (at `check` and again at write start) and used to (a)
 * reject clusters below the supported floor and (b) opt INTO higher-version Stream Load capabilities
 * up to the 4.1.x ceiling (request-body compression, column-mode partial update, Merge Commit).
 *
 * Floor = shared-data **3.3.x**; the PRIMARY KEY upsert/delete model itself is 3.1.0.
 */
object StarrocksVersionGate {

    data class SemVer(val major: Int, val minor: Int, val patch: Int) : Comparable<SemVer> {
        override fun compareTo(other: SemVer): Int =
            compareValuesBy(this, other, { it.major }, { it.minor }, { it.patch })

        override fun toString(): String = "$major.$minor.$patch"
    }

    // Supported floor: the connector targets shared-data >= 3.3.x. Older clusters are rejected at
    // `check`. (The PK model is 3.1.0, but 3.3 is the deliberate lower bound for the feature set.)
    val MIN_SUPPORTED = SemVer(3, 3, 0)

    // shared-data feature thresholds — opt INTO these when the detected version is high enough.
    val MIN_PK_DEDUP = SemVer(3, 1, 0) // PK tables, __op, merge_condition, row-mode partial update
    val MIN_COLUMN_MODE_PARTIAL = SemVer(3, 3, 1)
    val MIN_COMPRESSION = SemVer(3, 3, 2) // request-body Stream Load compression (JSON format only)
    val MIN_COLUMN_MODE_PARTIAL_WITH_CONDITION = SemVer(3, 3, 11)
    val MIN_MERGE_COMMIT = SemVer(3, 4, 0)

    data class Capabilities(
        val pkDedup: Boolean,
        val columnModePartialUpdate: Boolean,
        val columnModePartialWithCondition: Boolean,
        /** Request-body Stream Load compression (gzip/lz4/zstd). Honored by StarRocks for JSON only. */
        val compression: Boolean,
        val mergeCommit: Boolean,
    )

    private val VERSION_RE = Regex("""(\d+)\.(\d+)\.(\d+)""")

    fun parse(raw: String): SemVer {
        val m =
            VERSION_RE.find(raw)
                ?: throw IllegalArgumentException("Unrecognized StarRocks version: '$raw'")
        return SemVer(m.groupValues[1].toInt(), m.groupValues[2].toInt(), m.groupValues[3].toInt())
    }

    fun capabilities(raw: String): Capabilities {
        val v = parse(raw)
        return Capabilities(
            pkDedup = v >= MIN_PK_DEDUP,
            columnModePartialUpdate = v >= MIN_COLUMN_MODE_PARTIAL,
            columnModePartialWithCondition = v >= MIN_COLUMN_MODE_PARTIAL_WITH_CONDITION,
            compression = v >= MIN_COMPRESSION,
            mergeCommit = v >= MIN_MERGE_COMMIT,
        )
    }

    /** Throws if the cluster is below the supported floor (shared-data >= 3.3.x). */
    fun validate(raw: String) {
        val v = parse(raw)
        if (v < MIN_SUPPORTED) {
            throw IllegalStateException(
                "StarRocks $v is not supported: the connector requires shared-data >= " +
                    "$MIN_SUPPORTED. See STARROCKS-VERSION-COMPATIBILITY.md.",
            )
        }
    }
}
