/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.version

/**
 * StarRocks (shared-data) version feature gating — see repo `STARROCKS-VERSION-COMPATIBILITY.md`.
 *
 * Detected at `check` time from `SELECT current_version()` and used by the load path to decide
 * which Stream Load capabilities to use (column-mode partial update, conditional `merge_condition`,
 * Merge Commit). The hard floor is the PRIMARY KEY upsert/delete model (shared-data >= 3.1.0).
 */
object StarrocksVersionGate {

    data class SemVer(val major: Int, val minor: Int, val patch: Int) : Comparable<SemVer> {
        override fun compareTo(other: SemVer): Int =
            compareValuesBy(this, other, { it.major }, { it.minor }, { it.patch })

        override fun toString(): String = "$major.$minor.$patch"
    }

    // shared-data feature thresholds
    val MIN_PK_DEDUP = SemVer(3, 1, 0) // PK tables, __op, merge_condition, row-mode partial update
    val MIN_COLUMN_MODE_PARTIAL = SemVer(3, 3, 1)
    val MIN_COLUMN_MODE_PARTIAL_WITH_CONDITION = SemVer(3, 3, 11)
    val MIN_MERGE_COMMIT = SemVer(3, 4, 0)

    data class Capabilities(
        val pkDedup: Boolean,
        val columnModePartialUpdate: Boolean,
        val columnModePartialWithCondition: Boolean,
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
            mergeCommit = v >= MIN_MERGE_COMMIT,
        )
    }

    /** Throws if the cluster is too old for the connector's PRIMARY KEY upsert/delete model. */
    fun validate(raw: String) {
        val v = parse(raw)
        if (v < MIN_PK_DEDUP) {
            throw IllegalStateException(
                "StarRocks $v is not supported: PRIMARY KEY upsert/delete (shared-data) requires " +
                    ">= $MIN_PK_DEDUP. See STARROCKS-VERSION-COMPATIBILITY.md.",
            )
        }
    }
}
