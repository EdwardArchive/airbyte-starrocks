/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.client

import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.Overwrite
import io.airbyte.cdk.load.component.ColumnType
import io.airbyte.cdk.load.component.ColumnTypeChange
import io.airbyte.integrations.destination.starrocks.schema.StarrocksSqlTypes
import io.airbyte.integrations.destination.starrocks.sql.KeyModel
import java.sql.SQLException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Guards the append-vs-dedup table model (the concern that an append sync into a PRIMARY KEY table
 * would let StarRocks collapse duplicate-key rows): append/overwrite MUST map to DUPLICATE KEY
 * (no dedup), only dedupe maps to PRIMARY KEY (load-time upsert/delete).
 */
class StarrocksKeyModelTest {

    @Test
    fun `append and overwrite use DUPLICATE KEY so StarRocks does not dedup`() {
        assertEquals(KeyModel.DUPLICATE, keyModelFor(Append))
        assertEquals(KeyModel.DUPLICATE, keyModelFor(Overwrite))
    }

    @Test
    fun `dedupe uses PRIMARY KEY for load-time upsert and delete`() {
        assertEquals(
            KeyModel.PRIMARY,
            keyModelFor(Dedupe(primaryKey = listOf(listOf("id")), cursor = emptyList())),
        )
    }

    @Test
    fun `canonicalStarrocksType normalizes information_schema types to the mapper literals`() {
        // Regression: a 2nd sync compared discovered `bigint` to canonical `BIGINT` and tried to
        // ALTER the PRIMARY KEY columns (which StarRocks rejects). Normalization makes them equal.
        assertEquals(StarrocksSqlTypes.BIGINT, canonicalStarrocksType("bigint"))
        assertEquals(StarrocksSqlTypes.STRING, canonicalStarrocksType("varchar"))
        assertEquals(StarrocksSqlTypes.STRING, canonicalStarrocksType("varchar(1048576)"))
        assertEquals(StarrocksSqlTypes.DECIMAL, canonicalStarrocksType("decimal"))
        assertEquals(StarrocksSqlTypes.DECIMAL, canonicalStarrocksType("decimal(38,9)"))
        assertEquals(StarrocksSqlTypes.DATETIME, canonicalStarrocksType("datetime"))
        assertEquals(StarrocksSqlTypes.DATE, canonicalStarrocksType("date"))
        assertEquals(StarrocksSqlTypes.BOOLEAN, canonicalStarrocksType("boolean"))
        assertEquals(StarrocksSqlTypes.JSON, canonicalStarrocksType("json"))
    }

    // --- modifyColumnSql: schema-evolution nullability (#77) ---

    @Test
    fun `modifyColumnSql skips a nullability-only tighten to NOT NULL`() {
        // Evolution added the column NULLABLE (StarRocks can't ADD NOT NULL to a populated table), so a
        // later sync sees discovered-nullable vs desired-NOT-NULL. Re-issuing MODIFY ... NOT NULL would
        // be rejected every sync — it must be skipped instead.
        val change =
            ColumnTypeChange(
                ColumnType(StarrocksSqlTypes.BIGINT, true),
                ColumnType(StarrocksSqlTypes.BIGINT, false),
            )
        assertNull(modifyColumnSql("`db`.`t`", "age", change))
    }

    @Test
    fun `modifyColumnSql applies a type change but keeps the column nullable when it cannot be tightened`() {
        val change =
            ColumnTypeChange(
                ColumnType(StarrocksSqlTypes.STRING, true),
                ColumnType(StarrocksSqlTypes.BIGINT, false),
            )
        assertEquals(
            "ALTER TABLE `db`.`t` MODIFY COLUMN `age` BIGINT NULL",
            modifyColumnSql("`db`.`t`", "age", change),
        )
    }

    @Test
    fun `modifyColumnSql relaxes a NOT NULL column to NULL`() {
        val change =
            ColumnTypeChange(
                ColumnType(StarrocksSqlTypes.STRING, false),
                ColumnType(StarrocksSqlTypes.STRING, true),
            )
        assertEquals(
            "ALTER TABLE `db`.`t` MODIFY COLUMN `name` STRING NULL",
            modifyColumnSql("`db`.`t`", "name", change),
        )
    }

    @Test
    fun `modifyColumnSql preserves NOT NULL on a type change for an already NOT NULL column`() {
        val change =
            ColumnTypeChange(
                ColumnType(StarrocksSqlTypes.STRING, false),
                ColumnType(StarrocksSqlTypes.BIGINT, false),
            )
        assertEquals(
            "ALTER TABLE `db`.`t` MODIFY COLUMN `code` BIGINT NOT NULL",
            modifyColumnSql("`db`.`t`", "code", change),
        )
    }

    // --- isUnsupportedTypeChange: #70 Gap B (skip a MODIFY StarRocks rejects instead of failing) ---

    @Test
    fun `isUnsupportedTypeChange matches the StarRocks 1064 cannot-change rejection`() {
        assertTrue(
            isUnsupportedTypeChange(SQLException("Can not change BIGINT to DECIMAL128(38,9)", "HY000", 1064)),
        )
    }

    @Test
    fun `isUnsupportedTypeChange does NOT swallow the key-column DROP rejection so Gap A stays a hard error`() {
        // Same 1064 code, different message — must propagate so a PRIMARY KEY column drop still fails loudly.
        assertFalse(
            isUnsupportedTypeChange(
                SQLException("Can not drop key column in primary data model table", "HY000", 1064),
            ),
        )
    }

    @Test
    fun `isUnsupportedTypeChange ignores unrelated SQL errors and a null message`() {
        assertFalse(isUnsupportedTypeChange(SQLException("Communications link failure", "08S01", 0)))
        assertFalse(isUnsupportedTypeChange(SQLException(null as String?, "HY000", 1064)))
    }
}
