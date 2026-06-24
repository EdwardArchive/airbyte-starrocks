/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.sql

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class StarrocksSqlGeneratorTest {

    private val gen = StarrocksSqlGenerator()

    private val columns =
        listOf(
            StarrocksColumn("id", "BIGINT", nullable = true), // key, declared nullable on purpose
            StarrocksColumn("name", "STRING", nullable = true),
            StarrocksColumn("_airbyte_raw_id", "VARCHAR(64)", nullable = false),
            StarrocksColumn("_airbyte_generation_id", "BIGINT", nullable = false),
        )

    @Test
    fun `primary key table puts key columns first, NOT NULL, with PRIMARY KEY + DISTRIBUTED BY HASH`() {
        val ddl = gen.createTable("db", "orders", columns, keyColumns = listOf("id"), model = KeyModel.PRIMARY)

        // key column first and forced NOT NULL even though it was declared nullable
        val body = ddl.substringAfter("(").substringBefore("PRIMARY KEY")
        assertTrue(body.indexOf("`id`") < body.indexOf("`name`"), "key column must be declared first")
        assertTrue(body.contains("`id` BIGINT NOT NULL"), "key column must be NOT NULL")
        assertTrue(ddl.contains("PRIMARY KEY (`id`)"))
        assertTrue(ddl.contains("DISTRIBUTED BY HASH (`id`)"))
        assertTrue(ddl.startsWith("CREATE TABLE IF NOT EXISTS `db`.`orders`"))
    }

    @Test
    fun `composite primary key preserves key order in both clauses`() {
        val cols = columns + StarrocksColumn("region", "STRING", nullable = true)
        val ddl =
            gen.createTable("db", "t", cols, keyColumns = listOf("id", "region"), model = KeyModel.PRIMARY)
        assertTrue(ddl.contains("PRIMARY KEY (`id`, `region`)"))
        assertTrue(ddl.contains("DISTRIBUTED BY HASH (`id`, `region`)"))
        assertTrue(ddl.contains("`region` STRING NOT NULL"))
    }

    @Test
    fun `append table uses DUPLICATE KEY on the raw id`() {
        val ddl =
            gen.createTable(
                "db",
                "events",
                columns,
                keyColumns = listOf("_airbyte_raw_id"),
                model = KeyModel.DUPLICATE,
            )
        assertTrue(ddl.contains("DUPLICATE KEY (`_airbyte_raw_id`)"))
        assertTrue(ddl.contains("DISTRIBUTED BY HASH (`_airbyte_raw_id`)"))
    }

    @Test
    fun `rejects key column not present in columns`() {
        assertThrows<IllegalArgumentException> {
            gen.createTable("db", "t", columns, keyColumns = listOf("missing"), model = KeyModel.PRIMARY)
        }
    }

    @Test
    fun `database and drop helpers`() {
        assertEquals("CREATE DATABASE IF NOT EXISTS `airbyte`", gen.createDatabase("airbyte"))
        assertEquals("DROP TABLE IF EXISTS `db`.`t`", gen.dropTable("db", "t"))
    }
}
