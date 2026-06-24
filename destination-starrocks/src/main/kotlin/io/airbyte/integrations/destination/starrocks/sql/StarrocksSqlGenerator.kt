/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.sql

/** A resolved StarRocks column (name + SQL type + nullability). */
data class StarrocksColumn(val name: String, val sqlType: String, val nullable: Boolean)

/** StarRocks table key model: PRIMARY KEY (dedup/upsert) vs DUPLICATE KEY (append). */
enum class KeyModel {
    PRIMARY,
    DUPLICATE,
}

/**
 * Pure StarRocks DDL string generation (no CDK types — unit-testable in isolation). The
 * StreamTableSchema -> (columns, keys, model) extraction lives in the Airbyte client (issue #11).
 *
 * StarRocks PRIMARY KEY tables require the key columns to be declared FIRST, in key order, and
 * NOT NULL — this generator enforces both.
 */
class StarrocksSqlGenerator {

    fun createDatabase(database: String): String = "CREATE DATABASE IF NOT EXISTS `$database`"

    fun dropTable(database: String, table: String): String =
        "DROP TABLE IF EXISTS `$database`.`$table`"

    fun createTable(
        database: String,
        table: String,
        columns: List<StarrocksColumn>,
        keyColumns: List<String>,
        model: KeyModel,
        ifNotExists: Boolean = true,
    ): String {
        require(keyColumns.isNotEmpty()) { "keyColumns must not be empty" }
        val byName = columns.associateBy { it.name }
        keyColumns.forEach {
            require(byName.containsKey(it)) { "key column '$it' is not present in columns" }
        }

        // Key columns first, in key order, forced NOT NULL; then the remaining columns in order.
        val keyDefs = keyColumns.map { byName.getValue(it).copy(nullable = false) }
        val rest = columns.filter { it.name !in keyColumns }
        val columnLines =
            (keyDefs + rest).joinToString(",\n") { col ->
                "  `${col.name}` ${col.sqlType}${if (col.nullable) "" else " NOT NULL"}"
            }

        val keyClause = if (model == KeyModel.PRIMARY) "PRIMARY KEY" else "DUPLICATE KEY"
        val keyList = keyColumns.joinToString(", ") { "`$it`" }
        val exists = if (ifNotExists) "IF NOT EXISTS " else ""

        return buildString {
            append("CREATE TABLE ").append(exists).append("`$database`.`$table` (\n")
            append(columnLines).append('\n')
            append(")\n")
            append("$keyClause ($keyList)\n")
            append("DISTRIBUTED BY HASH ($keyList)")
        }
    }
}
