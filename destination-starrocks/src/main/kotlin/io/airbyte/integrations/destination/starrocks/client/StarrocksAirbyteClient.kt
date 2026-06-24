/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.client

import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.ImportType
import io.airbyte.cdk.load.component.ColumnChangeset
import io.airbyte.cdk.load.component.ColumnType
import io.airbyte.cdk.load.component.TableOperationsClient
import io.airbyte.cdk.load.component.TableSchema
import io.airbyte.cdk.load.component.TableSchemaEvolutionClient
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_EXTRACTED_AT
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_GENERATION_ID
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_META
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_RAW_ID
import io.airbyte.cdk.load.schema.model.StreamTableSchema
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.table.ColumnNameMapping
import io.airbyte.integrations.destination.starrocks.schema.StarrocksSqlTypes
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfiguration
import io.airbyte.integrations.destination.starrocks.sql.KeyModel
import io.airbyte.integrations.destination.starrocks.sql.quoteIdent
import io.airbyte.integrations.destination.starrocks.sql.StarrocksColumn
import io.airbyte.integrations.destination.starrocks.sql.StarrocksSqlGenerator
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.sql.DriverManager

private val log = KotlinLogging.logger {}

/**
 * Append/Overwrite -> DUPLICATE KEY (StarRocks keeps every row; a DUPLICATE key is NOT a uniqueness
 * constraint, and the key is the per-record unique `_airbyte_raw_id`, so append never collapses
 * business-duplicate records). Dedupe -> PRIMARY KEY (StarRocks upserts/deletes by key at load time
 * via `__op`). This is the guard against accidental dedup of append data.
 */
internal fun keyModelFor(importType: ImportType): KeyModel =
    if (importType is Dedupe) KeyModel.PRIMARY else KeyModel.DUPLICATE

/**
 * Maps an `information_schema.columns.DATA_TYPE` value (lowercase, length stripped) back to the
 * connector's canonical StarRocks type literal, so [StarrocksAirbyteClient.discoverSchema] compares
 * equal to the mapper's output. Without this, a second sync sees every column as "changed" (e.g.
 * discovered `bigint` vs canonical `BIGINT`) and tries to `ALTER ... MODIFY` the PRIMARY KEY columns,
 * which StarRocks rejects ("Can not modify key column ... for primary key table").
 */
internal fun canonicalStarrocksType(dataType: String): String =
    when (dataType.lowercase().substringBefore('(').trim()) {
        "boolean" -> StarrocksSqlTypes.BOOLEAN
        "bigint" -> StarrocksSqlTypes.BIGINT
        "decimal", "decimalv2", "decimalv3", "decimal32", "decimal64", "decimal128" ->
            StarrocksSqlTypes.DECIMAL
        "date" -> StarrocksSqlTypes.DATE
        "datetime" -> StarrocksSqlTypes.DATETIME
        "varchar", "char", "string" -> StarrocksSqlTypes.STRING
        "json" -> StarrocksSqlTypes.JSON
        else -> dataType.uppercase()
    }

/**
 * StarRocks implementation of the CDK [TableOperationsClient] / [TableSchemaEvolutionClient]. All DDL
 * and table metadata run over the MySQL protocol (port 9030). High-volume row writes do NOT go
 * through here — they use Stream Load over HTTP (see the dataflow Aggregate). This mirrors
 * `ClickhouseAirbyteClient`, adapted to StarRocks SQL.
 *
 * Deduplication is performed at load time via the `__op` column into a PRIMARY KEY table, so the
 * server-side [upsertTable] merge that ClickHouse leaves to its engine is likewise unimplemented
 * here.
 */
class StarrocksAirbyteClient(
    private val config: StarrocksConfiguration,
    private val sqlGenerator: StarrocksSqlGenerator,
) : TableOperationsClient, TableSchemaEvolutionClient {

    private fun <T> withConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(config.jdbcUrl, config.username, config.password).use(block)

    private fun execute(sql: String) {
        log.info { sql }
        withConnection { conn -> conn.createStatement().use { it.execute(sql) } }
    }

    override suspend fun createNamespace(namespace: String) {
        execute(sqlGenerator.createDatabase(namespace))
    }

    override suspend fun createTable(
        stream: DestinationStream,
        tableName: TableName,
        columnNameMapping: ColumnNameMapping,
        replace: Boolean,
    ) {
        if (replace) {
            execute(sqlGenerator.dropTable(tableName.namespace, tableName.name))
        }
        val (columns, keyColumns, model) = describeTable(stream.tableSchema)
        execute(
            sqlGenerator.createTable(
                database = tableName.namespace,
                table = tableName.name,
                columns = columns,
                keyColumns = keyColumns,
                model = model,
                // When replacing we just dropped the table; otherwise keep it idempotent.
                ifNotExists = !replace,
            ),
        )
    }

    override suspend fun dropTable(tableName: TableName) {
        execute(sqlGenerator.dropTable(tableName.namespace, tableName.name))
    }

    override suspend fun overwriteTable(sourceTableName: TableName, targetTableName: TableName) {
        // StarRocks atomically swaps the two tables' data/schema; the SWAP WITH operand must be an
        // unqualified name (same database). We then drop the (now-source) leftover.
        execute(
            sqlGenerator.swapTable(
                targetTableName.namespace,
                targetTableName.name,
                sourceTableName.name,
            ),
        )
        execute(sqlGenerator.dropTable(sourceTableName.namespace, sourceTableName.name))
    }

    override suspend fun copyTable(
        columnNameMapping: ColumnNameMapping,
        sourceTableName: TableName,
        targetTableName: TableName,
    ) {
        val cols =
            (META_COLUMNS + columnNameMapping.values).joinToString(", ") { quoteIdent(it) }
        execute(
            "INSERT INTO ${quoteIdent(targetTableName.namespace)}.${quoteIdent(targetTableName.name)} ($cols) " +
                "SELECT $cols FROM ${quoteIdent(sourceTableName.namespace)}.${quoteIdent(sourceTableName.name)}",
        )
    }

    override suspend fun upsertTable(
        stream: DestinationStream,
        columnNameMapping: ColumnNameMapping,
        sourceTableName: TableName,
        targetTableName: TableName,
    ) {
        // StarRocks deduplicates at LOAD time (`__op` into a PRIMARY KEY table), so there is no
        // server-side temp->real merge step to implement.
        throw NotImplementedError("StarRocks deduplicates at load time via __op into a PRIMARY KEY table")
    }

    override suspend fun namespaceExists(namespace: String): Boolean =
        withConnection { conn ->
            conn.prepareStatement(
                    "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?",
                )
                .use { ps ->
                    ps.setString(1, namespace)
                    ps.executeQuery().use { it.next() }
                }
        }

    override suspend fun tableExists(table: TableName): Boolean =
        withConnection { conn ->
            conn.prepareStatement(
                    "SELECT TABLE_NAME FROM information_schema.tables " +
                        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                )
                .use { ps ->
                    ps.setString(1, table.namespace)
                    ps.setString(2, table.name)
                    ps.executeQuery().use { it.next() }
                }
        }

    override suspend fun countTable(tableName: TableName): Long? =
        try {
            withConnection { conn ->
                conn.createStatement().use { stmt ->
                    stmt
                        .executeQuery(
                            "SELECT count(1) FROM ${quoteIdent(tableName.namespace)}.${quoteIdent(tableName.name)}",
                        )
                        .use { rs -> if (rs.next()) rs.getLong(1) else null }
                }
            }
        } catch (e: Exception) {
            // Missing table -> treat as "does not exist" so the CDK sees it as empty.
            log.debug(e) { "countTable failed for $tableName (treating as absent)" }
            null
        }

    override suspend fun getGenerationId(tableName: TableName): Long =
        try {
            withConnection { conn ->
                conn.createStatement().use { stmt ->
                    stmt
                        .executeQuery(
                            "SELECT ${quoteIdent(COLUMN_NAME_AB_GENERATION_ID)} " +
                                "FROM ${quoteIdent(tableName.namespace)}.${quoteIdent(tableName.name)} LIMIT 1",
                        )
                        .use { rs -> if (rs.next()) rs.getLong(1) else 0L }
                }
            }
        } catch (e: Exception) {
            log.error(e) { "Failed to read generation id from $tableName" }
            0L
        }

    override suspend fun discoverSchema(tableName: TableName): TableSchema =
        withConnection { conn ->
            val columns = LinkedHashMap<String, ColumnType>()
            conn.prepareStatement(
                    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM information_schema.columns " +
                        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                )
                .use { ps ->
                    ps.setString(1, tableName.namespace)
                    ps.setString(2, tableName.name)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            val name = rs.getString("COLUMN_NAME")
                            if (name in META_COLUMNS) continue
                            val type = rs.getString("DATA_TYPE")
                            val nullable = rs.getString("IS_NULLABLE").equals("YES", ignoreCase = true)
                            columns[name] = ColumnType(canonicalStarrocksType(type), nullable)
                        }
                    }
                }
            TableSchema(columns)
        }

    override fun computeSchema(
        stream: DestinationStream,
        columnNameMapping: ColumnNameMapping,
    ): TableSchema = TableSchema(stream.tableSchema.columnSchema.finalSchema)

    override suspend fun applyChangeset(
        stream: DestinationStream,
        columnNameMapping: ColumnNameMapping,
        tableName: TableName,
        expectedColumns: Map<String, ColumnType>,
        columnChangeset: ColumnChangeset,
    ) {
        if (columnChangeset.isNoop()) return

        // StarRocks PRIMARY KEY columns are immutable — never ALTER them (belt-and-suspenders on top
        // of canonicalStarrocksType, which prevents the spurious diffs that flagged them).
        val keyColumns = describeTable(stream.tableSchema).second.toSet()

        val qualifiedTable = "${quoteIdent(tableName.namespace)}.${quoteIdent(tableName.name)}"
        columnChangeset.columnsToAdd.forEach { (name, type) ->
            execute(
                "ALTER TABLE $qualifiedTable " +
                    "ADD COLUMN ${quoteIdent(name)} ${type.type}${if (type.nullable) " NULL" else " NOT NULL"}",
            )
        }
        columnChangeset.columnsToChange.forEach { (name, change) ->
            if (name in keyColumns) {
                log.warn { "Skipping ALTER on key column `$name` — StarRocks PRIMARY KEY columns are immutable" }
                return@forEach
            }
            val type = change.newType
            execute(
                "ALTER TABLE $qualifiedTable " +
                    "MODIFY COLUMN ${quoteIdent(name)} ${type.type}${if (type.nullable) " NULL" else " NOT NULL"}",
            )
        }
        columnChangeset.columnsToDrop.forEach { (name, _) ->
            execute("ALTER TABLE $qualifiedTable DROP COLUMN ${quoteIdent(name)}")
        }
    }

    /**
     * Resolves a [StreamTableSchema] into the StarRocks (columns, keyColumns, model) needed by the
     * SQL generator. Adds the four Airbyte meta columns and chooses the key model: Dedupe -> PRIMARY
     * on the stream PK; else DUPLICATE on `_airbyte_raw_id`.
     */
    private fun describeTable(
        tableSchema: StreamTableSchema,
    ): Triple<List<StarrocksColumn>, List<String>, KeyModel> {
        val metaColumns =
            listOf(
                StarrocksColumn(COLUMN_NAME_AB_RAW_ID, StarrocksSqlTypes.STRING, nullable = false),
                StarrocksColumn(COLUMN_NAME_AB_EXTRACTED_AT, StarrocksSqlTypes.DATETIME, nullable = false),
                StarrocksColumn(COLUMN_NAME_AB_META, StarrocksSqlTypes.STRING, nullable = false),
                StarrocksColumn(COLUMN_NAME_AB_GENERATION_ID, StarrocksSqlTypes.BIGINT, nullable = false),
            )
        val userColumns =
            tableSchema.columnSchema.finalSchema.map { (name, type) ->
                StarrocksColumn(name, type.type, type.nullable)
            }

        val importType = tableSchema.importType
        val keyColumns =
            if (importType is Dedupe) importType.primaryKey.map { it.single() }
            else listOf(COLUMN_NAME_AB_RAW_ID)
        return Triple(metaColumns + userColumns, keyColumns, keyModelFor(importType))
    }

    companion object {
        private val META_COLUMNS =
            listOf(
                COLUMN_NAME_AB_RAW_ID,
                COLUMN_NAME_AB_EXTRACTED_AT,
                COLUMN_NAME_AB_META,
                COLUMN_NAME_AB_GENERATION_ID,
            )
    }
}
