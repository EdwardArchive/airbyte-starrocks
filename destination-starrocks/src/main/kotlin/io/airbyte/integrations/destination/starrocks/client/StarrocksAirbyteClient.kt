/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.client

import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
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
import io.airbyte.integrations.destination.starrocks.sql.StarrocksColumn
import io.airbyte.integrations.destination.starrocks.sql.StarrocksSqlGenerator
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.sql.DriverManager

private val log = KotlinLogging.logger {}

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
        // StarRocks atomically swaps the two tables' data/schema; we then drop the (now-source) leftover.
        execute(
            "ALTER TABLE `${targetTableName.namespace}`.`${targetTableName.name}` " +
                "SWAP WITH `${sourceTableName.namespace}`.`${sourceTableName.name}`",
        )
        execute(sqlGenerator.dropTable(sourceTableName.namespace, sourceTableName.name))
    }

    override suspend fun copyTable(
        columnNameMapping: ColumnNameMapping,
        sourceTableName: TableName,
        targetTableName: TableName,
    ) {
        val cols =
            (META_COLUMNS + columnNameMapping.values).joinToString(", ") { "`$it`" }
        execute(
            "INSERT INTO `${targetTableName.namespace}`.`${targetTableName.name}` ($cols) " +
                "SELECT $cols FROM `${sourceTableName.namespace}`.`${sourceTableName.name}`",
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
                            "SELECT count(1) FROM `${tableName.namespace}`.`${tableName.name}`",
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
                            "SELECT `$COLUMN_NAME_AB_GENERATION_ID` " +
                                "FROM `${tableName.namespace}`.`${tableName.name}` LIMIT 1",
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
                            columns[name] = ColumnType(type, nullable)
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

        columnChangeset.columnsToAdd.forEach { (name, type) ->
            execute(
                "ALTER TABLE `${tableName.namespace}`.`${tableName.name}` " +
                    "ADD COLUMN `$name` ${type.type}${if (type.nullable) " NULL" else " NOT NULL"}",
            )
        }
        columnChangeset.columnsToChange.forEach { (name, change) ->
            val type = change.newType
            execute(
                "ALTER TABLE `${tableName.namespace}`.`${tableName.name}` " +
                    "MODIFY COLUMN `$name` ${type.type}${if (type.nullable) " NULL" else " NOT NULL"}",
            )
        }
        columnChangeset.columnsToDrop.forEach { (name, _) ->
            execute(
                "ALTER TABLE `${tableName.namespace}`.`${tableName.name}` DROP COLUMN `$name`",
            )
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

        return when (val importType = tableSchema.importType) {
            is Dedupe -> {
                val pks = importType.primaryKey.map { it.single() }
                Triple(metaColumns + userColumns, pks, KeyModel.PRIMARY)
            }
            else ->
                Triple(metaColumns + userColumns, listOf(COLUMN_NAME_AB_RAW_ID), KeyModel.DUPLICATE)
        }
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
