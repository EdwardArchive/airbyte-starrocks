/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.client

import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.ImportType
import io.airbyte.cdk.load.component.ColumnChangeset
import io.airbyte.cdk.load.component.ColumnType
import io.airbyte.cdk.load.component.ColumnTypeChange
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
import io.airbyte.integrations.destination.starrocks.tunnel.StarrocksSshTunnel
import io.airbyte.integrations.destination.starrocks.sql.quoteIdent
import io.airbyte.integrations.destination.starrocks.sql.StarrocksColumn
import io.airbyte.integrations.destination.starrocks.sql.StarrocksSqlGenerator
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

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
 * Builds the `ALTER TABLE ... MODIFY COLUMN` for a single changed column, or `null` if the change
 * must be skipped.
 *
 * StarRocks cannot tighten a populated column to `NOT NULL` (existing rows already hold NULLs, and
 * schema-evolution-added columns are stored NULLABLE on purpose — see [StarrocksAirbyteClient.applyChangeset]).
 * Discovering such a column reads it back as nullable while the desired schema still wants NOT NULL,
 * so a naive MODIFY is rejected by StarRocks and re-issued on every subsequent sync (#77). A
 * nullability-only tighten is therefore skipped; a genuine type change is still applied, but the
 * column is kept NULLABLE for the same reason the ADD path forces NULL.
 */
internal fun modifyColumnSql(qualifiedTable: String, name: String, change: ColumnTypeChange): String? {
    val current = change.originalType
    val desired = change.newType
    val tightensToNotNull = current.nullable && !desired.nullable
    if (tightensToNotNull && desired.type == current.type) return null
    val nullClause = if (desired.nullable || tightensToNotNull) " NULL" else " NOT NULL"
    return "ALTER TABLE $qualifiedTable MODIFY COLUMN ${quoteIdent(name)} ${desired.type}$nullClause"
}

/**
 * StarRocks rejects an in-place column type change it cannot perform (e.g. integer<->number, i.e.
 * `BIGINT`<->`DECIMAL`) with error 1064 "Can not change <X> to <Y>". The set of in-place-convertible
 * type pairs is version-dependent and not fully documented (e.g. `VARCHAR`->`DECIMAL` works despite
 * being absent from the docs), so rather than hardcode a matrix we let StarRocks decide and treat this
 * specific rejection as "skip this column" (issue #70, Gap B): the column keeps its old type until a
 * manual Refresh recreates the table. Any other SQLException is a real failure and must propagate.
 */
internal fun isUnsupportedTypeChange(e: SQLException): Boolean =
    e.errorCode == 1064 && e.message?.contains("Can not change", ignoreCase = true) == true

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
    private val tunnel: StarrocksSshTunnel,
) : TableOperationsClient, TableSchemaEvolutionClient {

    // When tunneling, connect to the SSH local forward; otherwise straight to StarRocks (#68).
    private val jdbcUrl: String = config.jdbcUrlFor(tunnel.jdbcHost, tunnel.jdbcPort)

    private fun <T> withConnection(block: (Connection) -> T): T =
        DriverManager.getConnection(jdbcUrl, config.username, config.password).use(block)

    private fun execute(sql: String) {
        log.info { sql }
        withConnection { conn -> conn.createStatement().use { it.execute(sql) } }
    }

    /**
     * StarRocks server version (`SELECT current_version()`), read once and cached. Used at write
     * start to opt into version-gated Stream Load capabilities (see [StarrocksVersionGate]); `check`
     * reads it separately for fail-fast validation.
     */
    private val cachedVersion: String by lazy {
        withConnection { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT current_version()").use { rs ->
                    if (rs.next()) rs.getString(1).orEmpty() else ""
                }
            }
        }
    }

    fun serverVersion(): String = cachedVersion

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
                replicationNum = config.replicationNum,
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

    // A genuinely missing table is reported as null ("absent") via the explicit existence check;
    // any other (connection/auth/query) error propagates rather than being silently swallowed (#42).
    override suspend fun countTable(tableName: TableName): Long? {
        if (!tableExists(tableName)) return null
        return withConnection { conn ->
            conn.createStatement().use { stmt ->
                stmt
                    .executeQuery(
                        "SELECT count(1) FROM ${quoteIdent(tableName.namespace)}.${quoteIdent(tableName.name)}",
                    )
                    .use { rs -> if (rs.next()) rs.getLong(1) else null }
            }
        }
    }

    // MAX (not LIMIT 1 without ORDER BY) so the generation id is deterministic even when the table
    // holds rows from multiple generations (#41). Missing table -> 0L; other errors propagate (#42).
    override suspend fun getGenerationId(tableName: TableName): Long {
        if (!tableExists(tableName)) return 0L
        return withConnection { conn ->
            conn.createStatement().use { stmt ->
                stmt
                    .executeQuery(
                        "SELECT MAX(${quoteIdent(COLUMN_NAME_AB_GENERATION_ID)}) " +
                            "FROM ${quoteIdent(tableName.namespace)}.${quoteIdent(tableName.name)}",
                    )
                    .use { rs -> if (rs.next()) rs.getLong(1) else 0L }
            }
        }
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
            // New columns are always added NULLABLE: StarRocks rejects `ADD COLUMN ... NOT NULL`
            // without a DEFAULT on a populated table, and existing rows have no value for a freshly
            // added column anyway (so they must be null) (#43).
            if (!type.nullable) {
                log.warn { "Adding column `$name` as NULLABLE — StarRocks cannot ADD a NOT NULL column to an existing table" }
            }
            execute("ALTER TABLE $qualifiedTable ADD COLUMN ${quoteIdent(name)} ${type.type} NULL")
        }
        columnChangeset.columnsToChange.forEach { (name, change) ->
            if (name in keyColumns) {
                log.warn { "Skipping ALTER on key column `$name` — StarRocks PRIMARY KEY columns are immutable" }
                return@forEach
            }
            val sql = modifyColumnSql(qualifiedTable, name, change)
            if (sql == null) {
                log.warn { "Skipping MODIFY on `$name` — StarRocks cannot tighten a populated column to NOT NULL" }
                return@forEach
            }
            try {
                execute(sql)
            } catch (e: SQLException) {
                // #70 Gap B: StarRocks can't convert this type in place (a non_breaking source type
                // change Airbyte still propagates) — skip it instead of failing the whole sync. The old
                // type lingers until a manual Refresh recreates the table.
                if (isUnsupportedTypeChange(e)) {
                    log.warn {
                        "Skipping MODIFY on `$name` — StarRocks cannot convert ${change.originalType.type} to " +
                            "${change.newType.type} in place (${e.message}). Run a Refresh to apply it."
                    }
                } else {
                    throw e
                }
            }
        }
        columnChangeset.columnsToDrop.forEach { (name, _) ->
            // #70 Gap A (deferred — intentionally not guarded): if `name` is a PRIMARY KEY column (the
            // source dropped/renamed its PK), StarRocks rejects this ("Can not drop key column in
            // primary data model table") and the sync fails. Skipping wouldn't actually help — the old
            // NOT NULL key column would then break the next load — so the real fix is a table
            // recreation, which Airbyte gates behind a manual Refresh (a PK removal is a breaking change
            // → the connection pauses for review). Revisit only to surface a clearer error than 1064.
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
