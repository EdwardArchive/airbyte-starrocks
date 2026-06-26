/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.dataflow

import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.dataflow.aggregate.Aggregate
import io.airbyte.cdk.load.dataflow.aggregate.AggregateFactory
import io.airbyte.cdk.load.dataflow.transform.RecordDTO
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_EXTRACTED_AT
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_GENERATION_ID
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_META
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_RAW_ID
import io.airbyte.cdk.load.table.CDC_DELETED_AT_COLUMN
import io.airbyte.cdk.load.table.directload.DirectLoadTableExecutionConfig
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.starrocks.http.StreamLoadClient
import io.airbyte.integrations.destination.starrocks.spec.LoadCompression
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfiguration
import io.airbyte.integrations.destination.starrocks.version.StarrocksVersionGate
import io.airbyte.integrations.destination.starrocks.write.load.CsvRowInsertBuffer
import io.airbyte.integrations.destination.starrocks.write.load.JsonRowInsertBuffer
import io.airbyte.integrations.destination.starrocks.write.load.RowInsertBuffer
import io.micronaut.context.annotation.Factory

/** Wraps a [RowInsertBuffer]: each accepted record is buffered; [flush] issues Stream Load. */
class StarrocksAggregate(
    internal val buffer: RowInsertBuffer,
) : Aggregate {

    override fun accept(record: RecordDTO) {
        buffer.accumulate(record.fields)
    }

    override suspend fun flush() {
        buffer.flush()
    }
}

@Factory
class StarrocksAggregateFactory(
    private val catalog: DestinationCatalog,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
    private val streamLoadClient: StreamLoadClient,
    private val config: StarrocksConfiguration,
    private val capabilities: StarrocksVersionGate.Capabilities,
) : AggregateFactory {

    override fun create(key: DestinationStream.Descriptor): Aggregate {
        val tableName =
            requireNotNull(streamStateStore.get(key)) {
                "No Stream Load execution config for stream '${key.name}'"
            }.tableName
        val stream = catalog.getStream(key)
        val cdcDelete = cdcDeleteEnabled(stream)

        // The CDC delete path appends an `__op` column; a source column already named `__op`
        // (reserved in StarRocks >= 3.3.6) would collide and corrupt the load — fail clearly (#45).
        require(!(cdcDelete && CsvRowInsertBuffer.OP_COLUMN in stream.tableSchema.columnSchema.finalSchema.keys)) {
            "Stream '${key.name}' has a column named '${CsvRowInsertBuffer.OP_COLUMN}', which collides " +
                "with the CDC operation column StarRocks uses for load-time delete/upsert. Rename it at the source."
        }

        val columns = finalColumns(stream)
        val buffer: RowInsertBuffer =
            if (config.loadAsJson) {
                // Compress only when requested AND the detected version supports request-body
                // compression (>= 3.3.2). `check` already fails fast on compression+CSV or +old
                // cluster; this is the write-time guard that actually flips it on.
                val compression =
                    if (LoadCompression.isEnabled(config.compression) && capabilities.compression) {
                        config.compression
                    } else {
                        null
                    }
                JsonRowInsertBuffer(
                    tableName,
                    columns,
                    cdcDelete,
                    streamLoadClient,
                    config.cdcSoftDelete,
                    compression = compression,
                )
            } else {
                CsvRowInsertBuffer(tableName, columns, cdcDelete, streamLoadClient, config.cdcSoftDelete)
            }
        return StarrocksAggregate(buffer)
    }

    companion object {
        private val META_COLUMNS =
            listOf(
                COLUMN_NAME_AB_RAW_ID,
                COLUMN_NAME_AB_EXTRACTED_AT,
                COLUMN_NAME_AB_META,
                COLUMN_NAME_AB_GENERATION_ID,
            )

        /** Final column order for the CSV/`columns` header: Airbyte meta columns then user columns. */
        fun finalColumns(stream: DestinationStream): List<String> =
            META_COLUMNS + stream.tableSchema.columnSchema.finalSchema.keys

        /** True only for Dedupe streams whose final schema carries the CDC deleted-at column. */
        fun cdcDeleteEnabled(stream: DestinationStream): Boolean =
            stream.tableSchema.importType is Dedupe &&
                stream.tableSchema.columnSchema.finalSchema.containsKey(CDC_DELETED_AT_COLUMN)
    }
}
