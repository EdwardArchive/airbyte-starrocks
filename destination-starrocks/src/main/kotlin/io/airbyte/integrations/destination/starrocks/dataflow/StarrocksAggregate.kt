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
import io.airbyte.integrations.destination.starrocks.write.load.CsvRowInsertBuffer
import io.micronaut.context.annotation.Factory

/** Wraps a [CsvRowInsertBuffer]: each accepted record becomes a CSV row; [flush] issues Stream Load. */
class StarrocksAggregate(
    internal val buffer: CsvRowInsertBuffer,
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
) : AggregateFactory {

    override fun create(key: DestinationStream.Descriptor): Aggregate {
        val tableName = streamStateStore.get(key)!!.tableName
        val stream = catalog.getStream(key)

        val buffer =
            CsvRowInsertBuffer(
                tableName = tableName,
                columns = finalColumns(stream),
                cdcDeleteEnabled = cdcDeleteEnabled(stream),
                streamLoadClient = streamLoadClient,
            )
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
