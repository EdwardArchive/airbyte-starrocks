/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.write.load

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.load.data.AirbyteValue
import io.airbyte.cdk.load.data.IntegerValue
import io.airbyte.cdk.load.data.NullValue
import io.airbyte.cdk.load.data.NumberValue
import io.airbyte.cdk.load.data.StringValue
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.table.CDC_DELETED_AT_COLUMN
import io.airbyte.integrations.destination.starrocks.http.StreamLoadClient
import java.math.BigDecimal
import java.math.BigInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class JsonRowInsertBufferTest {

    private val unusedClient = StreamLoadClient("localhost", 8030, "u", "p")
    private val table = TableName("db", "orders")
    private val columns = listOf("_airbyte_raw_id", "id", "name", "amount", CDC_DELETED_AT_COLUMN)
    private val mapper = ObjectMapper()

    private fun buf(cdc: Boolean, soft: Boolean = false) =
        JsonRowInsertBuffer(table, columns, cdcDeleteEnabled = cdc, streamLoadClient = unusedClient, softDelete = soft)

    @Test
    fun `numbers are JSON strings, literal backslash-N survives, null is JSON null`() {
        val b = buf(cdc = false)
        b.accumulate(
            mapOf<String, AirbyteValue>(
                "_airbyte_raw_id" to StringValue("rid"),
                "id" to IntegerValue(BigInteger("9223372036854775807")), // BIGINT max
                "name" to StringValue("\\N"), // literal backslash-N — NULL in CSV, must survive in JSON
                "amount" to NumberValue(BigDecimal("12345.678901234567890")),
                CDC_DELETED_AT_COLUMN to NullValue,
            ),
        )
        val node = mapper.readTree(b.bodySnapshot())[0]
        // integers/decimals as strings -> full precision (a JSON number would round to a double)
        assertTrue(node["id"].isTextual, "integers must be JSON strings")
        assertEquals("9223372036854775807", node["id"].asText())
        assertTrue(node["amount"].isTextual, "decimals must be JSON strings")
        assertEquals("12345.678901234567890", node["amount"].asText())
        // the literal `\N` string is preserved, not coerced to null
        assertEquals("\\N", node["name"].asText())
        assertTrue(node[CDC_DELETED_AT_COLUMN].isNull)
    }

    @Test
    fun `cdc delete adds __op 1 and soft delete keeps __op 0`() {
        val del = buf(cdc = true)
        del.accumulate(
            mapOf<String, AirbyteValue>(
                "_airbyte_raw_id" to StringValue("r"),
                CDC_DELETED_AT_COLUMN to StringValue("2026-01-01T00:00:00Z"),
            ),
        )
        assertEquals(1, mapper.readTree(del.bodySnapshot())[0][CsvRowInsertBuffer.OP_COLUMN].asInt())

        val soft = buf(cdc = true, soft = true)
        soft.accumulate(
            mapOf<String, AirbyteValue>(
                "_airbyte_raw_id" to StringValue("r"),
                CDC_DELETED_AT_COLUMN to StringValue("2026-01-01T00:00:00Z"),
            ),
        )
        assertEquals(0, mapper.readTree(soft.bodySnapshot())[0][CsvRowInsertBuffer.OP_COLUMN].asInt())
    }

    @Test
    fun `headers select JSON format and list columns including __op`() {
        assertEquals(
            mapOf(
                "format" to "JSON",
                "strip_outer_array" to "true",
                "columns" to "_airbyte_raw_id,id,name,amount,_ab_cdc_deleted_at,__op",
            ),
            buf(cdc = true).streamLoadHeaders(),
        )
    }
}
