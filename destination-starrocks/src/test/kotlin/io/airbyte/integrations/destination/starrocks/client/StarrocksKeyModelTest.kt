/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.client

import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.Dedupe
import io.airbyte.cdk.load.command.Overwrite
import io.airbyte.integrations.destination.starrocks.sql.KeyModel
import org.junit.jupiter.api.Assertions.assertEquals
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
}
