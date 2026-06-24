/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.version

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class StarrocksVersionGateTest {

    @Test
    fun `parses plain and suffixed version strings`() {
        assertEquals("3.3.11", StarrocksVersionGate.parse("3.3.11").toString())
        // current_version() can return extra build info after the semver
        assertEquals("4.1.1", StarrocksVersionGate.parse("4.1.1 e8a3a2c").toString())
        assertEquals("3.5.4", StarrocksVersionGate.parse("StarRocks-3.5.4").toString())
    }

    @Test
    fun `rejects non-version strings`() {
        assertThrows<IllegalArgumentException> { StarrocksVersionGate.parse("not-a-version") }
    }

    @Test
    fun `capabilities at the 3_3_11 baseline`() {
        val c = StarrocksVersionGate.capabilities("3.3.11")
        assertTrue(c.pkDedup)
        assertTrue(c.columnModePartialUpdate)
        assertTrue(c.columnModePartialWithCondition)
        assertFalse(c.mergeCommit) // 3.3.11 < 3.4.0
    }

    @Test
    fun `capabilities grow with version`() {
        val v35 = StarrocksVersionGate.capabilities("3.5.4")
        assertTrue(v35.mergeCommit)

        val v33early = StarrocksVersionGate.capabilities("3.3.5")
        assertTrue(v33early.pkDedup)
        assertTrue(v33early.columnModePartialUpdate)
        assertFalse(v33early.columnModePartialWithCondition) // needs >= 3.3.11
    }

    @Test
    fun `validate rejects versions below the PK floor`() {
        assertThrows<IllegalStateException> { StarrocksVersionGate.validate("3.0.5") }
        assertThrows<IllegalStateException> { StarrocksVersionGate.validate("2.5.0") }
    }

    @Test
    fun `validate accepts baseline and newer`() {
        StarrocksVersionGate.validate("3.1.0")
        StarrocksVersionGate.validate("3.3.11")
        StarrocksVersionGate.validate("4.1.1")
    }
}
