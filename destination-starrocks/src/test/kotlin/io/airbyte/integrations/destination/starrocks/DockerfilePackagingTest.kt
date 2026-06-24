/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File

/**
 * Regression guard for the packaging bug caught by the live Airbyte e2e: Airbyte's workload launcher
 * execs `$AIRBYTE_ENTRYPOINT` (= `/airbyte/base.sh`) DIRECTLY and ignores the Docker ENTRYPOINT, so
 * base.sh must be made executable, and the AIRBYTE_*_CMD vars must point at the connector launcher.
 * (A plain `docker run <img> spec` uses the ENTRYPOINT and masks the missing execute bit — that is
 * exactly how the bug slipped through local testing before, producing exit code 126 in Airbyte.)
 */
class DockerfilePackagingTest {

    private val dockerfile: String =
        listOf(File("Dockerfile"), File("destination-starrocks/Dockerfile"))
            .firstOrNull { it.exists() }
            ?.readText()
            ?: error("Dockerfile not found from ${File(".").absolutePath}")

    @Test
    fun `base_sh is made executable`() {
        assertTrue(
            Regex("""chmod\s+\+x\s+/airbyte/base\.sh""").containsMatchIn(dockerfile),
            "Dockerfile must `chmod +x /airbyte/base.sh` — Airbyte execs it directly (exit 126 otherwise)",
        )
    }

    @Test
    fun `airbyte op command env vars point at the launcher`() {
        for (v in listOf("AIRBYTE_SPEC_CMD", "AIRBYTE_CHECK_CMD", "AIRBYTE_WRITE_CMD")) {
            assertTrue(dockerfile.contains(v), "Dockerfile must set $v")
        }
        assertTrue(
            dockerfile.contains("/airbyte/bin/destination-starrocks"),
            "AIRBYTE_*_CMD must invoke the application launcher",
        )
    }
}
