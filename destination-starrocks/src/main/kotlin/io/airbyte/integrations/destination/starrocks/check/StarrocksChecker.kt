/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.check

import io.airbyte.cdk.load.check.DestinationChecker
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfiguration
import io.airbyte.integrations.destination.starrocks.version.StarrocksVersionGate
import jakarta.inject.Singleton
import java.sql.DriverManager

/**
 * `check` for StarRocks: connect over the MySQL protocol (9030), read `current_version()`, and
 * validate the version against the connector's feature gating ([StarrocksVersionGate], issue #14).
 *
 * CDK 1.0.13 [DestinationChecker] is non-parameterized: the config is injected and `check()` takes
 * no arguments. Any thrown exception becomes the FAILED connection-status message.
 */
@Singleton
class StarrocksChecker(
    private val config: StarrocksConfiguration,
) : DestinationChecker {

    override fun check() {
        val version =
            DriverManager.getConnection(config.jdbcUrl, config.username, config.password).use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT current_version()").use { rs ->
                        require(rs.next()) {
                            "StarRocks check failed: current_version() returned no rows"
                        }
                        rs.getString(1)
                    }
                }
            }
        require(!version.isNullOrBlank()) { "StarRocks check failed: empty version string" }
        StarrocksVersionGate.validate(version)
    }
}
