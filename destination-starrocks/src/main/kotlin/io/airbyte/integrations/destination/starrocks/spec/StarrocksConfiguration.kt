/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.spec

import io.airbyte.cdk.load.command.DestinationConfiguration
import io.airbyte.cdk.load.command.DestinationConfigurationFactory
import jakarta.inject.Singleton

/**
 * Typed, validated StarRocks destination configuration (post-parse view of [StarrocksSpecification]).
 *
 * - [port] (9030) = MySQL protocol for DDL and `SELECT current_version()`.
 * - [httpPort] (8030) = HTTP Stream Load (used by the load path, issues #9+).
 */
data class StarrocksConfiguration(
    val host: String,
    val port: Int,
    val httpPort: Int,
    val username: String,
    val password: String,
    val database: String,
    val ssl: Boolean,
    val enableJson: Boolean,
    val cdcSoftDelete: Boolean,
    val loadAsJson: Boolean,
) : DestinationConfiguration() {
    val resolvedDatabase: String = database.ifEmpty { Defaults.DATABASE_NAME }

    /** JDBC URL without a default schema — used for connectivity/`current_version()` checks. */
    val jdbcUrl: String =
        "jdbc:mysql://$host:$port/?useSSL=$ssl" + if (ssl) "&requireSSL=true" else ""

    object Defaults {
        const val DATABASE_NAME = "default"
    }
}

@Singleton
class StarrocksConfigurationFactory :
    DestinationConfigurationFactory<StarrocksSpecification, StarrocksConfiguration> {
    override fun makeWithoutExceptionHandling(pojo: StarrocksSpecification): StarrocksConfiguration =
        StarrocksConfiguration(
            host = pojo.host,
            port = pojo.port,
            httpPort = pojo.httpPort,
            username = pojo.username,
            password = pojo.password,
            database = pojo.database,
            ssl = pojo.ssl,
            enableJson = pojo.enableJson,
            cdcSoftDelete = CdcDeletionMode.isSoftDelete(pojo.cdcDeletionMode),
            loadAsJson = LoadFormat.isJson(pojo.loadFormat),
        )
}
