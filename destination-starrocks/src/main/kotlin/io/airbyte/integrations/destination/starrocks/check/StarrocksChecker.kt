/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.check

import io.airbyte.cdk.load.check.DestinationChecker
import io.airbyte.integrations.destination.starrocks.http.StreamLoadClient
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfiguration
import io.airbyte.integrations.destination.starrocks.sql.quoteIdent
import io.airbyte.integrations.destination.starrocks.version.StarrocksVersionGate
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID

/**
 * `check` for StarRocks. Validates BOTH planes the connector uses, so that a passing check actually
 * implies a working sync:
 * 1. Control plane (MySQL protocol, `port` 9030): connect, read `current_version()`, apply the
 *    connector's feature gating ([StarrocksVersionGate]).
 * 2. Data plane (HTTP Stream Load, `http_port` 8030): create a throwaway table, Stream Load one row,
 *    and drop it — exercising http_port reachability, Basic-auth, and INSERT privilege. Previously
 *    `check` only opened JDBC, so it could pass while every write failed (issue #44).
 *
 * CDK 1.0.13 [DestinationChecker] is non-parameterized: the config is injected and `check()` takes
 * no arguments. Any thrown exception becomes the FAILED connection-status message.
 */
@Singleton
class StarrocksChecker(
    private val config: StarrocksConfiguration,
) : DestinationChecker {

    override fun check() {
        DriverManager.getConnection(config.jdbcUrl, config.username, config.password).use { conn ->
            val version =
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT current_version()").use { rs ->
                        require(rs.next()) {
                            "StarRocks check failed: current_version() returned no rows"
                        }
                        rs.getString(1)
                    }
                }
            require(!version.isNullOrBlank()) { "StarRocks check failed: empty version string" }
            StarrocksVersionGate.validate(version)

            // Fail fast on an unsupported compression combo rather than silently sending an
            // uncompressed/garbled body at write time (StarRocks compresses JSON bodies only, >= 3.3.2).
            if (config.compressGzip) {
                require(config.loadAsJson) {
                    "StarRocks check failed: load_compression=gzip requires the JSON load format — " +
                        "StarRocks does not decompress CSV Stream Load bodies. Set load_format=JSON or " +
                        "load_compression=none."
                }
                require(StarrocksVersionGate.capabilities(version).compression) {
                    "StarRocks check failed: load_compression=gzip requires StarRocks >= " +
                        "${StarrocksVersionGate.MIN_COMPRESSION}; detected $version. Set load_compression=none."
                }
            }

            validateStreamLoad(conn)
        }
    }

    /** Round-trips one row through Stream Load into a throwaway table, then drops it. */
    private fun validateStreamLoad(conn: Connection) {
        val database = config.resolvedDatabase
        val table = "_airbyte_check_${UUID.randomUUID().toString().replace("-", "")}"
        val qualified = "${quoteIdent(database)}.${quoteIdent(table)}"

        conn.createStatement().use { stmt ->
            stmt.execute("CREATE DATABASE IF NOT EXISTS ${quoteIdent(database)}")
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS $qualified (${quoteIdent("id")} INT NOT NULL) " +
                    "DUPLICATE KEY(${quoteIdent("id")}) DISTRIBUTED BY HASH(${quoteIdent("id")}) " +
                    "BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")",
            )
        }
        try {
            val response =
                // Stream Load stays HTTP regardless of `ssl` (StarRocks' http_port is plain HTTP even
                // on TLS-enabled clusters; `ssl` is for the JDBC control plane only).
                StreamLoadClient(config.host, config.httpPort, config.username, config.password)
                    .streamLoad(
                        database = database,
                        table = table,
                        label = "airbyte-check-${UUID.randomUUID()}",
                        headers = mapOf("format" to "CSV", "column_separator" to ",", "columns" to "id"),
                        body = "1\n".toByteArray(),
                    )
            require(response.isSuccess) {
                "StarRocks check failed: Stream Load probe to $qualified did not succeed " +
                    "(status=${response.status}, message=${response.message}). Verify http_port " +
                    "(${config.httpPort}) reachability, credentials, and INSERT privilege."
            }
        } finally {
            conn.createStatement().use { it.execute("DROP TABLE IF EXISTS $qualified") }
        }
    }
}
