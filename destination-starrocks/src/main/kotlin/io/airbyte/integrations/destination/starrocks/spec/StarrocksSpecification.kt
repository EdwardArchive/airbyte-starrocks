/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.spec

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import io.airbyte.cdk.command.AIRBYTE_CLOUD_ENV
import io.airbyte.cdk.command.ConfigurationSpecification
import io.airbyte.cdk.load.spec.DestinationSpecificationExtension
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

/**
 * StarRocks destination connector configuration spec (OSS).
 *
 * Data plane = HTTP Stream Load (`http_port`, default 8030); control plane (DDL,
 * `SELECT current_version()`) = MySQL protocol (`port`, default 9030).
 * See STARROCKS-VERSION-COMPATIBILITY.md for the shared-data feature gating.
 */
@Singleton
@Requires(notEnv = [AIRBYTE_CLOUD_ENV])
class StarrocksSpecification : ConfigurationSpecification() {
    @get:JsonSchemaTitle("Host")
    @get:JsonPropertyDescription("FE host of the StarRocks cluster.")
    @get:JsonProperty("host")
    @get:JsonSchemaInject(json = """{"order": 0, "group": "connection"}""")
    val host: String = ""

    @get:JsonSchemaTitle("Query Port")
    @get:JsonPropertyDescription("MySQL-protocol port for queries/DDL. Default: 9030")
    @get:JsonProperty("port")
    @get:JsonSchemaInject(json = """{"order": 1, "default": 9030, "group": "connection"}""")
    val port: Int = 9030

    @get:JsonSchemaTitle("HTTP Port")
    @get:JsonPropertyDescription("FE HTTP port for Stream Load. Default: 8030")
    @get:JsonProperty("http_port")
    @get:JsonSchemaInject(json = """{"order": 2, "default": 8030, "group": "connection"}""")
    val httpPort: Int = 8030

    @get:JsonSchemaTitle("Username")
    @get:JsonPropertyDescription("Username to access the database.")
    @get:JsonProperty("username")
    @get:JsonSchemaInject(json = """{"order": 3, "default": "root", "group": "connection"}""")
    val username: String = "root"

    @get:JsonSchemaTitle("Password")
    @get:JsonPropertyDescription("Password associated with the username.")
    @get:JsonProperty("password")
    @get:JsonSchemaInject(json = """{"order": 4, "airbyte_secret": true, "group": "connection"}""")
    val password: String = ""

    @get:JsonSchemaTitle("Database")
    @get:JsonPropertyDescription("Name of the target database.")
    @get:JsonProperty("database")
    @get:JsonSchemaInject(json = """{"order": 5, "group": "connection"}""")
    val database: String = ""

    @get:JsonSchemaTitle("SSL")
    @get:JsonPropertyDescription("Use SSL for the MySQL-protocol connection.")
    @get:JsonProperty("ssl")
    @get:JsonSchemaInject(json = """{"order": 6, "default": false, "group": "connection"}""")
    val ssl: Boolean = false

    @get:JsonSchemaTitle("Enable JSON")
    @get:JsonPropertyDescription(
        "Store object fields using the StarRocks JSON type instead of a JSON-encoded string. " +
            "Useful for sources with rich JSON columns (e.g. Postgres jsonb).",
    )
    @get:JsonProperty("enable_json")
    @get:JsonSchemaInject(json = """{"order": 7, "default": false, "group": "connection"}""")
    val enableJson: Boolean = false

    @get:JsonSchemaTitle("CDC Deletion Mode")
    @get:JsonPropertyDescription(
        "How to handle CDC deletes for deduped streams. 'Hard delete' removes the row from the " +
            "destination. 'Soft delete' keeps the row and marks it via the _ab_cdc_deleted_at column " +
            "(query WHERE _ab_cdc_deleted_at IS NULL for live rows).",
    )
    @get:JsonProperty("cdc_deletion_mode")
    @get:JsonSchemaInject(
        json =
            """{"order": 8, "default": "Hard delete", "enum": ["Hard delete", "Soft delete"], "group": "connection"}""",
    )
    val cdcDeletionMode: String = CdcDeletionMode.HARD_DELETE

    @get:JsonSchemaTitle("Load Format")
    @get:JsonPropertyDescription(
        "Stream Load body format. CSV is compact and high-throughput (default). JSON avoids CSV " +
            "escaping edge cases (e.g. a literal \\N string being stored as NULL) and preserves large " +
            "integer/decimal precision, at some throughput cost.",
    )
    @get:JsonProperty("load_format")
    @get:JsonSchemaInject(
        json = """{"order": 9, "default": "CSV", "enum": ["CSV", "JSON"], "group": "connection"}""",
    )
    val loadFormat: String = LoadFormat.CSV

    @get:JsonSchemaTitle("SSL Mode")
    @get:JsonPropertyDescription(
        "Certificate verification for the SSL (JDBC) connection when SSL is enabled. 'required' " +
            "encrypts but does not verify the server certificate (use for self-signed certs); " +
            "'verify_ca'/'verify_identity' verify the certificate against the JVM trust store.",
    )
    @get:JsonProperty("ssl_mode")
    @get:JsonSchemaInject(
        json =
            """{"order": 10, "default": "required", "enum": ["required", "verify_ca", "verify_identity"], "group": "connection"}""",
    )
    val sslMode: String = SslMode.REQUIRED
}

object CdcDeletionMode {
    const val HARD_DELETE = "Hard delete"
    const val SOFT_DELETE = "Soft delete"

    /** Whether the configured mode is soft delete (retain rows with the tombstone). */
    fun isSoftDelete(mode: String): Boolean = mode.equals(SOFT_DELETE, ignoreCase = true)
}

object LoadFormat {
    const val CSV = "CSV"
    const val JSON = "JSON"

    fun isJson(format: String): Boolean = format.equals(JSON, ignoreCase = true)
}

object SslMode {
    const val REQUIRED = "required"
    const val VERIFY_CA = "verify_ca"
    const val VERIFY_IDENTITY = "verify_identity"

    /** Maps the spec value to a MySQL connector-j `sslMode` (used only when ssl is enabled). */
    fun toConnectorJ(mode: String): String =
        when (mode.lowercase()) {
            VERIFY_CA -> "VERIFY_CA"
            VERIFY_IDENTITY -> "VERIFY_IDENTITY"
            else -> "REQUIRED"
        }
}

@Singleton
class StarrocksSpecificationExtension : DestinationSpecificationExtension {
    override val supportedSyncModes =
        listOf(
            DestinationSyncMode.OVERWRITE,
            DestinationSyncMode.APPEND,
            DestinationSyncMode.APPEND_DEDUP,
        )
    override val supportsIncremental = true
    override val groups =
        listOf(
            DestinationSpecificationExtension.Group("connection", "Connection"),
        )
}
