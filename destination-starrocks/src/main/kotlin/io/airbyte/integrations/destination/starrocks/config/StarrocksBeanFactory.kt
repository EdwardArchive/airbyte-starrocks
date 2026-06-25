/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.config

import io.airbyte.cdk.command.ConfigurationSpecificationSupplier
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.dataflow.config.model.AggregatePublishingConfig
import io.airbyte.cdk.load.table.BaseDirectLoadInitialStatusGatherer
import io.airbyte.cdk.load.table.DatabaseInitialStatusGatherer
import io.airbyte.cdk.load.table.DefaultTempTableNameGenerator
import io.airbyte.cdk.load.table.TempTableNameGenerator
import io.airbyte.cdk.load.table.directload.DirectLoadInitialStatus
import io.airbyte.integrations.destination.starrocks.client.StarrocksAirbyteClient
import io.airbyte.integrations.destination.starrocks.http.StreamLoadClient
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfiguration
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfigurationFactory
import io.airbyte.integrations.destination.starrocks.spec.StarrocksSpecification
import io.airbyte.integrations.destination.starrocks.sql.StarrocksSqlGenerator
import io.airbyte.integrations.destination.starrocks.version.StarrocksVersionGate
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

/**
 * Exposes the *concrete* [StarrocksConfiguration] as an injectable bean. The CDK's built-in factory
 * only produces the abstract `DestinationConfiguration`, so connectors that inject their concrete
 * type provide this narrowing bean (mirrors `ClickhouseBeanFactory` / `PostgresBeanFactory`).
 *
 * The bean is lazy, so the `spec` operation (which has no config) never triggers it.
 */
@Factory
class StarrocksBeanFactory {

    @Singleton
    fun starrocksConfiguration(
        configFactory: StarrocksConfigurationFactory,
        specSupplier: ConfigurationSpecificationSupplier<StarrocksSpecification>,
    ): StarrocksConfiguration = configFactory.makeWithoutExceptionHandling(specSupplier.get())

    @Singleton fun starrocksSqlGenerator(): StarrocksSqlGenerator = StarrocksSqlGenerator()

    /** Stream Load HTTP client (FE http_port, default 8030) used by the dataflow load path. */
    @Singleton
    fun streamLoadClient(config: StarrocksConfiguration): StreamLoadClient =
        StreamLoadClient(
            host = config.host,
            httpPort = config.httpPort,
            username = config.username,
            password = config.password,
            // NOTE: Stream Load stays HTTP regardless of `ssl`. StarRocks serves the HTTP/Stream Load
            // port (8030) over plain HTTP even when the MySQL-protocol port (9030) has TLS — so
            // wiring `useSsl = config.ssl` broke loads on SSL clusters. `ssl` is JDBC-only.
        )

    /** DDL + table metadata over the MySQL protocol (port 9030). */
    @Singleton
    fun starrocksAirbyteClient(
        config: StarrocksConfiguration,
        sqlGenerator: StarrocksSqlGenerator,
    ): StarrocksAirbyteClient = StarrocksAirbyteClient(config, sqlGenerator)

    /**
     * Version-detected Stream Load capabilities (compression, Merge Commit, …), resolved once at
     * write start from `SELECT current_version()`. Lets the load path opt into higher-version
     * features up to the 4.1.x ceiling. Lazy/`write`-only, so `spec` never hits the cluster.
     */
    @Singleton
    fun starrocksCapabilities(client: StarrocksAirbyteClient): StarrocksVersionGate.Capabilities =
        StarrocksVersionGate.capabilities(client.serverVersion())

    @Singleton
    fun tempTableNameGenerator(): TempTableNameGenerator = DefaultTempTableNameGenerator()

    @Singleton
    fun initialStatusGatherer(
        client: StarrocksAirbyteClient,
        catalog: DestinationCatalog,
    ): DatabaseInitialStatusGatherer<DirectLoadInitialStatus> =
        object : BaseDirectLoadInitialStatusGatherer(client, catalog) {}

    @Singleton
    fun aggregatePublishingConfig(): AggregatePublishingConfig =
        AggregatePublishingConfig(maxRecordsPerAgg = MAX_RECORDS_PER_AGGREGATE)

    companion object {
        // Records per Stream Load request (one CSV flush). Conservative default; tune via e2e.
        private const val MAX_RECORDS_PER_AGGREGATE = 50_000L
    }
}
