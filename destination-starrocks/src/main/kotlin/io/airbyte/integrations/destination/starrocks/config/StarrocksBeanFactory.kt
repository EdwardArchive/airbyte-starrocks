/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks.config

import io.airbyte.cdk.command.ConfigurationSpecificationSupplier
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfiguration
import io.airbyte.integrations.destination.starrocks.spec.StarrocksConfigurationFactory
import io.airbyte.integrations.destination.starrocks.spec.StarrocksSpecification
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
}
