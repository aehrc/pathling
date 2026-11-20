/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Tests for the binding of {@link StorageConfiguration} within the server configuration.
 *
 * @author John Grimes
 */
class StorageConfigurationTest {

  @Configuration
  @EnableConfigurationProperties(ServerConfiguration.class)
  static class BindingConfiguration {}

  // The pathling.storage.schemaAutoMerge setting was retired, because the Parquet on FHIR layout
  // widens tables to fit the data. A configuration written for an earlier release that still sets
  // it must continue to bind and validate, so that upgrading does not break startup.
  @Test
  void bindsConfigurationThatSetsTheRetiredSchemaAutoMergeSetting() {
    new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(BindingConfiguration.class)
        .withPropertyValues(
            "pathling.storage.schemaAutoMerge=true", "pathling.storage.databaseName=legacy")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(ServerConfiguration.class).getStorage().getDatabaseName())
                  .isEqualTo("legacy");
            });
  }
}
