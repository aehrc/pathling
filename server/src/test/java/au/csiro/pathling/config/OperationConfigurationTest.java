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

/**
 * Tests for {@link OperationConfiguration}.
 *
 * @author John Grimes
 */
class OperationConfigurationTest {

  @Test
  void allOperationsEnabledByDefault() {
    // When: Creating a new OperationConfiguration with defaults.
    final OperationConfiguration config = new OperationConfiguration();

    // Then: All operations should be enabled by default.
    assertThat(config.isCreateEnabled()).isTrue();
    assertThat(config.isReadEnabled()).isTrue();
    assertThat(config.isUpdateEnabled()).isTrue();
    assertThat(config.isDeleteEnabled()).isTrue();
    assertThat(config.isSearchEnabled()).isTrue();
    assertThat(config.isBatchEnabled()).isTrue();
    assertThat(config.isExportEnabled()).isTrue();
    assertThat(config.isPatientExportEnabled()).isTrue();
    assertThat(config.isGroupExportEnabled()).isTrue();
    assertThat(config.isImportEnabled()).isTrue();
    assertThat(config.isImportPnpEnabled()).isTrue();
    assertThat(config.isViewDefinitionRunEnabled()).isTrue();
    assertThat(config.isViewDefinitionInstanceRunEnabled()).isTrue();
    assertThat(config.isViewDefinitionExportEnabled()).isTrue();
    assertThat(config.isBulkSubmitEnabled()).isTrue();
  }

  @Test
  void isAnyExportEnabledReturnsTrueWhenSystemExportEnabled() {
    // Given: A configuration with only system export enabled.
    final OperationConfiguration config = new OperationConfiguration();
    config.setPatientExportEnabled(false);
    config.setGroupExportEnabled(false);

    // Then: isAnyExportEnabled should return true.
    assertThat(config.isAnyExportEnabled()).isTrue();
  }

  @Test
  void isAnyExportEnabledReturnsTrueWhenPatientExportEnabled() {
    // Given: A configuration with only patient export enabled.
    final OperationConfiguration config = new OperationConfiguration();
    config.setExportEnabled(false);
    config.setGroupExportEnabled(false);

    // Then: isAnyExportEnabled should return true.
    assertThat(config.isAnyExportEnabled()).isTrue();
  }

  @Test
  void isAnyExportEnabledReturnsTrueWhenGroupExportEnabled() {
    // Given: A configuration with only group export enabled.
    final OperationConfiguration config = new OperationConfiguration();
    config.setExportEnabled(false);
    config.setPatientExportEnabled(false);

    // Then: isAnyExportEnabled should return true.
    assertThat(config.isAnyExportEnabled()).isTrue();
  }

  @Test
  void isAnyExportEnabledReturnsFalseWhenAllExportDisabled() {
    // Given: A configuration with all export operations disabled.
    final OperationConfiguration config = new OperationConfiguration();
    config.setExportEnabled(false);
    config.setPatientExportEnabled(false);
    config.setGroupExportEnabled(false);

    // Then: isAnyExportEnabled should return false.
    assertThat(config.isAnyExportEnabled()).isFalse();
  }

  @Test
  void operationsCanBeDisabledIndividually() {
    // When: Creating a configuration and disabling specific operations.
    final OperationConfiguration config = new OperationConfiguration();
    config.setCreateEnabled(false);
    config.setImportEnabled(false);
    config.setViewDefinitionRunEnabled(false);

    // Then: Only the disabled operations should be false.
    assertThat(config.isCreateEnabled()).isFalse();
    assertThat(config.isImportEnabled()).isFalse();
    assertThat(config.isViewDefinitionRunEnabled()).isFalse();

    // And: Other operations should remain enabled.
    assertThat(config.isReadEnabled()).isTrue();
    assertThat(config.isUpdateEnabled()).isTrue();
    assertThat(config.isDeleteEnabled()).isTrue();
    assertThat(config.isSearchEnabled()).isTrue();
    assertThat(config.isBatchEnabled()).isTrue();
    assertThat(config.isExportEnabled()).isTrue();
  }
}
