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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.TerminologyConfiguration.TerminologyConfigValidator;
import au.csiro.pathling.validation.ValidationUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for the class-level validation of {@link TerminologyConfiguration} that governs the {@code
 * mode}/{@code local} combinations, plus the field-level constraints of {@link
 * LocalTerminologyConfiguration}.
 *
 * <p>Full-bean validation is exercised through {@link ValidationUtils}, which uses the same EL-free
 * message interpolator that {@code PathlingContext.build()} relies on at context-creation time.
 *
 * @author John Grimes
 */
class TerminologyConfigurationTest {

  private final TerminologyConfigValidator validator = new TerminologyConfigValidator();

  @Test
  void serverModeIsDefault() {
    // The default configuration must select server mode to preserve current behaviour.
    final TerminologyConfiguration config = TerminologyConfiguration.builder().build();

    assertEquals(TerminologyMode.SERVER, config.getMode());
    assertTrue(validator.isValid(config, null));
  }

  @Test
  void serverModeValidWithoutLocalBlock() {
    // Server mode does not require a local block.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder().mode(TerminologyMode.SERVER).build();

    assertTrue(validator.isValid(config, null));
  }

  @Test
  void localModeInvalidWithoutLocalBlock() {
    // Local mode with no local block cannot resolve a storage path, so it is invalid.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder().mode(TerminologyMode.LOCAL).build();

    assertFalse(validator.isValid(config, null));
  }

  @Test
  void localModeInvalidWithoutStoragePath() {
    // Local mode requires a storage path within the local block.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().build())
            .build();

    assertFalse(validator.isValid(config, null));
  }

  @Test
  void localModeInvalidWithBlankStoragePath() {
    // A blank storage path is treated as missing.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath("   ").build())
            .build();

    assertFalse(validator.isValid(config, null));
  }

  @Test
  void localModeValidWithStoragePath() {
    // Local mode with a storage path is valid.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath("/data/tx-store").build())
            .build();

    assertTrue(validator.isValid(config, null));
  }

  @Test
  void localModeValidWithoutDefaultSnomedEdition() {
    // The default SNOMED edition is optional, so its absence must not fail validation.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath("/data/tx-store").build())
            .build();

    assertTrue(validator.isValid(config, null));
    assertNull(config.getLocal().getDefaultSnomedEdition());
  }

  @Test
  void expansionCacheSizeDefaultsToOneHundred() {
    // A newly built local configuration uses a sensible default cache size.
    final LocalTerminologyConfiguration local = LocalTerminologyConfiguration.builder().build();

    assertEquals(100, local.getExpansionCacheSize());
  }

  @Test
  void fullValidationAcceptsServerModeDefaults() {
    // The default (server) configuration must pass full bean validation unchanged.
    final TerminologyConfiguration config = TerminologyConfiguration.builder().build();

    assertTrue(ValidationUtils.validate(config).isEmpty());
  }

  @Test
  void fullValidationRejectsLocalModeWithoutStoragePath() {
    // The class-level constraint must be discovered by the bean validator so that it fires at
    // context-creation time.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder().mode(TerminologyMode.LOCAL).build();

    assertFalse(ValidationUtils.validate(config).isEmpty());
  }

  @Test
  void fullValidationRejectsCacheSizeBelowOne() {
    // The expansion cache must hold at least one entry.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(
                LocalTerminologyConfiguration.builder()
                    .storagePath("/data/tx-store")
                    .expansionCacheSize(0)
                    .build())
            .build();

    assertFalse(ValidationUtils.validate(config).isEmpty());
  }

  @Test
  void fullValidationAcceptsValidLocalConfiguration() {
    // A well-formed local configuration must pass full bean validation.
    final TerminologyConfiguration config =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(
                LocalTerminologyConfiguration.builder()
                    .storagePath("/data/tx-store")
                    .defaultSnomedEdition("32506021000036107")
                    .expansionCacheSize(50)
                    .build())
            .build();

    assertTrue(ValidationUtils.validate(config).isEmpty());
  }
}
