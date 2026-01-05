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

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for ImportMode enum.
 *
 * @author John Grimes
 */
class ImportModeTest {

  @Test
  void overwriteModeHasCorrectCode() {
    // When
    final ImportMode mode = ImportMode.OVERWRITE;

    // Then
    assertThat(mode.getCode()).isEqualTo("overwrite");
  }

  @Test
  void mergeModeHasCorrectCode() {
    // When
    final ImportMode mode = ImportMode.MERGE;

    // Then
    assertThat(mode.getCode()).isEqualTo("merge");
  }

  @Test
  void fromCodeReturnsOverwriteMode() {
    // When
    final ImportMode mode = ImportMode.fromCode("overwrite");

    // Then
    assertThat(mode).isEqualTo(ImportMode.OVERWRITE);
  }

  @Test
  void fromCodeReturnsMergeMode() {
    // When
    final ImportMode mode = ImportMode.fromCode("merge");

    // Then
    assertThat(mode).isEqualTo(ImportMode.MERGE);
  }

  @Test
  void fromCodeThrowsExceptionForInvalidCode() {
    // When/Then
    assertThatThrownBy(() -> ImportMode.fromCode("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown import mode: invalid");
  }

  @Test
  void fromCodeThrowsExceptionForNullCode() {
    // When/Then
    assertThatThrownBy(() -> ImportMode.fromCode(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown import mode: null");
  }

  @Test
  void enumValuesReturnsAllModes() {
    // When
    final ImportMode[] modes = ImportMode.values();

    // Then
    assertThat(modes).hasSize(2).contains(ImportMode.OVERWRITE, ImportMode.MERGE);
  }

  @Test
  void enumValueOfReturnsCorrectMode() {
    // When
    final ImportMode overwrite = ImportMode.valueOf("OVERWRITE");
    final ImportMode merge = ImportMode.valueOf("MERGE");

    // Then
    assertThat(overwrite).isEqualTo(ImportMode.OVERWRITE);
    assertThat(merge).isEqualTo(ImportMode.MERGE);
  }
}
