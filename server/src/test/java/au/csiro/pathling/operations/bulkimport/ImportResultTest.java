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

import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ImportResult record class.
 *
 * @author John Grimes
 */
class ImportResultTest {

  @Test
  void canCreateImportResultWithOwnerId() {
    // Given
    final String ownerId = "user123";

    // When
    final ImportResult result = new ImportResult(Optional.of(ownerId));

    // Then
    assertThat(result.ownerId()).isPresent();
    assertThat(result.ownerId()).contains(ownerId);
  }

  @Test
  void canCreateImportResultWithoutOwnerId() {
    // When
    final ImportResult result = new ImportResult(Optional.empty());

    // Then
    assertThat(result.ownerId()).isEmpty();
  }

  @Test
  void recordEqualityBasedOnOwnerId() {
    // Given
    final ImportResult result1 = new ImportResult(Optional.of("user123"));
    final ImportResult result2 = new ImportResult(Optional.of("user123"));
    final ImportResult result3 = new ImportResult(Optional.of("user456"));

    // When/Then
    assertThat(result1).isEqualTo(result2);
    assertThat(result1).isNotEqualTo(result3);
  }

  @Test
  void recordEqualityWithEmptyOwner() {
    // Given
    final ImportResult result1 = new ImportResult(Optional.empty());
    final ImportResult result2 = new ImportResult(Optional.empty());

    // When/Then
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  void recordHashCodeConsistency() {
    // Given
    final ImportResult result1 = new ImportResult(Optional.of("user123"));
    final ImportResult result2 = new ImportResult(Optional.of("user123"));

    // When/Then
    assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
  }

  @Test
  void recordToStringIncludesOwnerId() {
    // Given
    final ImportResult result = new ImportResult(Optional.of("user123"));

    // When
    final String toString = result.toString();

    // Then
    assertThat(toString).contains("ImportResult");
    assertThat(toString).contains("user123");
  }

  @Test
  void recordToStringWithEmptyOwnerId() {
    // Given
    final ImportResult result = new ImportResult(Optional.empty());

    // When
    final String toString = result.toString();

    // Then
    assertThat(toString).contains("ImportResult");
  }
}
