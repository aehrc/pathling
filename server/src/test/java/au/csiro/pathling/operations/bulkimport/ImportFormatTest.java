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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for ImportFormat enum.
 *
 * @author John Grimes
 */
class ImportFormatTest {

  // Tests for fromCode() method - NDJSON variations.

  @ParameterizedTest
  @DisplayName("fromCode should return NDJSON for valid NDJSON MIME type")
  @ValueSource(strings = {"application/fhir+ndjson", "APPLICATION/FHIR+NDJSON"})
  void fromCode_shouldReturnNdjsonForValidNdjsonMimeTypes(final String code) {
    assertThat(ImportFormat.fromCode(code)).isEqualTo(ImportFormat.NDJSON);
  }

  // Tests for fromCode() method - Parquet variations.

  @ParameterizedTest
  @DisplayName("fromCode should return PARQUET for all valid Parquet MIME type variants")
  @ValueSource(
      strings = {
        "application/vnd.apache.parquet",
        "APPLICATION/VND.APACHE.PARQUET",
      })
  void fromCode_shouldReturnParquetForValidParquetMimeTypes(final String code) {
    assertThat(ImportFormat.fromCode(code)).isEqualTo(ImportFormat.PARQUET);
  }

  // Tests for fromCode() method - invalid inputs.

  @ParameterizedTest
  @DisplayName("fromCode should throw exception for invalid format strings")
  @ValueSource(
      strings = {
        "application/xml",
        "application/json",
        "csv",
        "xml",
        "invalid",
        "ndjson",
        "parquet"
      })
  void fromCode_shouldThrowExceptionForInvalidFormats(final String code) {
    assertThatThrownBy(() -> ImportFormat.fromCode(code))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported format: " + code);
  }

  // Tests for getCode() method.

  @Test
  @DisplayName("getCode should return correct MIME type for NDJSON")
  void getCode_shouldReturnCorrectMimeTypeForNdjson() {
    assertThat(ImportFormat.NDJSON.getCode()).isEqualTo("application/fhir+ndjson");
  }

  @Test
  @DisplayName("getCode should return correct MIME type for PARQUET")
  void getCode_shouldReturnCorrectMimeTypeForParquet() {
    assertThat(ImportFormat.PARQUET.getCode()).isEqualTo("application/vnd.apache.parquet");
  }

  // Tests for getExtension() method.

  @Test
  @DisplayName("getExtension should return ndjson for NDJSON format")
  void getExtension_shouldReturnNdjsonForNdjsonFormat() {
    assertThat(ImportFormat.NDJSON.getExtension()).isEqualTo("ndjson");
  }

  @Test
  @DisplayName("getExtension should return parquet for PARQUET format")
  void getExtension_shouldReturnParquetForParquetFormat() {
    assertThat(ImportFormat.PARQUET.getExtension()).isEqualTo("parquet");
  }
}
