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

package au.csiro.pathling.operations.bulkexport;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for ExportOutputFormat enum.
 *
 * @author John Grimes
 */
class ExportOutputFormatTest {

  // Tests for fromParam() method - NDJSON variations.

  @ParameterizedTest
  @DisplayName("fromParam should return NDJSON for all valid NDJSON MIME type variants")
  @ValueSource(
      strings = {
        "application/fhir+ndjson",
        "application/ndjson",
        "ndjson",
        "APPLICATION/FHIR+NDJSON",
        "APPLICATION/NDJSON",
        "NDJSON"
      })
  void fromParam_shouldReturnNdjsonForValidNdjsonMimeTypes(final String param) {
    assertThat(ExportOutputFormat.fromParam(param)).isEqualTo(ExportOutputFormat.NDJSON);
  }

  // Tests for fromParam() method - Parquet variations.

  @ParameterizedTest
  @DisplayName("fromParam should return PARQUET for all valid Parquet MIME type variants")
  @ValueSource(
      strings = {
        "application/vnd.apache.parquet",
        "parquet",
        "APPLICATION/VND.APACHE.PARQUET",
        "PARQUET"
      })
  void fromParam_shouldReturnParquetForValidParquetMimeTypes(final String param) {
    assertThat(ExportOutputFormat.fromParam(param)).isEqualTo(ExportOutputFormat.PARQUET);
  }

  // Tests for fromParam() method - null and invalid inputs.

  @ParameterizedTest
  @DisplayName("fromParam should return NDJSON as default when param is null")
  @NullSource
  void fromParam_shouldReturnNdjsonWhenParamIsNull(final String param) {
    assertThat(ExportOutputFormat.fromParam(param)).isEqualTo(ExportOutputFormat.NDJSON);
  }

  @ParameterizedTest
  @DisplayName("fromParam should return null for invalid format strings")
  @ValueSource(
      strings = {
        "application/xml",
        "application/json",
        "csv",
        "xml",
        "invalid",
        "delta",
        "application/x-pathling-delta+parquet"
      })
  void fromParam_shouldReturnNullForInvalidFormats(final String param) {
    assertThat(ExportOutputFormat.fromParam(param)).isNull();
  }

  // Tests for asParam() method.

  @ParameterizedTest
  @DisplayName("asParam should return correct string representation for each format")
  @CsvSource({"NDJSON,ndjson", "PARQUET,parquet"})
  void asParam_shouldReturnCorrectStringRepresentation(
      final ExportOutputFormat format, final String expected) {
    assertThat(ExportOutputFormat.asParam(format)).isEqualTo(expected);
  }

  // Tests for getMimeType() method.

  @Test
  @DisplayName("getMimeType should return correct MIME type for NDJSON")
  void getMimeType_shouldReturnCorrectMimeTypeForNdjson() {
    assertThat(ExportOutputFormat.NDJSON.getMimeType()).isEqualTo("application/fhir+ndjson");
  }

  @Test
  @DisplayName("getMimeType should return correct MIME type for PARQUET")
  void getMimeType_shouldReturnCorrectMimeTypeForParquet() {
    assertThat(ExportOutputFormat.PARQUET.getMimeType())
        .isEqualTo("application/vnd.apache.parquet");
  }
}
