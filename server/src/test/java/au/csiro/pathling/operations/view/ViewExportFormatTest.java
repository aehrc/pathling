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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ViewExportFormat}.
 *
 * @author John Grimes
 */
class ViewExportFormatTest {

  // -------------------------------------------------------------------------
  // fromString parsing tests
  // -------------------------------------------------------------------------

  @Test
  void parsesNdjsonCode() {
    assertThat(ViewExportFormat.fromString("ndjson")).isEqualTo(ViewExportFormat.NDJSON);
  }

  @Test
  void parsesNdjsonContentType() {
    assertThat(ViewExportFormat.fromString("application/x-ndjson"))
        .isEqualTo(ViewExportFormat.NDJSON);
  }

  @Test
  void parsesCsvCode() {
    assertThat(ViewExportFormat.fromString("csv")).isEqualTo(ViewExportFormat.CSV);
  }

  @Test
  void parsesCsvContentType() {
    assertThat(ViewExportFormat.fromString("text/csv")).isEqualTo(ViewExportFormat.CSV);
  }

  @Test
  void parsesParquetCode() {
    assertThat(ViewExportFormat.fromString("parquet")).isEqualTo(ViewExportFormat.PARQUET);
  }

  @Test
  void parsesParquetContentType() {
    assertThat(ViewExportFormat.fromString("application/vnd.apache.parquet"))
        .isEqualTo(ViewExportFormat.PARQUET);
  }

  @Test
  void acceptsMediaTypeWithParameters() {
    // A supported media type carrying parameters is treated as that format, not rejected.
    assertThat(ViewExportFormat.fromString("text/csv;charset=utf-8"))
        .isEqualTo(ViewExportFormat.CSV);
  }

  @Test
  void rejectsUnknownNamingValue() {
    // An explicit unsupported format is rejected with the unsupported value named.
    assertThatThrownBy(() -> ViewExportFormat.fromString("unknown"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("unknown");
  }

  @Test
  void rejectsJsonAsUnsupported() {
    // json is only RECOMMENDED by the spec and is not supported by the export operation.
    assertThatThrownBy(() -> ViewExportFormat.fromString("json"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("json");
  }

  @Test
  void defaultsToNdjsonForEmptyString() {
    assertThat(ViewExportFormat.fromString("")).isEqualTo(ViewExportFormat.NDJSON);
  }

  @Test
  void defaultsToNdjsonForNull() {
    assertThat(ViewExportFormat.fromString(null)).isEqualTo(ViewExportFormat.NDJSON);
  }

  // -------------------------------------------------------------------------
  // File extension tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonHasCorrectExtension() {
    assertThat(ViewExportFormat.NDJSON.getFileExtension()).isEqualTo(".ndjson");
  }

  @Test
  void csvHasCorrectExtension() {
    assertThat(ViewExportFormat.CSV.getFileExtension()).isEqualTo(".csv");
  }

  @Test
  void parquetHasCorrectExtension() {
    assertThat(ViewExportFormat.PARQUET.getFileExtension()).isEqualTo(".parquet");
  }

  // -------------------------------------------------------------------------
  // Content type tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonHasCorrectContentType() {
    assertThat(ViewExportFormat.NDJSON.getContentType()).isEqualTo("application/x-ndjson");
  }

  @Test
  void csvHasCorrectContentType() {
    assertThat(ViewExportFormat.CSV.getContentType()).isEqualTo("text/csv");
  }

  @Test
  void parquetHasCorrectContentType() {
    assertThat(ViewExportFormat.PARQUET.getContentType())
        .isEqualTo("application/vnd.apache.parquet");
  }
}
