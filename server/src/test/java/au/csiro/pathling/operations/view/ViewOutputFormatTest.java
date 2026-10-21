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
 * Unit tests for {@link ViewOutputFormat}.
 *
 * @author John Grimes
 */
class ViewOutputFormatTest {

  // -------------------------------------------------------------------------
  // fromStringStrict parsing tests (used for the explicit _format parameter)
  // -------------------------------------------------------------------------

  @Test
  void fromStringStrictParsesSupportedCodes() {
    assertThat(ViewOutputFormat.fromStringStrict("ndjson")).isEqualTo(ViewOutputFormat.NDJSON);
    assertThat(ViewOutputFormat.fromStringStrict("csv")).isEqualTo(ViewOutputFormat.CSV);
    assertThat(ViewOutputFormat.fromStringStrict("json")).isEqualTo(ViewOutputFormat.JSON);
  }

  @Test
  void fromStringStrictParsesSupportedContentTypes() {
    assertThat(ViewOutputFormat.fromStringStrict("application/x-ndjson"))
        .isEqualTo(ViewOutputFormat.NDJSON);
    assertThat(ViewOutputFormat.fromStringStrict("text/csv")).isEqualTo(ViewOutputFormat.CSV);
    assertThat(ViewOutputFormat.fromStringStrict("application/json"))
        .isEqualTo(ViewOutputFormat.JSON);
  }

  @Test
  void fromStringStrictDefaultsToNdjsonForNull() {
    assertThat(ViewOutputFormat.fromStringStrict(null)).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromStringStrictDefaultsToNdjsonForBlank() {
    assertThat(ViewOutputFormat.fromStringStrict("   ")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromStringStrictAcceptsMediaTypeWithParameters() {
    // A supported media type carrying parameters is treated as that format, not rejected.
    assertThat(ViewOutputFormat.fromStringStrict("text/csv;charset=utf-8"))
        .isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromStringStrictRejectsUnknownNamingValue() {
    // An explicit unsupported format is rejected with the unsupported value named.
    assertThatThrownBy(() -> ViewOutputFormat.fromStringStrict("parquet"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("parquet");
  }

  // -------------------------------------------------------------------------
  // Content type tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonHasCorrectContentType() {
    assertThat(ViewOutputFormat.NDJSON.getContentType()).isEqualTo("application/x-ndjson");
  }

  @Test
  void csvHasCorrectContentType() {
    assertThat(ViewOutputFormat.CSV.getContentType()).isEqualTo("text/csv");
  }

  @Test
  void jsonHasCorrectContentType() {
    assertThat(ViewOutputFormat.JSON.getContentType()).isEqualTo("application/json");
  }
}
