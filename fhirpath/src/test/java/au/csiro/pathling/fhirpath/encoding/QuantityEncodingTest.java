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

package au.csiro.pathling.fhirpath.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.fhirpath.FhirPathQuantity;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests the Row encoding of a System quantity, and the decoding of the text of its value, which
 * reproduces the decoding of a decimal to {@code DECIMAL(32,6)} that the engine applies to a
 * column.
 *
 * @author Piotr Szul
 */
class QuantityEncodingTest {

  @ParameterizedTest(name = "{0} decodes to {1}")
  @CsvSource(
      value = {
        // A value with at most six fractional digits keeps its own scale.
        "1.50, 1.50",
        "90, 90",
        "1E+3, 1E+3",
        // More digits are rounded half up to six.
        "0.0000002, 0.000000",
        "0.0000005, 0.000001",
        "1.23456789, 1.234568",
        // Twenty-six integer digits fit, and twenty-seven do not.
        "12345678901234567890123456, 12345678901234567890123456",
        "123456789012345678901234567, NULL",
        // Text that is not a decimal, and no text, decode to null.
        "abc, NULL",
        "NULL, NULL"
      },
      nullValues = "NULL")
  void decodesTheTextOfAValue(@Nullable final String text, @Nullable final String expected) {
    assertThat(QuantityEncoding.decodeValue(text))
        .isEqualTo(expected == null ? null : new BigDecimal(expected));
  }

  @Test
  void encodedQuantityDecodesToTheSameQuantity() {
    final FhirPathQuantity quantity = FhirPathQuantity.parse("1.50 'mg'");
    assertThat(QuantityEncoding.decode(QuantityEncoding.encode(quantity))).isEqualTo(quantity);
  }

  @Test
  void encodedRowHasOneFieldPerFieldOfTheFixedShape() {
    assertThat(QuantityEncoding.encode(FhirPathQuantity.parse("4 days")).size())
        .isEqualTo(QuantityEncoding.dataType().size());
  }

  @Test
  void nullRowDecodesToNull() {
    assertThat(QuantityEncoding.decode(null)).isNull();
  }
}
