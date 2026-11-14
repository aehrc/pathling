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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.fhirpath.FhirPathQuantity;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.stream.IntStream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests the Row encoding of a System quantity, and the decoding of the text of its value, which
 * keeps every digit of the text.
 *
 * @author Piotr Szul
 */
class QuantityEncodingTest {

  @ParameterizedTest(name = "{0} decodes to {1}")
  @CsvSource(
      value = {
        // A value keeps its own scale.
        "1.50, 1.50",
        "90, 90",
        "1E+3, 1E+3",
        // Digits beyond the sixth fractional digit, and beyond twenty-six integer digits, are kept.
        "0.1234567891, 0.1234567891",
        "0.0000002, 0.0000002",
        "123456789012345678901234567, 123456789012345678901234567",
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
  void valueBeyondSixFractionalDigitsIsKeptByTheStringFormAndASameUnitConversion() {
    final FhirPathQuantity quantity = FhirPathQuantity.parse("0.1234567891 'mg'");
    final FhirPathQuantity decoded = QuantityEncoding.decode(QuantityEncoding.encode(quantity));

    assertThat(decoded).isNotNull();
    assertThat(decoded.toString()).isEqualTo("0.1234567891 'mg'");
    assertThat(decoded.convertToUnit("mg"))
        .hasValueSatisfying(q -> assertThat(q.getValue()).isEqualByComparingTo("0.1234567891"));
  }

  @Test
  void encodedRowHasOneFieldPerFieldOfTheFixedShape() {
    assertThat(QuantityEncoding.encode(FhirPathQuantity.parse("4 days")).size())
        .isEqualTo(QuantityEncoding.dataType().size());
  }

  @Test
  void rowWithTheFixedShapeAndASchemaDecodes() {
    final FhirPathQuantity quantity = FhirPathQuantity.parse("1.50 'mg'");
    final Row encoded = QuantityEncoding.encode(quantity);
    final Row row =
        new GenericRowWithSchema(
            IntStream.range(0, encoded.size()).mapToObj(encoded::get).toArray(),
            QuantityEncoding.dataType());
    assertThat(QuantityEncoding.decode(row)).isEqualTo(quantity);
  }

  @Test
  void rowWithADifferentShapeIsRejected() {
    final StructType schema =
        new StructType().add("code", DataTypes.StringType).add("value", DataTypes.StringType);
    final Row row = new GenericRowWithSchema(new Object[] {"mg", "1.5"}, schema);
    assertThatThrownBy(() -> QuantityEncoding.decode(row))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected a System quantity")
        .hasMessageContaining(schema.simpleString());
  }

  @Test
  void nullRowDecodesToNull() {
    assertThat(QuantityEncoding.decode(null)).isNull();
  }
}
