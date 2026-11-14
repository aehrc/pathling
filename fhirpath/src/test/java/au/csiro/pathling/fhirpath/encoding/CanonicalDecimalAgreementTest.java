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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import au.csiro.pathling.test.SpringBootUnitTest;
import java.math.BigDecimal;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that the canonical form of a stored quantity is computed from the value the engine's
 * System.Decimal holds for its text (owner, 2026-11-12): that the decoding of a known number agrees
 * with the decoding of the text in the plan, and that the canonical form computed from a text is
 * the one computed from that decoded value.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class CanonicalDecimalAgreementTest {

  private static final List<String> TEXTS =
      List.of(
          "1",
          "1.0",
          "1.0000001",
          "1.0000005",
          "-1.0000005",
          "0.1234567",
          " 2.5 ",
          "1E-7",
          "7.25E+4",
          "0.000001",
          "99999999999999999999999999999999.999999",
          "99999999999999999999999999999999.9999995",
          "1.0E32",
          "1.0E33",
          "1.0E40");

  @Autowired SparkSession spark;

  @Test
  void aKnownNumberDecodesAsItsTextDoesInThePlan() {
    final List<Row> rows =
        spark
            .createDataset(TEXTS, Encoders.STRING())
            .select(col("value"), DecimalNumericKind.decode(col("value")).alias("decoded"))
            .collectAsList();
    assertThat(rows).hasSize(TEXTS.size());
    for (final Row row : rows) {
      final BigDecimal expected =
          DecimalNumericKind.decodeValue(QuantityCanonicalKind.parseValue(row.getString(0)));
      final BigDecimal decoded = row.getDecimal(1);
      if (expected == null) {
        assertThat(decoded).as(row.getString(0)).isNull();
      } else {
        assertThat(decoded).as(row.getString(0)).isEqualByComparingTo(expected);
      }
    }
  }

  @Test
  void theCanonicalFormOfATextIsThatOfItsDecodedValue() {
    final Dataset<Row> quantities =
        spark
            .createDataset(TEXTS, Encoders.STRING())
            .select(
                struct(
                        col("value"),
                        lit(QuantityCanonicalKind.UCUM_SYSTEM).alias("system"),
                        lit("g").alias("code"))
                    .alias("quantity"));
    final Column quantity = col("quantity");
    final List<Row> rows =
        quantities
            .select(
                quantity.getField("value"),
                QuantityCanonicalKind.INSTANCE.compute(
                    QuantityEncoding.CANONICAL_CODE_COLUMN, quantity),
                QuantityCanonicalKind.INSTANCE.compute(
                    QuantityEncoding.CANONICAL_VALUE_EXACT_COLUMN, quantity))
            .collectAsList();
    for (final Row row : rows) {
      final CanonicalQuantity expected =
          QuantityCanonicalKind.toExact(
              QuantityCanonicalKind.canonicalOf(
                  DecimalNumericKind.decodeValue(
                      QuantityCanonicalKind.parseValue(row.getString(0))),
                  QuantityCanonicalKind.UCUM_SYSTEM,
                  "g"));
      assertThat(row.getString(1))
          .as(row.getString(0))
          .isEqualTo(expected == null ? null : expected.getCode());
      assertThat(row.get(2))
          .as(row.getString(0))
          .isEqualTo(QuantityCanonicalKind.toValueExactRow(expected));
    }
    // The digits beyond the scale of the System.Decimal do not take part.
    assertThat(exactValue(rows.get(2))).isEqualByComparingTo(exactValue(rows.get(0)));
  }

  /** The exact canonical value of a row, from its unscaled value and its scale. */
  private static BigDecimal exactValue(final Row row) {
    final Row exact = row.getStruct(2);
    return new BigDecimal(exact.getDecimal(0).toBigIntegerExact(), exact.getInt(1));
  }
}
