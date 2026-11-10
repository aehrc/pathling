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

package au.csiro.pathling.schema.annotation.kinds;

import static au.csiro.pathling.schema.LayoutFields.CANONICAL_CODE_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.CANONICAL_VALUE_EXACT_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.START_ANNOTATION;
import static au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind.CALENDAR_DURATION_SYSTEM;
import static au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind.EXACT_VALUE_TYPE;
import static au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind.UCUM_SYSTEM;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

/**
 * Tests the canonical form of a quantity, and the exact value of the canonical annotation, which
 * keeps at most 38 digits. The computed annotations themselves are tested where a Spark session
 * evaluates them, in the modules that run one.
 *
 * @author Piotr Szul
 */
class QuantityCanonicalKindTest {

  @Test
  void keepsValueThatFits() {
    final Row value = exactValue("1.25");
    assertEquals(new BigDecimal("125"), value.get(0));
    assertEquals(2, value.getInt(1));
  }

  @Test
  void dropsFractionalDigitsBeyondTheMaximum() {
    final Row value = exactValue("0." + "1".repeat(39));
    assertEquals(new BigDecimal("1".repeat(38)), value.get(0));
    assertEquals(38, value.getInt(1));
  }

  @Test
  void roundingThatCarriesStaysWithinTheMaximum() {
    // Rounding 39 nines to 38 digits carries into a new integer digit.
    final Row value = exactValue("0." + "9".repeat(39));
    assertEquals(new BigDecimal("1" + "0".repeat(37)), value.get(0));
    assertEquals(37, value.getInt(1));
  }

  @Test
  void roundingThatCarriesFromIntegerDigitsStaysWithinTheMaximum() {
    final Row value = exactValue("9." + "9".repeat(38));
    assertEquals(new BigDecimal("1" + "0".repeat(37)), value.get(0));
    assertEquals(36, value.getInt(1));
  }

  @Test
  void hasNoValueWhereIntegerDigitsAreTooMany() {
    assertNull(QuantityCanonicalKind.toExact(canonical("9".repeat(39))));
  }

  @Test
  void hasNoValueWhereRoundingCarriesBeyondTheIntegerDigits() {
    assertNull(QuantityCanonicalKind.toExact(canonical("9".repeat(38) + ".9")));
  }

  @Nonnull
  private static Row exactValue(@Nonnull final String value) {
    final CanonicalQuantity exact = QuantityCanonicalKind.toExact(canonical(value));
    assertEquals("m", exact.getCode());
    return QuantityCanonicalKind.toValueExactRow(exact);
  }

  @Test
  void theScaleIsNeverNegative() {
    // A value with trailing zeros before its decimal point is not given a negative scale.
    final Row value = exactValue("7.25E+4");
    assertEquals(new BigDecimal("72500"), value.get(0));
    assertEquals(0, value.getInt(1));
  }

  @Nonnull
  private static CanonicalQuantity canonical(@Nonnull final String value) {
    return new CanonicalQuantity(new BigDecimal(value), "m");
  }

  @Test
  void aUcumQuantityIsCanonicalised() {
    final CanonicalQuantity canonical =
        QuantityCanonicalKind.canonicalOf(new BigDecimal("1500"), UCUM_SYSTEM, "mg");
    assertEquals(0, new BigDecimal("1.5").compareTo(canonical.getValue()));
    assertEquals("g", canonical.getCode());
  }

  @Test
  void aDefiniteCalendarDurationIsCanonicalisedFromItsUcumEquivalent() {
    final CanonicalQuantity canonical =
        QuantityCanonicalKind.canonicalOf(
            new BigDecimal("2"), CALENDAR_DURATION_SYSTEM, "millisecond");
    assertEquals(0, new BigDecimal("0.002").compareTo(canonical.getValue()));
    assertEquals("s", canonical.getCode());
  }

  @Test
  void aQuantityOutsideUcumHasNoCanonicalForm() {
    // A calendar month has no fixed length.
    assertNull(
        QuantityCanonicalKind.canonicalOf(BigDecimal.ONE, CALENDAR_DURATION_SYSTEM, "month"));
    assertNull(QuantityCanonicalKind.canonicalOf(BigDecimal.ONE, "http://example.org", "mg"));
    assertNull(QuantityCanonicalKind.canonicalOf(BigDecimal.ONE, null, "mg"));
    assertNull(QuantityCanonicalKind.canonicalOf(null, UCUM_SYSTEM, "mg"));
  }

  @Test
  void aStoredValueIsParsedWithEveryDigit() {
    assertEquals(new BigDecimal("1.50"), QuantityCanonicalKind.parseValue(" 1.50 "));
    assertNull(QuantityCanonicalKind.parseValue("one"));
    assertNull(QuantityCanonicalKind.parseValue(null));
  }

  @Test
  void noCanonicalFormHasNoExactForm() {
    assertNull(QuantityCanonicalKind.toExact(null));
    assertNull(QuantityCanonicalKind.toValueExactRow(null));
  }

  @Test
  void aKnownQuantityIsCarriedAsConstants() {
    final CanonicalQuantity exact = QuantityCanonicalKind.toExact(canonical("1.25"));
    assertEquals(lit("m").cast(DataTypes.StringType), QuantityCanonicalKind.toCodeLiteral(exact));
    assertEquals(
        functions
            .named_struct(lit("value"), lit(new BigDecimal("125")), lit("scale"), lit(2))
            .cast(EXACT_VALUE_TYPE),
        QuantityCanonicalKind.toValueExactLiteral(exact));
  }

  @Test
  void aKnownQuantityWithoutACanonicalFormIsCarriedAsNullConstants() {
    assertEquals(lit(null).cast(DataTypes.StringType), QuantityCanonicalKind.toCodeLiteral(null));
    assertEquals(lit(null).cast(EXACT_VALUE_TYPE), QuantityCanonicalKind.toValueExactLiteral(null));
  }

  @Test
  void theAnnotationsAreComputedFromTheFieldsTheyRead() {
    final List<String> read = new ArrayList<>();
    final Column code =
        QuantityCanonicalKind.INSTANCE.compute(
            CANONICAL_CODE_ANNOTATION,
            col("quantity"),
            name -> {
              read.add(name);
              return col(name);
            });
    assertEquals(List.of("value", "system", "code"), read);
    assertNotEquals(
        code,
        QuantityCanonicalKind.INSTANCE.compute(
            CANONICAL_VALUE_EXACT_ANNOTATION, col("quantity"), name -> col(name)));
  }

  @Test
  void eachAnnotationOfAStructureIsItsOwnComputation() {
    final Column quantity = col("quantity");
    assertNotEquals(
        QuantityCanonicalKind.INSTANCE.compute(CANONICAL_CODE_ANNOTATION, quantity),
        QuantityCanonicalKind.INSTANCE.compute(CANONICAL_VALUE_EXACT_ANNOTATION, quantity));
    assertThrows(
        IllegalArgumentException.class,
        () -> QuantityCanonicalKind.INSTANCE.compute(START_ANNOTATION, quantity));
  }
}
