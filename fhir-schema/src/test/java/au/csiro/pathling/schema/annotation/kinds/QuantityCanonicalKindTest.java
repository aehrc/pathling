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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Tests the exact value of the canonical annotation, which keeps at most 38 digits.
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
    assertNull(QuantityCanonicalKind.toExactRow(canonical("9".repeat(39))));
  }

  @Test
  void hasNoValueWhereRoundingCarriesBeyondTheIntegerDigits() {
    assertNull(QuantityCanonicalKind.toExactRow(canonical("9".repeat(38) + ".9")));
  }

  @Nonnull
  private static Row exactValue(@Nonnull final String value) {
    final Row exact = QuantityCanonicalKind.toExactRow(canonical(value));
    assertEquals("m", exact.getString(1));
    return exact.getStruct(0);
  }

  @Nonnull
  private static CanonicalQuantity canonical(@Nonnull final String value) {
    return new CanonicalQuantity(new BigDecimal(value), "m");
  }
}
