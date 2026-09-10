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

package au.csiro.pathling.fhirpath.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ConversionFactorTest {

  @Test
  void testInverseProducesExactResult() {
    assertEquals(
        BigDecimal.ONE, ConversionFactor.inverseOf(new BigDecimal(12)).apply(new BigDecimal(12)));
  }

  @Test
  void testFractionProducesExactResult() {
    assertEquals(
        new BigDecimal(4),
        ConversionFactor.ofFraction(new BigDecimal(2), new BigDecimal(3)).apply(new BigDecimal(6)));
  }

  /** A zero denominator is rejected, as it would make the conversion undefined. */
  @Test
  void testZeroDenominatorIsRejected() {
    final IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConversionFactor.ofFraction(BigDecimal.ONE, BigDecimal.ZERO));
    assertEquals("denominator cannot be zero", error.getMessage());
  }

  /**
   * A zero denominator is rejected regardless of its scale. Zero has infinitely many
   * representations in {@link BigDecimal} ("0", "0.0", "0E-10"), and only numerical comparison
   * detects them all.
   */
  @Test
  void testScaledZeroDenominatorIsRejected() {
    for (final String zero : new String[] {"0.0", "0.00", "0E-10", "-0.0"}) {
      final BigDecimal denominator = new BigDecimal(zero);
      final IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () -> ConversionFactor.ofFraction(BigDecimal.ONE, denominator),
              "denominator " + zero + " should be rejected");
      assertEquals("denominator cannot be zero", error.getMessage());
    }
  }

  /** The inverse of a scaled zero is also rejected, as it delegates the same check. */
  @Test
  void testInverseOfScaledZeroIsRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> ConversionFactor.inverseOf(new BigDecimal("0.0")));
  }
}
