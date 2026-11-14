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

package au.csiro.pathling.schema.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/** Tests the canonicalisation of quantities to the base units of UCUM, and conversion of units. */
class UcumUnitsTest {

  @Test
  void aValueIsCanonicalisedToTheBaseUnits() {
    final CanonicalQuantity canonical = UcumUnits.canonicalize(new BigDecimal("1500"), "mg");
    assertEquals(0, new BigDecimal("1.5").compareTo(canonical.getValue()));
    assertEquals("g", canonical.getCode());
  }

  @Test
  void aTemperatureIsCanonicalisedWithItsOffset() {
    final CanonicalQuantity canonical = UcumUnits.canonicalize(BigDecimal.ZERO, "Cel");
    assertEquals(0, new BigDecimal("273.15").compareTo(canonical.getValue()));
    assertEquals("K", canonical.getCode());
  }

  @Test
  void aValueWithoutUnitsHasTheCodeOfNoUnit() {
    final CanonicalQuantity canonical = UcumUnits.canonicalize(new BigDecimal("50"), "%");
    assertEquals(0, new BigDecimal("0.5").compareTo(canonical.getValue()));
    assertEquals(UcumUnits.NO_UNIT_CODE, canonical.getCode());
  }

  @Test
  void aCodeUcumCannotParseHasNoCanonicalForm() {
    assertNull(UcumUnits.canonicalize(BigDecimal.ONE, "not a unit"));
  }

  @Test
  void aValueIsConvertedByAFactor() {
    assertEquals(
        0, new BigDecimal("1.5").compareTo(UcumUnits.convert(new BigDecimal("1500"), "mg", "g")));
  }

  @Test
  void aTemperatureIsConvertedWithItsOffset() {
    assertEquals(
        0, new BigDecimal("273.15").compareTo(UcumUnits.convert(BigDecimal.ZERO, "Cel", "K")));
  }

  @Test
  void unitsOfDifferentDimensionsAreNotConverted() {
    assertNull(UcumUnits.convert(BigDecimal.ONE, "m", "s"));
  }

  @Test
  void aCodeUcumCannotParseIsNotConverted() {
    assertNull(UcumUnits.convert(BigDecimal.ONE, "not a unit", "m"));
  }

  @Test
  void nothingIsConvertedWithoutAValueOrUnits() {
    assertNull(UcumUnits.convert(null, "mg", "g"));
    assertNull(UcumUnits.convert(BigDecimal.ONE, null, "g"));
    assertNull(UcumUnits.convert(BigDecimal.ONE, "mg", null));
  }
}
