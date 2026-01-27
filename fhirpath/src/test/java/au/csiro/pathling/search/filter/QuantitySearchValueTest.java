/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.search.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Tests for {@link QuantitySearchValue}. */
class QuantitySearchValueTest {

  // ========== Value-only parsing tests ==========

  @Test
  void parseValueOnly() {
    final QuantitySearchValue value = QuantitySearchValue.parse("5.4");

    assertEquals(SearchPrefix.EQ, value.getPrefix());
    assertEquals("5.4", value.getNumericValue());
    assertEquals(Optional.empty(), value.getSystem());
    assertEquals(Optional.empty(), value.getCode());
    assertTrue(value.isValueOnly());
  }

  @Test
  void parseValueOnlyWithPrefix() {
    final QuantitySearchValue value = QuantitySearchValue.parse("gt100");

    assertEquals(SearchPrefix.GT, value.getPrefix());
    assertEquals("100", value.getNumericValue());
    assertTrue(value.isValueOnly());
  }

  @Test
  void parseValueOnlyNegative() {
    final QuantitySearchValue value = QuantitySearchValue.parse("-5.5");

    assertEquals(SearchPrefix.EQ, value.getPrefix());
    assertEquals("-5.5", value.getNumericValue());
    assertTrue(value.isValueOnly());
  }

  // ========== System|code parsing tests ==========

  @Test
  void parseWithSystemAndCode() {
    final QuantitySearchValue value = QuantitySearchValue.parse("5.4|http://unitsofmeasure.org|mg");

    assertEquals(SearchPrefix.EQ, value.getPrefix());
    assertEquals("5.4", value.getNumericValue());
    assertEquals(Optional.of("http://unitsofmeasure.org"), value.getSystem());
    assertEquals(Optional.of("mg"), value.getCode());
    assertFalse(value.isValueOnly());
  }

  @Test
  void parseWithSystemAndCodeWithPrefix() {
    final QuantitySearchValue value =
        QuantitySearchValue.parse("le5.4|http://unitsofmeasure.org|mg");

    assertEquals(SearchPrefix.LE, value.getPrefix());
    assertEquals("5.4", value.getNumericValue());
    assertEquals(Optional.of("http://unitsofmeasure.org"), value.getSystem());
    assertEquals(Optional.of("mg"), value.getCode());
  }

  @Test
  void parseWithEmptySystemAndCode() {
    // ||mg means any system, code must be "mg"
    final QuantitySearchValue value = QuantitySearchValue.parse("5.4||mg");

    assertEquals(SearchPrefix.EQ, value.getPrefix());
    assertEquals("5.4", value.getNumericValue());
    assertEquals(Optional.empty(), value.getSystem()); // empty = no constraint
    assertEquals(Optional.of("mg"), value.getCode());
    assertFalse(value.isValueOnly());
  }

  @Test
  void parseWithEmptySystemAndEmptyCode() {
    // || means any system, any code (equivalent to value-only but with pipes)
    final QuantitySearchValue value = QuantitySearchValue.parse("5.4||");

    assertEquals(SearchPrefix.EQ, value.getPrefix());
    assertEquals("5.4", value.getNumericValue());
    assertEquals(Optional.empty(), value.getSystem());
    assertEquals(Optional.empty(), value.getCode());
    assertTrue(value.isValueOnly()); // effectively value-only
  }

  // ========== Prefix tests ==========

  @Test
  void parseAllPrefixes() {
    assertEquals(SearchPrefix.EQ, QuantitySearchValue.parse("eq5|sys|code").getPrefix());
    assertEquals(SearchPrefix.NE, QuantitySearchValue.parse("ne5|sys|code").getPrefix());
    assertEquals(SearchPrefix.GT, QuantitySearchValue.parse("gt5|sys|code").getPrefix());
    assertEquals(SearchPrefix.GE, QuantitySearchValue.parse("ge5|sys|code").getPrefix());
    assertEquals(SearchPrefix.LT, QuantitySearchValue.parse("lt5|sys|code").getPrefix());
    assertEquals(SearchPrefix.LE, QuantitySearchValue.parse("le5|sys|code").getPrefix());
  }

  // ========== Invalid format tests ==========

  @Test
  void parseInvalidFormatTwoParts() {
    assertThrows(IllegalArgumentException.class, () -> QuantitySearchValue.parse("5.4|system"));
  }

  @Test
  void parseInvalidFormatFourParts() {
    assertThrows(
        IllegalArgumentException.class, () -> QuantitySearchValue.parse("5.4|system|code|extra"));
  }

  @Test
  void parseInvalidFormatSystemWithoutCode() {
    // System without code is not supported
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> QuantitySearchValue.parse("5.4|http://unitsofmeasure.org|"));

    assertTrue(exception.getMessage().contains("System without code is not supported"));
  }

  // ========== toString tests ==========

  @Test
  void toStringValueOnly() {
    final QuantitySearchValue value = QuantitySearchValue.parse("gt5.4");
    final String str = value.toString();

    assertTrue(str.contains("prefix=GT"));
    assertTrue(str.contains("numericValue='5.4'"));
    assertFalse(str.contains("system="));
    assertFalse(str.contains("code="));
  }

  @Test
  void toStringWithSystemAndCode() {
    final QuantitySearchValue value = QuantitySearchValue.parse("5.4|http://ucum.org|mg");
    final String str = value.toString();

    assertTrue(str.contains("prefix=EQ"));
    assertTrue(str.contains("numericValue='5.4'"));
    assertTrue(str.contains("system='http://ucum.org'"));
    assertTrue(str.contains("code='mg'"));
  }

  @Test
  void toStringWithCodeOnly() {
    final QuantitySearchValue value = QuantitySearchValue.parse("5.4||mg");
    final String str = value.toString();

    assertFalse(str.contains("system=")); // system is empty, not shown
    assertTrue(str.contains("code='mg'"));
  }
}
