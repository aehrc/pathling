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

package au.csiro.pathling.sql.misc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DecimalToLiteralTest {

  private DecimalToLiteral decimalToLiteral;

  @BeforeEach
  void setUp() {
    decimalToLiteral = new DecimalToLiteral();
  }

  @Test
  void testCallWithNullValue() {
    assertNull(decimalToLiteral.call(null, null));
  }

  @Test
  void testCallWithNullScale() {
    final BigDecimal value = new BigDecimal("123.456");
    assertEquals("123.456", decimalToLiteral.call(value, null));
  }

  @Test
  void testCallWithScale() {
    final BigDecimal value = new BigDecimal("123.456");
    assertEquals("123.45", decimalToLiteral.call(value, 2));
    assertEquals("123.4", decimalToLiteral.call(value, 1));
    assertEquals("123", decimalToLiteral.call(value, 0));
  }

  @Test
  void testCallWithZeroScale() {
    final BigDecimal value = new BigDecimal("123.456");
    assertEquals("123", decimalToLiteral.call(value, 0));
  }

  @Test
  void testCallWithNegativeScale() {
    final BigDecimal value = new BigDecimal("123.456");
    assertNull(decimalToLiteral.call(value, -1));
    assertNull(decimalToLiteral.call(value, -2));
  }

  @Test
  void testCallWithRounding() {
    final BigDecimal value = new BigDecimal("123.999");
    assertEquals("123.99", decimalToLiteral.call(value, 2));
    assertEquals("123.9", decimalToLiteral.call(value, 1));
    assertEquals("123", decimalToLiteral.call(value, 0));
  }
}
