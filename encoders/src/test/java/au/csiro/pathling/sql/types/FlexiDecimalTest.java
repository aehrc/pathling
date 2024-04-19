/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

public class FlexiDecimalTest {
  
  @Test
  void testNormalize() {
    // Normalize big decimal with negative scale
    assertEquals(new BigDecimal("123000"), FlexiDecimal.normalize(new BigDecimal("1.23E+5")));
    assertEquals(new BigDecimal("99900000000000000000000000000000000000"),
        FlexiDecimal.normalize(new BigDecimal("9.99E+37")));
    // To many significant digits --> null
    assertNull(FlexiDecimal.normalize(new BigDecimal("1.0E+38")));

    // normalize decimals to reduced precision
    assertEquals(new BigDecimal("0.010000000000000000000000000000000000000"),
        FlexiDecimal.normalize(new BigDecimal("0.010000000000000000000000000000000000000000000")));

    assertEquals(FlexiDecimal.MAX_PRECISION,
        FlexiDecimal.normalize(new BigDecimal("0.010000000000000000000000000000000000000000000"))
            .precision());

    assertEquals(new BigDecimal("0.015555555555555555555555555555555555556"),
        FlexiDecimal.normalize(new BigDecimal(
            "0.01555555555555555555555555555555555555555555555555555555555555555555555555")));
  }
}
