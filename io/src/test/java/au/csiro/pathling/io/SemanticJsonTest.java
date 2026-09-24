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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Test;

/**
 * Pins the comparison the round trip is judged by, so that a round trip reported green is green for
 * a reason. A comparison that ignored the difference between a number and its text would pass a
 * serialiser that quoted every number.
 */
class SemanticJsonTest {

  @Test
  void ignoresObjectKeyOrder() {
    assertSame("{\"a\":1,\"b\":2}", "{\"b\":2,\"a\":1}");
  }

  @Test
  void treatsArrayOrderAsSignificant() {
    assertDiffers("{\"a\":[1,2]}", "{\"a\":[2,1]}");
  }

  @Test
  void comparesNumbersNumerically() {
    assertSame("{\"a\":1.50}", "{\"a\":1.5}");
    assertSame("{\"a\":1e2}", "{\"a\":100.0}");
    assertSame("{\"a\":0.0000001}", "{\"a\":1.0E-7}");
    assertDiffers("{\"a\":1.5}", "{\"a\":1.6}");
  }

  @Test
  void distinguishesANumberFromItsText() {
    assertDiffers("{\"a\":1.50}", "{\"a\":\"1.50\"}");
  }

  @Test
  void distinguishesAnAbsentKeyFromANullOne() {
    assertDiffers("{\"a\":1}", "{\"a\":1,\"b\":null}");
  }

  private static void assertSame(@Nonnull final String left, @Nonnull final String right) {
    assertTrue(
        SemanticJson.difference("root", SemanticJson.parse(left), SemanticJson.parse(right))
            .isEmpty(),
        left + " should compare equal to " + right);
  }

  private static void assertDiffers(@Nonnull final String left, @Nonnull final String right) {
    assertFalse(
        SemanticJson.difference("root", SemanticJson.parse(left), SemanticJson.parse(right))
            .isEmpty(),
        left + " should differ from " + right);
  }
}
