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

import static au.csiro.pathling.schema.LayoutFields.NUMERIC_ANNOTATION;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.utilities.SystemDecimal;
import java.math.BigDecimal;
import org.apache.spark.sql.Column;
import org.junit.jupiter.api.Test;

/** Tests that the numeric annotation of a decimal is the decimal at the engine's type. */
class DecimalNumericKindTest {

  @Test
  void theAnnotationIsTheStoredTextCastSafelyToTheEnginesDecimal() {
    final Column value = col("value");
    assertEquals(
        value.try_cast(SystemDecimal.TYPE),
        DecimalNumericKind.INSTANCE.compute(NUMERIC_ANNOTATION, value));
  }

  @Test
  void aStoredAnnotationIsReadAtTheEnginesDecimal() {
    final Column resolved = col("resolved");
    assertEquals(DecimalNumericKind.decode(resolved), DecimalNumericKind.INSTANCE.read(resolved));
    assertEquals(resolved.try_cast(SystemDecimal.TYPE), DecimalNumericKind.decode(resolved));
  }

  @Test
  void aKnownNumberIsDecodedAsTheEnginesDecimalHoldsIt() {
    // Each number, and what the engine's System.Decimal holds for it, or null where it holds
    // nothing.
    final String[][] cases = {
      {"1", "1"},
      {"1.0", "1.0"},
      {"1.0000001", "1.000000"},
      {"1.0000005", "1.000001"},
      {"-1.0000005", "-1.000001"},
      {"0.1234567", "0.123457"},
      {"1E-7", "0.000000"},
      {"7.25E+4", "7.25E+4"},
      {"0E+40", "0E+40"},
      {"99999999999999999999999999999999.999999", "99999999999999999999999999999999.999999"},
      {"99999999999999999999999999999999.9999995", null},
      {"1.0E32", null},
      {"1.0E33", null}
    };
    for (final String[] c : cases) {
      final BigDecimal decoded = DecimalNumericKind.decodeValue(new BigDecimal(c[0]));
      if (c[1] == null) {
        assertNull(decoded, c[0]);
      } else {
        assertEquals(new BigDecimal(c[1]), decoded, c[0]);
      }
    }
  }

  @Test
  void noNumberDecodesToNoValue() {
    assertNull(DecimalNumericKind.decodeValue(null));
  }
}
