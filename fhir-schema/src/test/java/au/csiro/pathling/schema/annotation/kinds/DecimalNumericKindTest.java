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

import au.csiro.pathling.utilities.SystemDecimal;
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
}
