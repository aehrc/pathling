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

import static au.csiro.pathling.schema.LayoutFields.END_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.START_ANNOTATION;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.spark.sql.Column;
import org.junit.jupiter.api.Test;

/**
 * Tests the columns the range annotations of a date are computed by. The bounds themselves are
 * tested where a Spark session evaluates them, in the modules that run one.
 */
class DateRangeKindTest {

  @Test
  void eachFieldIsComputedByItsOwnBound() {
    final Column value = col("value");
    assertEquals(
        DateRangeKind.INSTANCE.start(value),
        DateRangeKind.INSTANCE.compute(START_ANNOTATION, value));
    assertEquals(
        DateRangeKind.INSTANCE.end(value), DateRangeKind.INSTANCE.compute(END_ANNOTATION, value));
    assertNotEquals(DateRangeKind.INSTANCE.start(value), DateRangeKind.INSTANCE.end(value));
  }

  @Test
  void aTextThatMayNotBeADateHasBoundsOfItsOwn() {
    final Column value = col("value");
    assertNotEquals(DateRangeKind.INSTANCE.start(value), DateRangeKind.INSTANCE.startOrNull(value));
    assertNotEquals(DateRangeKind.INSTANCE.end(value), DateRangeKind.INSTANCE.endOrNull(value));
    assertNotEquals(
        DateRangeKind.INSTANCE.startOrNull(value), DateRangeKind.INSTANCE.endOrNull(value));
  }

  @Test
  void buildingTheColumnsParsesNoDate() {
    final long before = DateRangeKind.getParseCount();
    DateRangeKind.INSTANCE.compute(START_ANNOTATION, col("value"));
    DateRangeKind.INSTANCE.endOrNull(col("value"));
    assertEquals(before, DateRangeKind.getParseCount());
  }
}
