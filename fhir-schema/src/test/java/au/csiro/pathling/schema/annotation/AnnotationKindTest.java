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

package au.csiro.pathling.schema.annotation;

import static au.csiro.pathling.schema.LayoutFields.END_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.NUMERIC_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.START_ANNOTATION;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.schema.annotation.kinds.DateRangeKind;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.junit.jupiter.api.Test;

/** Tests what every annotation kind shares: its fields, its switch and how it is applied. */
class AnnotationKindTest {

  @Test
  void aKindIsCarriedInItsFieldsInTheirStoredOrder() {
    assertEquals(List.of(START_ANNOTATION, END_ANNOTATION), DateRangeKind.INSTANCE.getFields());
    assertEquals("DateRangeKind[__start, __end]", DateRangeKind.INSTANCE.toString());
  }

  @Test
  void aKindIsStoredWhereItsSwitchIsOn() {
    final AnnotationSwitch range = DateRangeKind.INSTANCE.getSwitch();
    assertSame(AnnotationSwitch.RANGE, range);
    assertEquals("range", range.toString());
    assertTrue(range.isOn(SchemaConfiguration.builder().build()));
    assertFalse(range.isOn(SchemaConfiguration.builder().enableRangeAnnotation(false).build()));
  }

  @Test
  void aFieldThatIsNotTheKindsIsRejected() {
    final Column value = col("value");
    final IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> DecimalNumericKind.INSTANCE.compute(START_ANNOTATION, value));
    assertEquals("Not an annotation of DecimalNumericKind[__numeric]: __start", error.getMessage());
  }

  @Test
  void aKindThatDoesNotReadFieldsComputesFromTheWholeValue() {
    final List<String> read = new ArrayList<>();
    final Column computed =
        DateRangeKind.INSTANCE.compute(
            START_ANNOTATION,
            col("value"),
            name -> {
              read.add(name);
              return col(name);
            });
    assertEquals(DateRangeKind.INSTANCE.compute(START_ANNOTATION, col("value")), computed);
    assertEquals(List.of(), read);
  }

  @Test
  void applyingAKindResolvesTheStoredFieldWithTheComputedOneAsFallback() {
    final List<Object> resolved = new ArrayList<>();
    final Column stored = col("stored");
    final Column applied =
        DecimalNumericKind.INSTANCE.apply(
            NUMERIC_ANNOTATION,
            col("value"),
            col("group"),
            (structure, field, fallback) -> {
              resolved.add(structure);
              resolved.add(field);
              resolved.add(fallback);
              return stored;
            });
    assertSame(stored, applied);
    assertEquals(
        List.of(
            col("group"),
            NUMERIC_ANNOTATION,
            DecimalNumericKind.INSTANCE.compute(NUMERIC_ANNOTATION, col("value"))),
        resolved);
  }

  @Test
  void aKindWithoutAReadTypeTakesTheAnnotationAsResolved() {
    final Column resolved = col("resolved");
    assertSame(resolved, DateRangeKind.INSTANCE.read(resolved));
  }
}
