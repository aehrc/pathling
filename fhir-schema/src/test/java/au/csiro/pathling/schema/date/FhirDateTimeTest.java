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

package au.csiro.pathling.schema.date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

/** Tests the values FHIR allows that Java's own parser does not. */
class FhirDateTimeTest {

  @Test
  void aLeapSecondIsTakenAsTheSecondBeforeIt() {
    final FhirDateTime parsed = FhirDateTime.parse("2016-12-31T23:59:60Z");
    assertEquals(ChronoUnit.SECONDS, parsed.getPrecision());
    assertEquals(Instant.parse("2016-12-31T23:59:59Z"), parsed.getLowerBoundary());
    assertEquals(Instant.parse("2016-12-31T23:59:59Z"), parsed.getUpperBoundary());
  }

  @Test
  void aLeapSecondKeepsItsFraction() {
    final FhirDateTime parsed = FhirDateTime.parse("2016-12-31T23:59:60.5+10:00");
    assertEquals(ChronoUnit.NANOS, parsed.getPrecision());
    assertEquals(Instant.parse("2016-12-31T13:59:59.5Z"), parsed.getLowerBoundary());
  }

  @Test
  void aFractionBeyondTheNanosecondIsTruncated() {
    final FhirDateTime parsed = FhirDateTime.parse("2023-06-15T10:30:00.1234567891234Z");
    assertEquals(ChronoUnit.NANOS, parsed.getPrecision());
    assertEquals(Instant.parse("2023-06-15T10:30:00.123456789Z"), parsed.getLowerBoundary());
    assertEquals(parsed.getLowerBoundary(), parsed.getUpperBoundary());
  }

  @Test
  void aSecondBeyondTheLeapSecondIsRejected() {
    assertThrows(DateTimeParseException.class, () -> FhirDateTime.parse("2016-12-31T23:59:61Z"));
  }

  @Test
  void aDateThatDoesNotExistIsRejected() {
    assertThrows(DateTimeParseException.class, () -> FhirDateTime.parse("2023-02-30"));
    assertThrows(DateTimeParseException.class, () -> FhirDateTime.parse("2023-13"));
    assertThrows(DateTimeParseException.class, () -> FhirDateTime.parse("2023-06-15T25"));
  }
}
