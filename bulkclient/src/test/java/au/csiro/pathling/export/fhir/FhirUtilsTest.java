/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class FhirUtilsTest {


  @Test
  void testParseFhirInstantWithZoneZ() {
    assertEquals(FhirUtils.parseFhirInstant("2023-01-02T00:01:02.123Z"),
        Instant.parse("2023-01-02T00:01:02.123Z"));
  }

  @Test
  void testParseFhirInstantWithExplicitOffset() {
    assertEquals(FhirUtils.parseFhirInstant("2023-01-02T00:01:02.123+00:00"),
        Instant.parse("2023-01-02T00:01:02.123Z"));
  }

  @Test
  void testFormatFhirInstant() {
    // We always express it in UTC.
    assertEquals("2023-01-02T00:01:02.123Z",
        FhirUtils.formatFhirInstant(Instant.parse("2023-01-02T00:01:02.123Z")));
  }
}
