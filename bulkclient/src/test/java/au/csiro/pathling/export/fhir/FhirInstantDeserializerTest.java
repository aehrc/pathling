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

import com.google.gson.JsonArray;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class FhirInstantDeserializerTest {


  final FhirInstantDeserializer fhirInstantDeserializer = new FhirInstantDeserializer();

  final Instant testInstant = Instant.parse("2023-01-02T00:01:02.123Z");
  final long testInstantEpochMilli = testInstant.toEpochMilli();

  @Test
  void deserializeFromMillsecondsValue() {

    assertEquals(Instant.parse("2023-01-02T00:01:02.123Z"),
        fhirInstantDeserializer.deserialize(new JsonPrimitive(testInstantEpochMilli), Instant.class,
            null));
  }

  @Test
  void deserializeFromFHIRStringValueWithZoneZ() {
    assertEquals(Instant.parse("2023-01-02T00:01:02.123Z"),
        fhirInstantDeserializer.deserialize(new JsonPrimitive("2023-01-02T00:01:02.123Z"),
            Instant.class,
            null));
  }

  @Test
  void deserializeFromFHIRStringValueWithExplicitOffset() {
    assertEquals(Instant.parse("2023-01-02T00:01:02.123Z"),
        fhirInstantDeserializer.deserialize(new JsonPrimitive("2023-01-02T01:31:02.123+01:30"),
            Instant.class,
            null));
  }

  @Test
  void deserializeFromInvalidStringValue() {
    final JsonParseException ex = assertThrows(JsonParseException.class,
        () -> fhirInstantDeserializer.deserialize(new JsonPrimitive("1234"), Instant.class, null));
    assertEquals("Failed to parse Instant from string: \"1234\"", ex.getMessage());
  }

  @Test
  void deserializeFromInvalidPrimitive() {
    final JsonParseException ex = assertThrows(JsonParseException.class,
        () -> fhirInstantDeserializer.deserialize(new JsonPrimitive(true), Instant.class, null));
    assertEquals("Failed to parse Instant from: true", ex.getMessage());
  }

  @Test
  void deserializeFromNonPrimitive() {
    final JsonParseException ex = assertThrows(JsonParseException.class,
        () -> fhirInstantDeserializer.deserialize(new JsonArray(0), Instant.class, null));
    assertEquals("Failed to parse Instant from non-primitive: []", ex.getMessage());
  }

}
