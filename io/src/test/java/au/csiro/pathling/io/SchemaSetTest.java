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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Tests the building, parsing and checking of a schema set (T005, research R10). */
class SchemaSetTest {

  @Nonnull
  private static final StructType PATIENT =
      StructType.fromDDL("resourceType STRING, id STRING, name ARRAY<STRUCT<family: STRING>>");

  @Nonnull
  private static final StructType OBSERVATION =
      StructType.fromDDL("resourceType STRING, id STRING, status STRING");

  private static void assertRefusedNaming(
      @Nonnull final Executable call, @Nonnull final String name) {
    final IllegalArgumentException error = assertThrows(IllegalArgumentException.class, call);
    assertTrue(error.getMessage().contains(name), error.getMessage());
  }

  @Test
  void buildsFromMapAndLooksUpByType() {
    final SchemaSet set = SchemaSet.of(Map.of("Patient", PATIENT, "Observation", OBSERVATION));

    assertEquals(Optional.of(PATIENT), set.schema("Patient"));
    assertEquals(Optional.of(OBSERVATION), set.schema("Observation"));
    assertEquals(Optional.empty(), set.schema("Condition"));
    assertEquals(Map.of("Patient", PATIENT, "Observation", OBSERVATION), set.schemas());
  }

  @Test
  void parsesSchemaSetFileContent() {
    final String content =
        "{\"Patient\":" + PATIENT.json() + ",\"Observation\":" + OBSERVATION.json() + "}";

    final SchemaSet set = SchemaSet.parse(content);

    assertEquals(Map.of("Patient", PATIENT, "Observation", OBSERVATION), set.schemas());
  }

  @Test
  void refusesTypeNamedMoreThanOnceNamingIt() {
    final String content =
        "{\"Patient\":" + PATIENT.json() + ",\"Patient\":" + PATIENT.json() + "}";

    assertRefusedNaming(() -> SchemaSet.parse(content), "Patient more than once");
  }

  @Test
  void refusesUnknownResourceTypeNamingKey() {
    final SchemaSet set = SchemaSet.of(Map.of("Patient", PATIENT, "NotAType", OBSERVATION));

    assertRefusedNaming(() -> set.requireResourceTypes(TransformFixtures.DEFINITIONS), "NotAType");
  }

  @Test
  void acceptsKnownResourceTypes() {
    final SchemaSet set = SchemaSet.of(Map.of("Patient", PATIENT, "Observation", OBSERVATION));

    assertEquals(set.schemas(), set.requireResourceTypes(TransformFixtures.DEFINITIONS).schemas());
  }

  @Test
  void refusesNonStructValueNamingType() {
    final String content = "{\"Patient\":" + PATIENT.json() + ",\"Observation\":\"string\"}";

    assertRefusedNaming(() -> SchemaSet.parse(content), "Observation");
  }

  @Test
  void refusesMalformedValueNamingType() {
    final String content =
        "{\"Patient\":" + PATIENT.json() + ",\"Observation\":{\"type\":\"struct\",\"fields\":7}}";

    assertRefusedNaming(() -> SchemaSet.parse(content), "Observation");
  }

  @Test
  void refusesContentThatIsNotJsonObject() {
    assertThrows(IllegalArgumentException.class, () -> SchemaSet.parse("[" + PATIENT.json() + "]"));
    assertThrows(IllegalArgumentException.class, () -> SchemaSet.parse("{\"Patient\":"));
  }

  @Test
  void emptySetEqualsNoSet() {
    final SchemaSet built = SchemaSet.of(Map.of());
    final SchemaSet parsed = SchemaSet.parse("{}");

    assertTrue(built.isEmpty());
    assertTrue(parsed.isEmpty());
    assertEquals(Map.of(), built.schemas());
    assertEquals(Optional.empty(), parsed.schema("Patient"));
  }
}
