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
package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests the conversion of a schema set to and from a schema set file (T060, FR-028). */
class SchemaSetsTest {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Field metadata that is nested, as a struct type's JSON form carries it. */
  @Nonnull
  private static final Metadata NESTED_METADATA =
      new MetadataBuilder()
          .putString("comment", "the given name")
          .putMetadata("source", new MetadataBuilder().putLong("version", 2).build())
          .build();

  /**
   * A schema set whose schemas are out of canonical order, and carry non-nullable fields, empty and
   * nested field metadata, arrays and nested structs, all of which a file must keep.
   */
  @Nonnull
  private static Map<String, StructType> schemaSet() {
    final StructType patient =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("resourceType", DataTypes.StringType, true),
                DataTypes.createStructField(
                    "name",
                    DataTypes.createArrayType(
                        DataTypes.createStructType(
                            Arrays.asList(
                                DataTypes.createStructField(
                                    "given",
                                    DataTypes.createArrayType(DataTypes.StringType, false),
                                    true,
                                    NESTED_METADATA),
                                DataTypes.createStructField(
                                    "family", DataTypes.StringType, false))),
                        false),
                    true)));
    final StructType condition =
        StructType.fromDDL(
            "resourceType STRING, id STRING, subject STRUCT<reference: STRING>,"
                + " _recordedDate STRUCT<__start: TIMESTAMP, __end: TIMESTAMP>");
    final Map<String, StructType> schemas = new LinkedHashMap<>();
    schemas.put("Patient", patient);
    schemas.put("Condition", condition);
    return schemas;
  }

  @Test
  void toJsonMapsEachResourceTypeToTheJsonFormOfItsStructType() throws Exception {
    final Map<String, StructType> schemas = schemaSet();
    final ObjectNode expected = MAPPER.createObjectNode();
    schemas.forEach((type, schema) -> expected.set(type, readTree(schema.json())));

    assertEquals(expected, MAPPER.readTree(SchemaSets.toJson(schemas)));
  }

  @Test
  void fromJsonOfToJsonEqualsTheSetExactly() {
    final Map<String, StructType> schemas = schemaSet();

    final Map<String, StructType> readBack = SchemaSets.fromJson(SchemaSets.toJson(schemas));

    // Struct type equality covers field order, nullability and field metadata at every depth.
    assertEquals(schemas, readBack);
    assertEquals(
        List.copyOf(schemas.keySet()),
        List.copyOf(readBack.keySet()),
        "the resource types should keep the order of the file");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "not json", "[]", "{\"Patient\": ", "\"Patient\""})
  void refusesMalformedContent(@Nonnull final String json) {
    assertThrows(IllegalArgumentException.class, () -> SchemaSets.fromJson(json));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"Condition\": \"string\"}",
        "{\"Condition\": 1}",
        "{\"Condition\": {\"type\": \"array\", \"elementType\": \"string\","
            + " \"containsNull\": true}}"
      })
  void refusesValueThatIsNotAStructNamingItsKey(@Nonnull final String json) {
    final IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> SchemaSets.fromJson(json));

    assertTrue(error.getMessage().contains("Condition"), error.getMessage());
  }

  @Test
  void emptyObjectGivesAnEmptySet() {
    assertEquals(Map.of(), SchemaSets.fromJson("{}"));
  }

  @Nonnull
  private static JsonNode readTree(@Nonnull final String json) {
    try {
      return MAPPER.readTree(json);
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
