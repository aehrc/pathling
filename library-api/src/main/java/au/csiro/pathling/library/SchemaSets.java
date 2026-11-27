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

import au.csiro.pathling.io.SchemaSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.types.StructType;

/**
 * Converts a schema set to and from the content of a schema set file: one JSON object mapping each
 * resource type code to the JSON form of its struct type (FR-028).
 */
public final class SchemaSets {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  private SchemaSets() {}

  /**
   * Returns the content of a schema set file for a schema set.
   *
   * @param schemas the schema set, keyed by resource type code
   * @return the JSON content
   */
  @Nonnull
  public static String toJson(@Nonnull final Map<String, StructType> schemas) {
    final ObjectNode content = MAPPER.createObjectNode();
    // The set refuses a null key or schema, naming it, before anything is written.
    SchemaSet.of(schemas)
        .schemas()
        .forEach(
            (resourceType, schema) -> {
              try {
                content.set(resourceType, MAPPER.readTree(schema.json()));
              } catch (final JsonProcessingException e) {
                throw new IllegalStateException("Spark gave a type JSON form that is not JSON", e);
              }
            });
    return content.toString();
  }

  /**
   * Returns the schema set held by the content of a schema set file.
   *
   * @param json the JSON content
   * @return the schema set, keyed by resource type code, in the order of the file
   * @throws IllegalArgumentException if the content is malformed, if it names a resource type more
   *     than once, naming it, or if a value is not a struct type, naming its key
   */
  @Nonnull
  public static Map<String, StructType> fromJson(@Nonnull final String json) {
    return SchemaSet.parse(json).schemas();
  }
}
