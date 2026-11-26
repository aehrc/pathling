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

import au.csiro.pathling.definition.DefinitionContext;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * A schema set: a schema in this layout for each of a number of resource types, keyed by resource
 * type code (FR-004, research R10). It is the internal form of the map that the public API is
 * given, and of a schema set file's content.
 *
 * <p>An empty set is the same as no set.
 */
public final class SchemaSet {

  @Nonnull private static final JsonFactory FACTORY = new JsonFactory();

  @Nonnull private final Map<String, StructType> schemas;

  private SchemaSet(@Nonnull final Map<String, StructType> schemas) {
    this.schemas = schemas;
  }

  /**
   * Returns the schema set a map holds.
   *
   * @param schemas the schema of each resource type, keyed by resource type code
   * @return the schema set
   * @throws IllegalArgumentException if a schema is null, naming its key
   */
  @Nonnull
  public static SchemaSet of(@Nonnull final Map<String, StructType> schemas) {
    schemas.forEach(
        (resourceType, schema) -> {
          if (schema == null) {
            throw new IllegalArgumentException("No schema is given for " + resourceType);
          }
        });
    return new SchemaSet(Map.copyOf(schemas));
  }

  /**
   * Parses the content of a schema set file: one JSON object from resource type code to the JSON
   * form of a struct type (data-model.md).
   *
   * @param json the content
   * @return the schema set
   * @throws IllegalArgumentException if the content is not a JSON object, or a value is not a
   *     struct type or is malformed, naming its key
   */
  @Nonnull
  public static SchemaSet parse(@Nonnull final String json) {
    final Map<String, StructType> schemas = new HashMap<>();
    try (final JsonParser parser = FACTORY.createParser(json)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("A schema set is not a JSON object");
      }
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        final String resourceType = parser.currentName();
        parser.nextToken();
        // Each value is copied out as text, so that Spark parses it as the JSON form of a type.
        final StringWriter value = new StringWriter();
        try (final JsonGenerator generator = FACTORY.createGenerator(value)) {
          generator.copyCurrentStructure(parser);
        }
        schemas.put(resourceType, structType(resourceType, value.toString()));
      }
      if (parser.currentToken() != JsonToken.END_OBJECT || parser.nextToken() != null) {
        throw new IllegalArgumentException("A schema set is not a JSON object");
      }
    } catch (final IOException e) {
      throw new IllegalArgumentException("A schema set is not valid JSON: " + e.getMessage(), e);
    }
    return new SchemaSet(Map.copyOf(schemas));
  }

  /**
   * Checks that every key of this set is a resource type code the definitions know.
   *
   * @param definitions the definitions
   * @return this set
   * @throws IllegalArgumentException if a key is not a resource type code, naming it
   */
  @Nonnull
  public SchemaSet requireResourceTypes(@Nonnull final DefinitionContext definitions) {
    // The definitions refuse a name they do not describe as a resource type, naming it.
    schemas.keySet().forEach(definitions::findResourceDefinition);
    return this;
  }

  /**
   * Returns the schema of a resource type, if this set has one.
   *
   * @param resourceType the resource type code
   * @return the schema, or empty if this set has none for the type
   */
  @Nonnull
  public Optional<StructType> schema(@Nonnull final String resourceType) {
    return Optional.ofNullable(schemas.get(resourceType));
  }

  /**
   * Returns the schemas of this set, keyed by resource type code.
   *
   * @return the schemas
   */
  @Nonnull
  public Map<String, StructType> schemas() {
    return schemas;
  }

  /**
   * Returns whether this set has no schemas, which is the same as no set.
   *
   * @return whether the set is empty
   */
  public boolean isEmpty() {
    return schemas.isEmpty();
  }

  /** Parses the JSON form of the schema of one resource type, which must be a struct type. */
  @Nonnull
  private static StructType structType(
      @Nonnull final String resourceType, @Nonnull final String json) {
    final DataType type;
    try {
      type = DataType.fromJson(json);
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException("The schema of " + resourceType + " is malformed", e);
    }
    if (type instanceof final StructType struct) {
      return struct;
    }
    throw new IllegalArgumentException("The schema of " + resourceType + " is not a struct type");
  }
}
