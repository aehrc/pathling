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
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;

/**
 * A schema set: a schema in this layout for each of a number of resource types, keyed by resource
 * type code (FR-004, research R10). It is the internal form of the map that the public API is
 * given, and of a schema set file's content.
 *
 * <p>An empty set is the same as no set.
 */
public final class SchemaSet {

  private SchemaSet() {}

  /**
   * Returns the schema set a map holds.
   *
   * @param schemas the schema of each resource type, keyed by resource type code
   * @return the schema set
   * @throws IllegalArgumentException if a schema is null, naming its key
   */
  @Nonnull
  public static SchemaSet of(@Nonnull final Map<String, StructType> schemas) {
    throw new UnsupportedOperationException("Schema sets are not yet implemented.");
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
    throw new UnsupportedOperationException("Schema sets are not yet implemented.");
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
    throw new UnsupportedOperationException("Schema sets are not yet implemented.");
  }

  /**
   * Returns the schema of a resource type, if this set has one.
   *
   * @param resourceType the resource type code
   * @return the schema, or empty if this set has none for the type
   */
  @Nonnull
  public Optional<StructType> schema(@Nonnull final String resourceType) {
    throw new UnsupportedOperationException("Schema sets are not yet implemented.");
  }

  /**
   * Returns the schemas of this set, keyed by resource type code.
   *
   * @return the schemas
   */
  @Nonnull
  public Map<String, StructType> schemas() {
    throw new UnsupportedOperationException("Schema sets are not yet implemented.");
  }

  /**
   * Returns whether this set has no schemas, which is the same as no set.
   *
   * @return whether the set is empty
   */
  public boolean isEmpty() {
    throw new UnsupportedOperationException("Schema sets are not yet implemented.");
  }
}
