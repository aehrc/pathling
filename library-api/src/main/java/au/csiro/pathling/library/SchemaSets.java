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

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.types.StructType;

/**
 * Converts a schema set to and from the content of a schema set file: one JSON object mapping each
 * resource type code to the JSON form of its struct type (FR-028).
 */
public final class SchemaSets {

  private SchemaSets() {}

  /**
   * Returns the content of a schema set file for a schema set.
   *
   * @param schemas the schema set, keyed by resource type code
   * @return the JSON content
   */
  @Nonnull
  public static String toJson(@Nonnull final Map<String, StructType> schemas) {
    throw new UnsupportedOperationException("SchemaSets.toJson is not implemented yet.");
  }

  /**
   * Returns the schema set held by the content of a schema set file.
   *
   * @param json the JSON content
   * @return the schema set, keyed by resource type code, in the order of the file
   * @throws IllegalArgumentException if the content is malformed, or a value is not a struct type
   */
  @Nonnull
  public static Map<String, StructType> fromJson(@Nonnull final String json) {
    throw new UnsupportedOperationException("SchemaSets.fromJson is not implemented yet.");
  }
}
