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

package au.csiro.pathling.library.io;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.types.StructType;

/**
 * The schema equality of spec SC-002, which the tests of a schema that is "the same" or "equal"
 * use: base schemas equal field for field, in order at every depth, ignoring nullability and field
 * metadata. Annotation fields are left out, since they are compared against the configuration.
 */
public final class SchemaEquality {

  private SchemaEquality() {}

  /**
   * Returns whether two schemas are the same.
   *
   * @param left one schema
   * @param right the other schema
   * @return whether they are the same
   */
  public static boolean same(@Nonnull final StructType left, @Nonnull final StructType right) {
    throw new UnsupportedOperationException("Schema equality is not yet implemented.");
  }

  /**
   * Returns whether two schema sets are the same: they name the same resource types, and the
   * schemas of each type are the same.
   *
   * @param left one schema set
   * @param right the other schema set
   * @return whether they are the same
   */
  public static boolean sameSet(
      @Nonnull final Map<String, StructType> left, @Nonnull final Map<String, StructType> right) {
    throw new UnsupportedOperationException("Schema equality is not yet implemented.");
  }
}
