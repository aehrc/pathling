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

package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.encoders.ColumnFunctions;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * A primitive with no annotation kind, such as a string, a code, an integer or a boolean, as the
 * engine holds it where a primitive's id and extensions are read (decisions 141 and 152): a flat
 * structure of its stored value, {@code value}, and the metadata fields of its metadata group that
 * the schema carries, its {@code id} and {@code extension}. Under a definition that does not read
 * them, such a primitive is its bare value, and is never held in this form.
 *
 * <p>The fields are read from the metadata group by name, never as the group, so that a read names
 * only the fields an expression uses.
 */
public final class StoredPrimitive {

  /** The name of the stored value of a primitive. */
  @Nonnull public static final String VALUE = "value";

  private StoredPrimitive() {}

  /**
   * Returns the primitive of a stored value and its metadata group, or the primitives of a
   * repeating one paired by position, each with its value and the named metadata fields of its
   * group, where its schema carries them. Nothing is left out: a primitive without a value is a
   * structure without one.
   *
   * @param values the stored value, or the values where it repeats
   * @param groups the metadata group beside it, or the groups where it repeats
   * @param metadata the names of the metadata fields to read from the groups
   * @param repeating whether the primitive repeats
   * @return the primitives, paired with their groups
   */
  @Nonnull
  public static Column paired(
      @Nonnull final Column values,
      @Nonnull final Column groups,
      @Nonnull final List<String> metadata,
      final boolean repeating) {
    return ColumnFunctions.resolveOrNone(
        groups, metadata, List.of(VALUE), List.of(values), repeating);
  }

  /**
   * Returns the value of a primitive.
   *
   * @param primitive the primitive
   * @return its value
   */
  @Nonnull
  public static Column valueOf(@Nonnull final Column primitive) {
    return primitive.getField(VALUE);
  }
}
