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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import scala.Option;

/**
 * Reads the fields of a structure passed to a function by name, enforcing the type the function
 * requires of each.
 *
 * <p>A function is correct only for input of the types it expects. A field of another type is a
 * breach of that contract, and is reported as an {@link IllegalArgumentException} naming the field,
 * rather than read as if it had the type or taken as null.
 *
 * @author Piotr Szul
 */
public final class RowFields {

  private RowFields() {}

  /**
   * Reads a text field of a structure by name.
   *
   * @param row the structure
   * @param name the name of the field
   * @return the text, or null where the field is null or the structure does not carry it
   * @throws IllegalArgumentException where the structure carries the field with a type other than
   *     text
   */
  @Nullable
  public static String requireString(@Nonnull final Row row, @Nonnull final String name) {
    final Option<Object> found = row.schema().getFieldIndex(name);
    if (found.isEmpty()) {
      return null;
    }
    final int index = (Integer) found.get();
    final DataType type = row.schema().fields()[index].dataType();
    checkArgument(
        type instanceof StringType,
        "Field '" + name + "' must be a string, but is " + type.simpleString());
    return row.isNullAt(index) ? null : row.getString(index);
  }
}
