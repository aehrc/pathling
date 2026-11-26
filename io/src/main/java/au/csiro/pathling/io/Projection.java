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

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.transform_values;
import static org.apache.spark.sql.functions.when;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Projects a dataset onto a target schema by field name, at every depth.
 *
 * <p>Fields the target lacks are dropped, fields the dataset lacks are supplied as typed nulls, and
 * the fields come out in the target's order. Structs are matched through arrays and map values. No
 * value is cast: a field present on both sides keeps the dataset's type below the point where the
 * two schemas stop being structs, arrays or maps.
 */
public final class Projection {

  private Projection() {}

  /**
   * Presents the dataset under the target schema.
   *
   * <p>Only the parts of the dataset that differ from the target are rebuilt; a subtree whose type
   * already matches is passed straight through.
   *
   * @param dataset the dataset to project
   * @param target the schema to present it under
   * @return the projected dataset
   */
  @Nonnull
  public static Dataset<Row> project(
      @Nonnull final Dataset<Row> dataset, @Nonnull final StructType target) {
    return dataset.select(projectFields(dataset::col, dataset.schema(), target));
  }

  /** Projects a column onto a target type, resolving struct fields by name at every depth. */
  @Nonnull
  private static Column projectColumn(
      @Nonnull final Column column,
      @Nonnull final DataType sourceType,
      @Nonnull final DataType targetType) {
    if (sourceType.sameType(targetType)) {
      // Nothing below this point differs, so there is nothing to rebuild.
      return column;
    }
    if (sourceType instanceof final StructType source
        && targetType instanceof final StructType target) {
      // A struct built field by field is never null, so the null case has to be restored
      // explicitly, or an absent nested struct would be written as one full of nulls.
      return when(column.isNull(), lit(null).cast(target))
          .otherwise(struct(projectFields(column::getField, source, target)));
    }
    if (sourceType instanceof final ArrayType source
        && targetType instanceof final ArrayType target) {
      return transform(
          column, element -> projectColumn(element, source.elementType(), target.elementType()));
    }
    if (sourceType instanceof final MapType source && targetType instanceof final MapType target) {
      return transform_values(
          column, (key, value) -> projectColumn(value, source.valueType(), target.valueType()));
    }
    return column;
  }

  /**
   * Builds one column per target field, taking it from the source by name where the source has it,
   * and supplying a typed null where it does not.
   *
   * @param fieldAccessor how to reach a named field of the source, which differs between the top
   *     level of a dataset and a nested struct
   * @param source the schema the columns are being taken from
   * @param target the schema the columns are being presented under
   * @return the projected columns, in the target's field order
   */
  @Nonnull
  private static Column[] projectFields(
      @Nonnull final Function<String, Column> fieldAccessor,
      @Nonnull final StructType source,
      @Nonnull final StructType target) {
    return Arrays.stream(target.fields())
        .map(
            field -> {
              final StructField sourceField = fieldOrNull(source, field.name());
              return (sourceField == null
                      ? lit(null).cast(field.dataType())
                      : projectColumn(
                          fieldAccessor.apply(sourceField.name()),
                          sourceField.dataType(),
                          field.dataType()))
                  .alias(field.name());
            })
        .toArray(Column[]::new);
  }

  /**
   * Returns the named field of the struct, or null if it has none.
   *
   * <p>Matched on the exact name rather than through the session's resolver, unlike the decode
   * side. A difference of case alone therefore reads as a difference of field, which is the
   * conservative reading, since the two names may well have been meant to be different.
   */
  @Nullable
  private static StructField fieldOrNull(
      @Nonnull final StructType struct, @Nonnull final String name) {
    return Arrays.stream(struct.fields())
        .filter(field -> field.name().equals(name))
        .findFirst()
        .orElse(null);
  }
}
