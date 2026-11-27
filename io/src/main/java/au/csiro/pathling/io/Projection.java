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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
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
 *
 * <p>Fields are matched on their exact names. A name that differs from another only in case is
 * refused rather than matched or treated as a different field, since it cannot be told which was
 * meant, and Spark's resolver would match either of them ambiguously. Paths are named in messages
 * delimited by backticks, as dotted names in which arrays and maps add nothing.
 */
public final class Projection {

  private Projection() {}

  /**
   * Presents the dataset under the target schema.
   *
   * <p>Only the parts of the dataset that differ from the target are rebuilt; a subtree whose type
   * already matches, field names included and nullability aside, is passed straight through.
   *
   * <p>The result takes the target's field names and order, but not its nullability or field
   * metadata, which come from the dataset where a field is taken from it, including within a
   * subtree that is rebuilt. A field that only the target has is nullable. Where a field is present
   * on both sides with leaf types that differ, the result keeps the dataset's type.
   *
   * @param dataset the dataset to project
   * @param target the schema to present it under
   * @return the projected dataset
   * @throws IllegalArgumentException if a field of the dataset differs from a field of the target
   *     only in case, or if a struct of the dataset that the projection descends into holds two
   *     fields that differ only in case, naming both paths
   */
  @Nonnull
  public static Dataset<Row> project(
      @Nonnull final Dataset<Row> dataset, @Nonnull final StructType target) {
    // The check comes first and stands alone, so that nothing below it resolves a field by a name
    // that matches more than one.
    requireCaseExact(dataset.schema(), target);
    return dataset.select(projectFields(dataset::col, dataset.schema(), target));
  }

  /**
   * Checks that the names of a source schema and a target schema can be matched exactly: that no
   * field of the source differs from a field of the target only in case, and that no struct of the
   * source that a projection descends into holds two fields differing only in case.
   *
   * @param source the schema of the dataset to be projected
   * @param target the schema to project it onto
   * @throws IllegalArgumentException naming both paths of the first such pair found
   */
  private static void requireCaseExact(
      @Nonnull final StructType source, @Nonnull final StructType target) {
    requireCaseExactStruct(source, target, "");
  }

  private static void requireCaseExactStruct(
      @Nonnull final StructType source,
      @Nonnull final StructType target,
      @Nonnull final String prefix) {
    final Map<String, String> byLowerCase = new HashMap<>();
    Arrays.stream(source.fieldNames())
        .forEach(
            name ->
                Optional.ofNullable(byLowerCase.putIfAbsent(name.toLowerCase(Locale.ROOT), name))
                    .ifPresent(
                        other -> {
                          throw new IllegalArgumentException(
                              "The fields "
                                  + quoted(prefix, other)
                                  + " and "
                                  + quoted(prefix, name)
                                  + " differ only in case");
                        }));
    Arrays.stream(target.fields())
        .forEach(
            field -> {
              final StructField sourceField = fieldOrNull(source, field.name());
              if (sourceField != null) {
                requireCaseExactType(
                    sourceField.dataType(), field.dataType(), prefix + field.name() + ".");
              } else {
                Optional.ofNullable(byLowerCase.get(field.name().toLowerCase(Locale.ROOT)))
                    .ifPresent(
                        name -> {
                          throw new IllegalArgumentException(
                              "The field "
                                  + quoted(prefix, name)
                                  + " differs only in case from the field "
                                  + quoted(prefix, field.name())
                                  + " of the target schema");
                        });
              }
            });
  }

  /** Descends into a pair of types wherever a projection would rebuild them. */
  private static void requireCaseExactType(
      @Nonnull final DataType sourceType,
      @Nonnull final DataType targetType,
      @Nonnull final String prefix) {
    if (sameExactly(sourceType, targetType)) {
      return;
    }
    if (sourceType instanceof final StructType source
        && targetType instanceof final StructType target) {
      requireCaseExactStruct(source, target, prefix);
    } else if (sourceType instanceof final ArrayType source
        && targetType instanceof final ArrayType target) {
      requireCaseExactType(source.elementType(), target.elementType(), prefix);
    } else if (sourceType instanceof final MapType source
        && targetType instanceof final MapType target) {
      requireCaseExactType(source.valueType(), target.valueType(), prefix);
    }
  }

  @Nonnull
  private static String quoted(@Nonnull final String prefix, @Nonnull final String name) {
    return "`" + prefix + name + "`";
  }

  /**
   * Returns whether two types are the same, with field names compared exactly and nullability
   * ignored. Spark's own {@code sameType} compares field names as the session's resolver does,
   * which by default ignores case, so it would pass through a subtree whose names differ in case.
   */
  private static boolean sameExactly(@Nonnull final DataType left, @Nonnull final DataType right) {
    if (left instanceof final StructType leftStruct
        && right instanceof final StructType rightStruct) {
      final StructField[] leftFields = leftStruct.fields();
      final StructField[] rightFields = rightStruct.fields();
      return leftFields.length == rightFields.length
          && IntStream.range(0, leftFields.length)
              .allMatch(
                  i ->
                      leftFields[i].name().equals(rightFields[i].name())
                          && sameExactly(leftFields[i].dataType(), rightFields[i].dataType()));
    }
    if (left instanceof final ArrayType leftArray && right instanceof final ArrayType rightArray) {
      return sameExactly(leftArray.elementType(), rightArray.elementType());
    }
    if (left instanceof final MapType leftMap && right instanceof final MapType rightMap) {
      return sameExactly(leftMap.keyType(), rightMap.keyType())
          && sameExactly(leftMap.valueType(), rightMap.valueType());
    }
    return left.equals(right);
  }

  /**
   * Projects a column onto a target type, resolving struct fields by name at every depth.
   *
   * @param column the column to project
   * @param sourceType the type of the column
   * @param nullable whether the source declares that the column can be null: the nullability of its
   *     field, or the {@code containsNull} or {@code valueContainsNull} of the array or map holding
   *     it
   * @param targetType the type to project it onto
   * @return the projected column
   */
  @Nonnull
  private static Column projectColumn(
      @Nonnull final Column column,
      @Nonnull final DataType sourceType,
      final boolean nullable,
      @Nonnull final DataType targetType) {
    if (sameExactly(sourceType, targetType)) {
      // Nothing below this point differs, so there is nothing to rebuild.
      return column;
    }
    if (sourceType instanceof final StructType source
        && targetType instanceof final StructType target) {
      final Column rebuilt = struct(projectFields(column::getField, source, target));
      // A struct built field by field is never null, so where the source can be null the null case
      // has to be restored explicitly, or an absent nested struct would be written as one full of
      // nulls. Where the source cannot be null, the null branch is left out, since it would make
      // the result nullable.
      return nullable ? when(column.isNull(), lit(null).cast(target)).otherwise(rebuilt) : rebuilt;
    }
    if (sourceType instanceof final ArrayType source
        && targetType instanceof final ArrayType target) {
      return transform(
          column,
          element ->
              projectColumn(
                  element, source.elementType(), source.containsNull(), target.elementType()));
    }
    if (sourceType instanceof final MapType source && targetType instanceof final MapType target) {
      return transform_values(
          column,
          (key, value) ->
              projectColumn(
                  value, source.valueType(), source.valueContainsNull(), target.valueType()));
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
                          sourceField.nullable(),
                          field.dataType()))
                  .alias(field.name());
            })
        .toArray(Column[]::new);
  }

  /**
   * Returns the named field of the struct, or null if it has none.
   *
   * <p>Matched on the exact name rather than through the session's resolver, which by default
   * ignores case and so could match a field the target does not name. A name that differs from the
   * target's only in case is not matched here: {@link #requireCaseExact} refuses it before any
   * field is resolved, since it cannot be told whether the two names were meant to be the same.
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
