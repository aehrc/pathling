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

package au.csiro.pathling.library.io.sink;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Describes how the schema of a batch differs from the schema of the table it is written into, at
 * every level of nesting.
 *
 * <p>Fields are matched by name, so a difference of field order alone is not a difference, and
 * nullability is not compared, because a write places fields by name and admits a non-nullable
 * field into a nullable one. Each difference is named by its path, in which an array element
 * contributes {@code []} and a map value {@code {}}, as {@code name[].use}.
 */
final class SchemaDifferences {

  private SchemaDifferences() {}

  /**
   * Returns a description of each difference between the two schemas, or an empty list if they
   * carry the same fields with the same types.
   *
   * @param table the schema of the table
   * @param data the schema of the data being written
   * @return the differences, in a stable order
   */
  @Nonnull
  static List<String> between(@Nonnull final StructType table, @Nonnull final StructType data) {
    final List<String> differences = new ArrayList<>();
    compare("", table, data, differences);
    return differences;
  }

  private static void compare(
      @Nonnull final String path,
      @Nonnull final DataType table,
      @Nonnull final DataType data,
      @Nonnull final List<String> differences) {
    if (table instanceof final StructType tableStruct
        && data instanceof final StructType dataStruct) {
      compareStructs(path, tableStruct, dataStruct, differences);
    } else if (table instanceof final ArrayType tableArray
        && data instanceof final ArrayType dataArray) {
      compare(path + "[]", tableArray.elementType(), dataArray.elementType(), differences);
    } else if (table instanceof final MapType tableMap && data instanceof final MapType dataMap) {
      compare(path + "{}", tableMap.keyType(), dataMap.keyType(), differences);
      compare(path + "{}", tableMap.valueType(), dataMap.valueType(), differences);
    } else if (!table.sameType(data)) {
      differences.add(
          "'"
              + path
              + "' is "
              + table.simpleString()
              + " in the table but "
              + data.simpleString()
              + " in the data");
    }
  }

  private static void compareStructs(
      @Nonnull final String path,
      @Nonnull final StructType table,
      @Nonnull final StructType data,
      @Nonnull final List<String> differences) {
    final Map<String, StructField> dataFields = byName(data);
    final Map<String, StructField> tableFields = byName(table);
    for (final StructField field : table.fields()) {
      final String fieldPath = join(path, field.name());
      final StructField other = dataFields.get(field.name());
      if (other == null) {
        differences.add("'" + fieldPath + "' is in the table but not in the data");
      } else {
        compare(fieldPath, field.dataType(), other.dataType(), differences);
      }
    }
    Arrays.stream(data.fields())
        .filter(field -> !tableFields.containsKey(field.name()))
        .forEach(
            field ->
                differences.add(
                    "'" + join(path, field.name()) + "' is in the data but not in the table"));
  }

  @Nonnull
  private static Map<String, StructField> byName(@Nonnull final StructType struct) {
    return Arrays.stream(struct.fields())
        .collect(
            Collectors.toMap(StructField::name, Function.identity(), (first, second) -> first));
  }

  @Nonnull
  private static String join(@Nonnull final String path, @Nonnull final String name) {
    return path.isEmpty() ? name : path + "." + name;
  }
}
