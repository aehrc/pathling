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
package au.csiro.pathling.legacy;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Finds the columns of a table that a decoder would not read.
 *
 * <p>The legacy decoder resolves the columns it reads by name, and ignores any other, so a column
 * of the table that the decoder's schema lacks would be dropped from the export without a word.
 * This is what finds them, so that they can be refused instead.
 *
 * @author Piotr Szul
 */
final class SchemaCoverage {

  @Nonnull private static final String SEPARATOR = ".";

  private SchemaCoverage() {}

  /**
   * Returns the paths of the fields of a table that a schema does not read: those it lacks, and
   * those it reads as another type. Nullability is not compared, because a table read from files
   * holds every field as nullable.
   *
   * @param table the schema of the table
   * @param reader the schema the table is read with
   * @return the paths, in the table's field order and depth first
   */
  @Nonnull
  static List<String> unread(@Nonnull final StructType table, @Nonnull final StructType reader) {
    return unread(table, reader, "").toList();
  }

  @Nonnull
  private static Stream<String> unread(
      @Nonnull final StructType table,
      @Nonnull final StructType reader,
      @Nonnull final String prefix) {
    return Arrays.stream(table.fields())
        .flatMap(
            field -> {
              final String path = prefix + field.name();
              return field(reader, field.name())
                  .map(read -> unread(field.dataType(), read.dataType(), path))
                  .orElseGet(() -> Stream.of(path));
            });
  }

  @Nonnull
  private static Stream<String> unread(
      @Nonnull final DataType table, @Nonnull final DataType reader, @Nonnull final String path) {
    if (table instanceof final StructType tableStruct
        && reader instanceof final StructType readerStruct) {
      return unread(tableStruct, readerStruct, path + SEPARATOR);
    }
    if (table instanceof final ArrayType tableArray
        && reader instanceof final ArrayType readerArray) {
      return unread(tableArray.elementType(), readerArray.elementType(), path);
    }
    if (table instanceof final MapType tableMap && reader instanceof final MapType readerMap) {
      return Stream.concat(
          unread(tableMap.keyType(), readerMap.keyType(), path),
          unread(tableMap.valueType(), readerMap.valueType(), path));
    }
    return table.equals(reader) ? Stream.empty() : Stream.of(path);
  }

  @Nonnull
  private static Optional<StructField> field(
      @Nonnull final StructType struct, @Nonnull final String name) {
    return Arrays.stream(struct.fields()).filter(field -> field.name().equals(name)).findFirst();
  }
}
