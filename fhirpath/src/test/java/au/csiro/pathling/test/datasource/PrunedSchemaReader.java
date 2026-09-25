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

package au.csiro.pathling.test.datasource;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Writes encoded resources to Parquet and reads them back, either with the schema they were written
 * with or with a schema from which chosen elements have been removed. The removed elements are then
 * genuinely absent from the dataset the engine reads, as they are from a pruned table, rather than
 * dropped by a projection that the analyzer could see through.
 *
 * <p>An element is named by its dotted path from the resource root, such as {@code active} or
 * {@code name.family}. Each step below the root descends through a struct or an array of structs.
 *
 * @author Piotr Szul
 */
public final class PrunedSchemaReader {

  @Nonnull private final SparkSession spark;

  @Nonnull private final String path;

  @Nonnull private final StructType schema;

  private PrunedSchemaReader(
      @Nonnull final SparkSession spark,
      @Nonnull final String path,
      @Nonnull final StructType schema) {
    this.spark = spark;
    this.path = path;
    this.schema = schema;
  }

  /**
   * Writes a dataset to Parquet at the given location, and returns a reader over the written files.
   *
   * @param dataset the dataset to write
   * @param path the directory to write to
   * @return a reader over the written files
   */
  @Nonnull
  public static PrunedSchemaReader write(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String path) {
    dataset.coalesce(1).write().mode(SaveMode.Overwrite).parquet(path);
    return new PrunedSchemaReader(dataset.sparkSession(), path, dataset.schema());
  }

  /**
   * Reads the written files back with the schema they were written with.
   *
   * @return the dataset as written
   */
  @Nonnull
  public Dataset<Row> read() {
    return spark.read().schema(schema).parquet(path);
  }

  /**
   * Reads the written files back with the named elements removed from the schema.
   *
   * @param elementPaths the dotted paths of the elements to remove
   * @return the dataset, without the named elements
   */
  @Nonnull
  public Dataset<Row> readWithout(@Nonnull final String... elementPaths) {
    StructType pruned = schema;
    for (final String elementPath : elementPaths) {
      pruned = without(pruned, Arrays.asList(elementPath.split("\\.")));
    }
    return spark.read().schema(pruned).parquet(path);
  }

  @Nonnull
  private static StructType without(
      @Nonnull final StructType struct, @Nonnull final List<String> steps) {
    final String head = steps.get(0);
    if (Arrays.stream(struct.fieldNames()).noneMatch(head::equals)) {
      throw new IllegalArgumentException("No field named " + head + " in " + struct.simpleString());
    }
    if (steps.size() == 1) {
      return new StructType(
          Arrays.stream(struct.fields())
              .filter(field -> !field.name().equals(head))
              .toArray(StructField[]::new));
    }
    final List<String> rest = steps.subList(1, steps.size());
    return new StructType(
        Arrays.stream(struct.fields())
            .map(
                field ->
                    field.name().equals(head)
                        ? new StructField(
                            field.name(),
                            without(field.dataType(), rest),
                            field.nullable(),
                            field.metadata())
                        : field)
            .toArray(StructField[]::new));
  }

  @Nonnull
  private static DataType without(@Nonnull final DataType type, @Nonnull final List<String> steps) {
    if (type instanceof final StructType struct) {
      return without(struct, steps);
    }
    if (type instanceof final ArrayType array) {
      return new ArrayType(without(array.elementType(), steps), array.containsNull());
    }
    throw new IllegalArgumentException("Cannot descend into " + type.simpleString());
  }
}
