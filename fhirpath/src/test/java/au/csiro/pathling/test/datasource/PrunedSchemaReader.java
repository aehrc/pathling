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

import au.csiro.pathling.definition.ChoiceDefinition;
import au.csiro.pathling.definition.NodeDefinition;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
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
 * <p>A reader may be given the definition of the resource, or a schema carrying every element the
 * definitions describe for it, such as the previous layout's dense schema. An element so described
 * may then be named for removal even where the written schema does not carry it, because the new
 * layout has already pruned it, and it is left as it is. An element that is neither described nor
 * carried is still refused, so that a misspelt path fails rather than leaving the data unchanged.
 *
 * @author Piotr Szul
 */
public final class PrunedSchemaReader {

  @Nonnull private final SparkSession spark;

  @Nonnull private final String path;

  @Nonnull private final StructType schema;

  @Nullable private final Predicate<List<String>> described;

  private PrunedSchemaReader(
      @Nonnull final SparkSession spark,
      @Nonnull final String path,
      @Nonnull final StructType schema,
      @Nullable final Predicate<List<String>> described) {
    this.spark = spark;
    this.path = path;
    this.schema = schema;
    this.described = described;
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
    return writeChecking(dataset, path, null);
  }

  /**
   * Writes a dataset to Parquet at the given location, and returns a reader over the written files
   * that accepts the removal of an element the written schema has already pruned.
   *
   * @param dataset the dataset to write, in either layout
   * @param path the directory to write to
   * @param describedSchema a schema carrying every element the definitions describe for the
   *     resource, against which a path absent from the written schema is checked
   * @return a reader over the written files
   */
  @Nonnull
  public static PrunedSchemaReader write(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String path,
      @Nonnull final StructType describedSchema) {
    return writeChecking(dataset, path, steps -> carries(describedSchema, steps));
  }

  /**
   * Writes a dataset to Parquet at the given location, and returns a reader over the written files
   * that accepts the removal of an element the written schema has already pruned, provided the
   * definitions describe it.
   *
   * @param dataset the dataset to write
   * @param path the directory to write to
   * @param fhirContext the FHIR context whose definitions a path absent from the written schema is
   *     checked against
   * @param resourceType the type of every resource in the dataset
   * @return a reader over the written files
   */
  @Nonnull
  public static PrunedSchemaReader write(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String path,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String resourceType) {
    final NodeDefinition definition =
        FhirDefinitionContext.of(fhirContext).findResourceDefinition(resourceType);
    return writeChecking(dataset, path, steps -> describes(definition, steps));
  }

  @Nonnull
  private static PrunedSchemaReader writeChecking(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String path,
      @Nullable final Predicate<List<String>> described) {
    dataset.coalesce(1).write().mode(SaveMode.Overwrite).parquet(path);
    return new PrunedSchemaReader(dataset.sparkSession(), path, dataset.schema(), described);
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
   * Reads the written files back with the named elements removed from the schema. An element the
   * written schema does not carry is left as it is where it is described.
   *
   * @param elementPaths the dotted paths of the elements to remove
   * @return the dataset, without the named elements
   * @throws IllegalArgumentException where an element is neither carried by the written schema nor
   *     described
   */
  @Nonnull
  public Dataset<Row> readWithout(@Nonnull final String... elementPaths) {
    StructType pruned = schema;
    for (final String elementPath : elementPaths) {
      final List<String> steps = Arrays.asList(elementPath.split("\\."));
      if (carries(pruned, steps) || described == null || !described.test(steps)) {
        pruned = without(pruned, steps);
      }
    }
    return spark.read().schema(pruned).parquet(path);
  }

  /** Returns whether a schema carries the element at a path. */
  private static boolean carries(@Nonnull final DataType type, @Nonnull final List<String> steps) {
    DataType current = type;
    for (final String step : steps) {
      while (current instanceof final ArrayType array) {
        current = array.elementType();
      }
      if (!(current instanceof final StructType struct)
          || Arrays.stream(struct.fieldNames()).noneMatch(step::equals)) {
        return false;
      }
      current = struct.apply(step).dataType();
    }
    return true;
  }

  /**
   * Returns whether the definitions describe the element at a dotted path, as a stored column names
   * it. A step may name a choice variant, such as {@code valueString}, which its parent resolves,
   * but not the choice itself, such as {@code value}, which is never stored as a column.
   *
   * @param definition the definition to resolve the path from
   * @param elementPath the dotted path of the element
   * @return true where every step of the path resolves
   */
  public static boolean describes(
      @Nonnull final NodeDefinition definition, @Nonnull final String elementPath) {
    return describes(definition, Arrays.asList(elementPath.split("\\.")));
  }

  private static boolean describes(
      @Nonnull final NodeDefinition definition, @Nonnull final List<String> steps) {
    Optional<? extends NodeDefinition> current = Optional.of(definition);
    for (final String step : steps) {
      current =
          current
              .flatMap(node -> node.getChildElement(step))
              .filter(child -> !(child instanceof ChoiceDefinition));
    }
    return current.isPresent();
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
