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

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.transform_values;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.utilities.StructureMerge;
import io.delta.tables.DeltaMergeBuilder;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A data sink that writes data to a Delta Lake table on a filesystem.
 *
 * @author John Grimes
 */
final class DeltaSink implements DataSink {

  /** The Pathling context to use. */
  @Nonnull private final PathlingContext context;

  /** The path to write the Delta database to. */
  @Nonnull private final String path;

  /** The save mode to use when writing data. */
  @Nonnull private final SaveMode saveMode;

  /** A function that maps resource type to file name. */
  @Nonnull private final UnaryOperator<String> fileNameMapper;

  /**
   * Constructs a DeltaSink with a custom file name mapper.
   *
   * @param context the PathlingContext to use
   * @param path the path to write the Delta database to
   * @param saveMode the {@link SaveMode} to use
   * @param fileNameMapper a function that maps resource type to file name
   */
  DeltaSink(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final SaveMode saveMode,
      @Nonnull final UnaryOperator<String> fileNameMapper) {
    this.context = context;
    this.path = path;
    this.saveMode = saveMode;
    this.fileNameMapper = fileNameMapper;
  }

  /**
   * Constructs a DeltaSink with default file naming.
   *
   * @param context the PathlingContext to use
   * @param path the path to write the Delta database to
   * @param saveMode the {@link SaveMode} to use
   */
  DeltaSink(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final SaveMode saveMode) {
    // By default, name the files using the resource type alone.
    this(context, path, saveMode, UnaryOperator.identity());
  }

  /**
   * Writes every resource type of the source to its table.
   *
   * <p>Every resource type is read, and every table an append or a merge would write into is
   * classified, before anything is written, so that a refusal of either kind leaves every table as
   * it was rather than some written and some not (FR-037a).
   *
   * @param source the data to write
   * @return the tables written
   * @throws IllegalStateException if the data carries the range annotations of dates and the
   *     session does not write timestamps to Parquet as INT64 microseconds (decision 138)
   * @throws au.csiro.pathling.library.io.source.UnsupportedLayoutException if a table the write
   *     would append to or merge into is in a layout this release does not read, or the source
   *     refuses its own data
   */
  @Override
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    final Map<String, Dataset<Row>> datasets = new LinkedHashMap<>();
    source.getResourceTypes().forEach(type -> datasets.put(type, source.read(type)));
    StoredTimestamps.requireMicros(context.getSpark(), datasets);
    if (saveMode == SaveMode.APPEND || saveMode == SaveMode.MERGE) {
      datasets.keySet().forEach(this::checkTargetLayout);
    }

    final List<FileInformation> fileInfos = new ArrayList<>();
    for (final Map.Entry<String, Dataset<Row>> entry : datasets.entrySet()) {
      final String resourceType = entry.getKey();
      final Dataset<Row> dataset = entry.getValue();
      final String tablePath = tablePath(resourceType);

      fileInfos.add(new FileInformation(resourceType, tablePath));

      switch (saveMode) {
        case ERROR_IF_EXISTS, APPEND, IGNORE, OVERWRITE ->
            writeDataset(dataset, tablePath, saveMode);
        case MERGE -> {
          if (deltaTableExists(tablePath)) {
            // If the table already exists, merge the data in.
            final DeltaTable table = DeltaTable.forPath(context.getSpark(), tablePath);
            merge(context, resourceType, table, dataset);
          } else {
            // If the table does not exist, create it. If an error occurs here, there must be a
            // pre-existing file at the path that is not a Delta table.
            writeDataset(dataset, tablePath, SaveMode.ERROR_IF_EXISTS);
          }
        }
        default -> throw new IllegalStateException("Unexpected save mode: " + saveMode);
      }
    }
    return new WriteDetails(fileInfos);
  }

  /**
   * Returns the path of the table a resource type is written to.
   *
   * @param resourceType the resource type
   * @return the path of its table
   */
  @Nonnull
  private String tablePath(@Nonnull final String resourceType) {
    return safelyJoinPaths(path, String.join(".", fileNameMapper.apply(resourceType), "parquet"));
  }

  /**
   * Refuses a write into an existing table in a layout this release does not read, before the write
   * could widen the table to carry both layouts. The table's schema is read from its log.
   *
   * @param resourceType the resource type whose table is checked
   */
  private void checkTargetLayout(@Nonnull final String resourceType) {
    final String tablePath = tablePath(resourceType);
    if (deltaTableExists(tablePath)) {
      LayoutDetector.check(
          resourceType, DeltaTable.forPath(context.getSpark(), tablePath).toDF().schema());
    }
  }

  /**
   * Writes the data to a Delta table at the specified path with the specified save mode.
   *
   * @param dataset the dataset to write to the Delta table
   * @param tablePath the path to write the Delta table to
   * @param saveMode the save mode to use for writing
   */
  private static void writeDataset(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String tablePath,
      @Nonnull final SaveMode saveMode) {
    final var writer = dataset.write().format("delta");

    // Apply save mode if it has a Spark equivalent
    saveMode.getSparkSaveMode().ifPresent(writer::mode);

    // Delta Lake requires explicit schema overwrite permission when using OVERWRITE mode.
    if (saveMode == SaveMode.OVERWRITE) {
      writer.option("overwriteSchema", "true");
    }

    // A schema fitted to the data means a batch carrying elements the table has not seen before is
    // the steady state, so an append widens the table to admit them rather than failing (FR-042).
    if (saveMode == SaveMode.APPEND) {
      writer.option("mergeSchema", "true");
    }

    writer.save(tablePath);
  }

  /**
   * Merges the given dataset into the specified Delta table, matching resources on their id.
   *
   * <p>A matched resource is replaced in full, so an element its earlier version carried and the
   * dataset does not is left null rather than kept. Where the table carries fields the dataset does
   * not, the dataset is presented under the table's schema with those fields null.
   *
   * <p>Where the dataset carries fields the table does not, the table is widened to admit them, as
   * an append widens it (decision 41). Under a schema fitted to the data, a batch carrying elements
   * the table has not seen before is the steady state rather than an error, so refusing to widen
   * here would fire on ordinary use, and append and upsert would answer the same question two ways.
   *
   * @param context the Pathling context, whose FHIR definitions order the union of the two schemas
   * @param resourceType the type of the resources being merged
   * @param table the Delta table to merge into
   * @param dataset the dataset containing updates to be merged
   */
  static void merge(
      @Nonnull final PathlingContext context,
      @Nonnull final String resourceType,
      @Nonnull final DeltaTable table,
      @Nonnull final Dataset<Row> dataset) {
    final StructType targetSchema = table.toDF().schema();
    final Set<String> targetPaths = fieldPaths(targetSchema);
    final Set<String> sourcePaths = fieldPaths(dataset.schema());
    final boolean widening = !targetPaths.containsAll(sourcePaths);

    // The dataset is presented under a schema carrying every field of the table, with the fields
    // it lacks null, so that the merge itself is an ordinary aligned one and a row it matches is
    // replaced in full rather than keeping the values the table held for the fields the dataset
    // cannot express. Delta's own schema evolution alone would leave those values in place.
    //
    // Where the dataset also carries fields the table lacks, that schema is the union of the two,
    // and the merge is permitted to evolve the table to it. The union is produced by the shared
    // structure merge, as every merged schema Pathling produces is (FR-057, FR-058). The merge
    // matches fields by name, so the order Delta stores an evolved table in is its own.
    final Dataset<Row> updates;
    if (widening) {
      final StructType union =
          StructureMerge.merge(
              targetSchema,
              dataset.schema(),
              DefinitionCanonicalStructure.forResource(
                  FhirDefinitionContext.of(context.getFhirContext()), resourceType));
      updates = conformToTarget(dataset, union);
    } else if (!sourcePaths.containsAll(targetPaths)) {
      updates = conformToTarget(dataset, targetSchema);
    } else {
      updates = dataset;
    }

    // Perform a merge operation where we match on the 'id' column.
    final DeltaMergeBuilder builder =
        table.as("original").merge(updates.as("updates"), "original.id = updates.id");
    (widening ? builder.withSchemaEvolution() : builder)
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute();
  }

  /**
   * Presents the dataset under the target schema, supplying as nulls the fields the target carries
   * and the dataset does not.
   *
   * <p>Only the parts of the dataset that differ from the target are rebuilt; a subtree whose type
   * already matches is passed straight through.
   *
   * @param dataset the dataset to project, whose fields must be a subset of the target's
   * @param target the schema to present it under
   * @return the projected dataset
   */
  @Nonnull
  private static Dataset<Row> conformToTarget(
      @Nonnull final Dataset<Row> dataset, @Nonnull final StructType target) {
    return dataset.select(conformFields(dataset::col, dataset.schema(), target));
  }

  /** Projects a column onto a target type, resolving struct fields by name at every depth. */
  @Nonnull
  private static Column conformColumn(
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
          .otherwise(struct(conformFields(column::getField, source, target)));
    }
    if (sourceType instanceof final ArrayType source
        && targetType instanceof final ArrayType target) {
      return transform(
          column, element -> conformColumn(element, source.elementType(), target.elementType()));
    }
    if (sourceType instanceof final MapType source && targetType instanceof final MapType target) {
      return transform_values(
          column, (key, value) -> conformColumn(value, source.valueType(), target.valueType()));
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
  private static Column[] conformFields(
      @Nonnull final Function<String, Column> fieldAccessor,
      @Nonnull final StructType source,
      @Nonnull final StructType target) {
    return Arrays.stream(target.fields())
        .map(
            field -> {
              final StructField sourceField = fieldOrNull(source, field.name());
              return (sourceField == null
                      ? lit(null).cast(field.dataType())
                      : conformColumn(
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
   * side. A difference of case alone therefore reads as a difference of field, so the merge is left
   * to whatever Delta would have done with it rather than being quietly conformed - which is the
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

  /**
   * Collects the path of every field in the schema, at every level of nesting.
   *
   * <p>Array elements and map values contribute a marker to the path rather than a name, so that a
   * field of a struct within an array cannot be confused with a field of a struct of the same name
   * held directly.
   *
   * @param schema the schema to walk
   * @return the set of field paths
   */
  @Nonnull
  private static Set<String> fieldPaths(@Nonnull final StructType schema) {
    final Set<String> paths = new HashSet<>();
    collectFieldPaths("", schema, paths);
    return paths;
  }

  private static void collectFieldPaths(
      @Nonnull final String prefix,
      @Nonnull final DataType dataType,
      @Nonnull final Set<String> paths) {
    if (dataType instanceof final StructType struct) {
      for (final StructField field : struct.fields()) {
        final String path = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
        paths.add(path);
        collectFieldPaths(path, field.dataType(), paths);
      }
    } else if (dataType instanceof final ArrayType array) {
      collectFieldPaths(prefix + "[]", array.elementType(), paths);
    } else if (dataType instanceof final MapType map) {
      collectFieldPaths(prefix + "{}", map.valueType(), paths);
    }
  }

  /**
   * Checks if a Delta table exists at the specified path.
   *
   * @param tablePath the path to the table to check
   * @return true if the Delta table exists, false otherwise
   */
  private boolean deltaTableExists(@Nonnull final String tablePath) {
    return DeltaTable.isDeltaTable(context.getSpark(), tablePath);
  }
}
