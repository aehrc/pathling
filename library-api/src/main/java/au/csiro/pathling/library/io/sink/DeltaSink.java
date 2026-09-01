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

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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

  @Override
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    final List<FileInformation> fileInfos = new ArrayList<>();
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String fileName = String.join(".", fileNameMapper.apply(resourceType), "parquet");
      final String tablePath = safelyJoinPaths(path, fileName);

      fileInfos.add(new FileInformation(resourceType, tablePath));

      switch (saveMode) {
        case ERROR_IF_EXISTS, APPEND, IGNORE, OVERWRITE ->
            writeDataset(dataset, tablePath, saveMode);
        case MERGE -> {
          if (deltaTableExists(tablePath)) {
            // If the table already exists, merge the data in.
            final DeltaTable table = DeltaTable.forPath(context.getSpark(), tablePath);
            merge(table, dataset);
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

    writer.save(tablePath);
  }

  /**
   * Merges the given dataset into the specified Delta table.
   *
   * <p>Where the table carries fields the dataset does not, the merge is permitted to leave those
   * fields null on the rows it writes, rather than failing. This is the state a warehouse is in
   * when it was written with more open types configured than the encoder now writing to it, which
   * is otherwise unwritable even though the data is perfectly readable.
   *
   * @param table the Delta table to merge into
   * @param dataset the dataset containing updates to be merged
   */
  static void merge(@Nonnull final DeltaTable table, @Nonnull final Dataset<Row> dataset) {
    final StructType targetSchema = table.toDF().schema();

    // Where the target carries fields the dataset does not, and the dataset carries none the target
    // does not, present the dataset under the target's own schema with those fields null. The merge
    // itself is then an ordinary aligned one, so a row it matches is replaced in full rather than
    // keeping the values the target happened to hold for the fields the source cannot express.
    //
    // Delta's own schema evolution was not used for this. It is the mechanism that makes the write
    // possible at all, but it couples two behaviours behind one flag - null-filling on insert, and
    // evolving the target to admit fields only the source carries - and it leaves a matched row's
    // other columns untouched rather than replacing the row.
    //
    // Where the two schemas differ in both directions at once, the dataset is passed through
    // unchanged and the merge fails as it otherwise would: widening the target is the caller's
    // decision, not this method's.
    final Dataset<Row> updates =
        isPurelyNarrowing(targetSchema, dataset.schema())
            ? conformToTarget(dataset, targetSchema)
            : dataset;

    // Perform a merge operation where we match on the 'id' column.
    table
        .as("original")
        .merge(updates.as("updates"), "original.id = updates.id")
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
   * Determines whether the source schema is a strict subset of the target schema, at every level of
   * nesting.
   *
   * @param target the schema of the table being merged into
   * @param source the schema of the dataset being merged
   * @return true if the target carries fields the source does not, and the source carries none the
   *     target does not
   */
  private static boolean isPurelyNarrowing(
      @Nonnull final StructType target, @Nonnull final StructType source) {
    final Set<String> targetPaths = fieldPaths(target);
    final Set<String> sourcePaths = fieldPaths(source);
    return targetPaths.containsAll(sourcePaths) && !sourcePaths.containsAll(targetPaths);
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
