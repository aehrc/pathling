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

import au.csiro.pathling.encoders.ResourceTypes;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * The command line of the legacy export.
 *
 * <pre>
 * legacy-export &lt;source&gt; &lt;parquet|delta&gt; &lt;output&gt; [--resource-type &lt;type&gt;]
 * </pre>
 *
 * <p>The source is either one table or a directory of tables. A table holds the resource type its
 * name starts with, up to the first full stop, as in {@code Patient.parquet}, unless {@code
 * --resource-type} names it. A source whose name is not a resource type, where no type is named, is
 * a directory, and every table in it whose name is a resource type is exported. The output is a
 * directory, which receives files named {@code <type>.<part>.ndjson}.
 *
 * <p>The command exits with 0 where every table is exported, 1 where an export fails, and 2 where
 * the arguments are not as the usage describes. Its messages are for the person running it, so they
 * are written to standard output and standard error rather than logged.
 *
 * @author Piotr Szul
 */
@Slf4j
public final class LegacyExportCommand {

  /** The exit status where every table is exported. */
  static final int SUCCESS = 0;

  /** The exit status where an export fails. */
  static final int FAILURE = 1;

  /** The exit status where the arguments are not as the usage describes. */
  static final int USAGE_ERROR = 2;

  @Nonnull
  static final String USAGE =
      String.join(
          System.lineSeparator(),
          "Usage: legacy-export <source> <parquet|delta> <output> [--resource-type <type>]",
          "",
          "Exports tables in the legacy layout, written by the Pathling FHIR encoders, to FHIR",
          "NDJSON in <output>, as files named <type>.<part>.ndjson.",
          "",
          "  <source>         A table, or a directory of tables. A table holds the resource type",
          "                   its name starts with, as in Patient.parquet.",
          "  <parquet|delta>  The format the tables are stored in.",
          "  <output>         The directory to write the NDJSON files into.",
          "  --resource-type  The resource type the table at <source> holds, where its name does",
          "                   not say.");

  private LegacyExportCommand() {}

  /**
   * Runs the command and exits with its status.
   *
   * @param args the arguments, as the usage describes
   */
  public static void main(@Nonnull final String[] args) {
    System.exit(run(args, System.out, System.err));
  }

  /**
   * Runs the command.
   *
   * @param args the arguments, as the usage describes
   * @param out where to report what was exported
   * @param err where to report what went wrong
   * @return the exit status
   */
  static int run(
      @Nonnull final String[] args,
      @Nonnull final PrintStream out,
      @Nonnull final PrintStream err) {
    final LegacyExportArguments arguments;
    try {
      arguments = LegacyExportArguments.parse(args);
    } catch (final IllegalArgumentException e) {
      err.println(e.getMessage());
      err.println(USAGE);
      return USAGE_ERROR;
    }
    try {
      final SparkSession spark = session(arguments.getFormat());
      final LegacyExport export = LegacyExport.of(spark);
      tables(spark, arguments, out)
          .forEach(
              (resourceType, table) -> {
                final List<String> written =
                    export.export(
                        resourceType, table, arguments.getFormat(), arguments.getOutput());
                out.println(
                    "Exported "
                        + resourceType
                        + " from "
                        + table
                        + " to "
                        + written.size()
                        + " files in "
                        + arguments.getOutput()
                        + ".");
              });
      return SUCCESS;
    } catch (final LegacyExportException e) {
      err.println(e.getMessage());
      return FAILURE;
    } catch (final RuntimeException e) {
      // Anything else that stops the export, such as a file that cannot be read part way through,
      // is reported as a failure of the command rather than escaping as a stack trace.
      err.println("The export failed: " + e);
      log.error("The export failed", e);
      return FAILURE;
    }
  }

  /**
   * Finds the tables a source names, keyed by the resource type each holds.
   *
   * @param spark the session whose file system configuration is used to list a directory
   * @param arguments the arguments
   * @param out where to report what is skipped
   * @return the location of each table, keyed by its resource type
   * @throws LegacyExportException if the source names no table
   */
  @Nonnull
  private static Map<String, String> tables(
      @Nonnull final SparkSession spark,
      @Nonnull final LegacyExportArguments arguments,
      @Nonnull final PrintStream out) {
    final String source = arguments.getSource();
    final Optional<String> named =
        arguments.getNamedResourceType().or(() -> resourceTypeOf(new Path(source)));
    if (named.isPresent()) {
      return Map.of(named.get(), source);
    }
    final Map<String, String> tables = new TreeMap<>();
    for (final FileStatus child : children(spark, new Path(source))) {
      final Path path = child.getPath();
      final Optional<String> resourceType = resourceTypeOf(path);
      if (resourceType.isEmpty()) {
        out.println("Skipped " + path + ", whose name is not a resource type.");
      } else if (tables.putIfAbsent(resourceType.get(), path.toString()) != null) {
        throw new LegacyExportException(
            source
                + " holds more than one table of "
                + resourceType.get()
                + ": "
                + tables.get(resourceType.get())
                + " and "
                + path
                + ". Export each with "
                + LegacyExportArguments.RESOURCE_TYPE_OPTION
                + ".");
      }
    }
    if (tables.isEmpty()) {
      throw new LegacyExportException(
          "The name of "
              + source
              + " is not a resource type, and it holds no table whose name is. Name the "
              + "resource type the table holds with "
              + LegacyExportArguments.RESOURCE_TYPE_OPTION
              + ".");
    }
    return tables;
  }

  /** The resource type a table's name starts with, up to the first full stop, if it is one. */
  @Nonnull
  private static Optional<String> resourceTypeOf(@Nonnull final Path path) {
    final String stem = path.getName().split("\\.", 2)[0];
    return ResourceTypes.isSupported(stem) ? Optional.of(stem) : Optional.empty();
  }

  @Nonnull
  private static List<FileStatus> children(
      @Nonnull final SparkSession spark, @Nonnull final Path directory) {
    try {
      final FileSystem fileSystem =
          directory.getFileSystem(spark.sparkContext().hadoopConfiguration());
      if (!fileSystem.exists(directory)) {
        throw new LegacyExportException("The source " + directory + " does not exist.");
      }
      return Arrays.stream(fileSystem.listStatus(directory))
          .filter(child -> !child.getPath().getName().startsWith("_"))
          .filter(child -> !child.getPath().getName().startsWith("."))
          .toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns the active Spark session, or creates one. Delta Lake is configured only to read Delta
   * tables, so that Parquet tables can be exported where it is not on the classpath. Where the
   * environment names no master, as {@code spark-submit} does, the session runs locally.
   */
  @Nonnull
  private static SparkSession session(@Nonnull final LegacyTableFormat format) {
    final SparkSession.Builder builder = SparkSession.builder().appName("Pathling legacy export");
    if (format == LegacyTableFormat.DELTA) {
      builder
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config(
              "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    }
    if (!new SparkConf().contains("spark.master")) {
      builder.master("local[*]");
    }
    return builder.getOrCreate();
  }
}
