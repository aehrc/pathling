/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.utilities.Datasets;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.commons.io.file.SimplePathVisitor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@Slf4j
@SuppressWarnings("UnusedReturnValue")
public class DatasetAssert {

  public static boolean LOG_PHYSICAL_PLAN =
      Boolean.parseBoolean(System.getProperty("pathling.test.ds.logPhysicalPlan", "false"));

  public static boolean LOG_DATASET =
      Boolean.parseBoolean(System.getProperty("pathling.test.ds.logRows", "false"));

  public static void logDataset(@Nonnull final Dataset<Row> dataset) {
    if (LOG_PHYSICAL_PLAN) {
      log.info("Physical plan:\n {}", dataset.queryExecution().executedPlan().toString());
    }
    if (LOG_DATASET) {
      log.info("Dataset:");
      if (log.isInfoEnabled()) {
        // OK: show allowed here
        dataset.show();
      }
    }
  }

  public static DatasetAssert of(@Nonnull final Dataset<Row> dataset) {
    return new DatasetAssert(dataset);
  }

  @Getter
  @Nonnull
  private Dataset<Row> dataset;

  public DatasetAssert(@Nonnull final Dataset<Row> dataset) {
    this.dataset = dataset;
    logDataset(dataset);
  }

  @Nonnull
  public DatasetAssert hasRows(@Nonnull final List<Row> expected) {
    final List<Row> actualRows = dataset.collectAsList();
    assertEquals(expected, actualRows);
    return this;
  }

  @Nonnull
  public DatasetAssert hasRows(@Nonnull final DatasetBuilder expected) {
    return hasRows(expected.build());
  }

  @Nonnull
  public DatasetAssert hasRows(@Nonnull final Row... expected) {
    return hasRows(Arrays.asList(expected));
  }

  @Nonnull
  public DatasetAssert hasRows(@Nonnull final Dataset<Row> expected) {
    return hasRows(expected.collectAsList());
  }

  @Nonnull
  public DatasetAssert hasRows(@Nonnull final SparkSession spark,
      @Nonnull final String expectedCsvPath) {
    return hasRows(spark, expectedCsvPath, false);
  }

  public DatasetAssert hasRows(@Nonnull final SparkSession spark,
      @Nonnull final String expectedCsvPath, final boolean header) {
    Assertions.assertDatasetAgainstTsv(spark, expectedCsvPath, dataset, header);
    return this;
  }

  @Nonnull
  public DatasetAssert hasRowsUnordered(@Nonnull final Row... expected) {
    return hasRowsUnordered(Arrays.asList(expected));
  }

  private static class MyMultiSet<T> extends HashMultiSet<T> {

    public MyMultiSet(@Nonnull final Collection<T> collection) {
      super(collection);
    }

    @Override
    public String toString() {
      return entrySet().stream()
          .map(Object::toString)
          .sorted()
          .collect(Collectors.joining("\n"));
    }
  }

  @Nonnull
  private DatasetAssert hasRowsUnordered(@Nonnull final Collection<Row> expected) {
    if (expected.isEmpty() && dataset.isEmpty()) {
      return this;
    }
    final MultiSet<Row> actualRows = new MyMultiSet<>(dataset.collectAsList());
    final MultiSet<Row> expectedRows = new MyMultiSet<>(expected);
    assertEquals(expectedRows, actualRows);
    return this;
  }

  @Nonnull
  @SuppressWarnings("UnusedReturnValue")
  public DatasetAssert hasRowsUnordered(@Nonnull final Dataset<Row> expected) {
    return hasRowsUnordered(expected.collectAsList());
  }

  @Nonnull
  public DatasetAssert hasRowsAndColumnsUnordered(@Nonnull final Dataset<Row> expected) {
    if (expected.isEmpty() && dataset.isEmpty()) {
      return this;
    }
    // First, get the list of columns from the expected and actual datasets, sort them and assert
    // that they are equal.
    final List<String> expectedColumns = Arrays.asList(expected.columns());
    final List<String> actualColumns = Arrays.asList(dataset.columns());
    expectedColumns.sort(String::compareTo);
    actualColumns.sort(String::compareTo);
    assertEquals(expectedColumns, actualColumns);

    // Then re-project the expected and actual datasets using the ordered list of columns.
    final Dataset<Row> expectedReprojected = expected.select(expectedColumns.stream()
        .map(expected::col).toArray(Column[]::new));
    final Dataset<Row> actualReprojected = dataset.select(actualColumns.stream()
        .map(dataset::col).toArray(Column[]::new));

    // Finally, assert that the re-projected datasets are equal.
    new DatasetAssert(actualReprojected).hasRowsUnordered(expectedReprojected);
    return this;
  }

  @Nonnull
  public DatasetAssert rowsAreAllNotEqual(@Nonnull final List<Row> expected) {
    final List<Row> actualRows = dataset.collectAsList();
    for (int i = 0; i < expected.size(); i++) {
      final Row expectedRow = expected.get(i);
      final Row actualRow = actualRows.get(i);
      assertNotEquals(expectedRow, actualRow);
    }
    return this;
  }

  @SuppressWarnings({"UnusedReturnValue", "unused"})
  @Nonnull
  public DatasetAssert rowsAreAllNotEqual(@Nonnull final Dataset<Row> expected) {
    return rowsAreAllNotEqual(expected.collectAsList());
  }

  @Nonnull
  public DatasetAssert apply(@Nonnull final UnaryOperator<Dataset<Row>> operator) {
    dataset = operator.apply(dataset);
    return this;
  }

  @Nonnull
  @SuppressWarnings("unused")
  public DatasetAssert debugSchema() {
    dataset.printSchema();
    return this;
  }

  @Nonnull
  @SuppressWarnings("unused")
  public DatasetAssert debugRows() {
    // OK: show allowed here
    dataset.show();
    return this;
  }

  @Nonnull
  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public DatasetAssert debugAllRows() {
    dataset.collectAsList().forEach(row -> System.out.println(row.mkString(",")));
    return this;
  }

  @Nonnull
  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public DatasetAssert saveAllRowsToCsv(@Nonnull final SparkSession spark,
      @Nonnull final String location, @Nonnull final String name) {
    final Path path = Path.of(location, name + ".csv");

    try {
      Files.delete(path);
    } catch (final IOException e) {
      log.info("Existing file not found, skipping delete");
    }
    Datasets.writeCsv(dataset, path.toUri().toString(), SaveMode.Overwrite);
    throw new AssertionError(
        "Rows saved to CSV, check that the file is correct and replace this line with an assertion");
  }


  @Nonnull
  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public DatasetAssert printAsTsv() {
    dataset.select(
            functions.array_join(
                functions.array(
                    Stream.of(dataset.columns()).map(functions::col).toArray(Column[]::new)), "\t"
            ).as("value"))
        .as(Encoders.STRING())
        .collectAsList()
        .forEach(System.out::println);
    return this;
  }


  private static class DeleteDirectoryVisitor extends SimplePathVisitor {
    
    @Override
    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
        throws IOException {
      Files.delete(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(final Path dir, final IOException exc)
        throws IOException {
      Files.delete(dir);
      return FileVisitResult.CONTINUE;
    }
  }
}
