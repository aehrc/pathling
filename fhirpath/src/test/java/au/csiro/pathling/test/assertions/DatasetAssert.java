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

import static au.csiro.pathling.test.assertions.Assertions.assertDatasetAgainstCsv;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.file.SimplePathVisitor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@Slf4j
@SuppressWarnings("UnusedReturnValue")
public class DatasetAssert {

  public static DatasetAssert of(@Nonnull final Dataset<Row> dataset) {
    return new DatasetAssert(dataset);
  }

  @Getter
  @Nonnull
  private Dataset<Row> dataset;

  public DatasetAssert(@Nonnull final Dataset<Row> dataset) {
    this.dataset = dataset;
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
    assertDatasetAgainstCsv(spark, expectedCsvPath, dataset);
    return this;
  }

  @Nonnull
  private DatasetAssert hasRowsUnordered(@Nonnull final Collection<Row> expected) {
    final List<Row> actualRows = dataset.collectAsList();
    assertEquals(expected.size(), actualRows.size());

    if (!actualRows.containsAll(expected)) {
      return Assertions.fail("Some rows are missing.", expected, actualRows);
    }
    if (!expected.containsAll(actualRows)) {
      return Assertions.fail("Unexpected rows found.", expected, actualRows);
    }
    return this;
  }

  @Nonnull
  @SuppressWarnings("UnusedReturnValue")
  public DatasetAssert hasRowsUnordered(@Nonnull final Dataset<Row> expected) {
    return hasRowsUnordered(expected.collectAsList());
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
    dataset.show();
    return this;
  }

  @Nonnull
  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public DatasetAssert debugAllRows() {
    dataset.collectAsList().forEach(System.out::println);
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
