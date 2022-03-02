/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static au.csiro.pathling.test.assertions.Assertions.assertDatasetAgainstCsv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Storage;
import au.csiro.pathling.io.ResultWriter;
import au.csiro.pathling.test.builders.DatasetBuilder;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
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
    assertTrue(actualRows.containsAll(expected));
    assertTrue(expected.containsAll(actualRows));
    assertEquals(expected.size(), actualRows.size());
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

  @SuppressWarnings("UnusedReturnValue")
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
      @Nonnull final String location, @Nonnull final String name) throws IOException {
    final Path path = Path.of(location, name + ".csv");

    try {
      Files.delete(path);
    } catch (final IOException e) {
      log.info("Existing file not found, skipping delete");
    }

    final Configuration configuration = new Configuration();
    final Storage storage = new Storage();
    storage.setWarehouseUrl("file://" + location);
    configuration.setStorage(storage);
    final ResultWriter resultWriter = new ResultWriter(configuration, spark);
    resultWriter.write(dataset, name, SaveMode.Overwrite);
    final Path tempPath = Path.of(location, "results", name + ".csv");
    Files.copy(tempPath, path);

    try {
      Files.walkFileTree(Path.of(location, "results"), new DeleteDirectoryVisitor());
    } catch (final IOException e) {
      log.error("Problem cleaning up", e);
    }

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
