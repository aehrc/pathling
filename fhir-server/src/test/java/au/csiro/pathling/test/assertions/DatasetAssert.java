/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static au.csiro.pathling.test.assertions.Assertions.assertDatasetAgainstCsv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Storage;
import au.csiro.pathling.io.ResultWriter;
import au.csiro.pathling.test.builders.DatasetBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
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

  @Nonnull
  private final Dataset<Row> dataset;

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
    try {
      Files.delete(Path.of(location, name + ".csv"));
    } catch (final IOException e) {
      log.info("Existing file not found, skipping delete");
    }

    final Configuration configuration = new Configuration();
    final Storage storage = new Storage();
    storage.setResultUrl("file://" + location);
    configuration.setStorage(storage);
    final ResultWriter resultWriter = new ResultWriter(configuration, spark);
    resultWriter.write(dataset, Optional.of(name), SaveMode.Overwrite);

    try {
      Files.delete(Path.of(location, "." + name + ".csv.crc"));
    } catch (final IOException e) {
      log.info("CRC file not found, skipping delete");
    }
   
    return this;
  }
}
