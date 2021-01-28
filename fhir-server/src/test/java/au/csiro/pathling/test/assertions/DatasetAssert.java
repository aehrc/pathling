/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static au.csiro.pathling.test.assertions.Assertions.assertDatasetAgainstCsv;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.test.builders.DatasetBuilder;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
public class DatasetAssert {

  @Nonnull
  private final Dataset<Row> dataset;

  public DatasetAssert(@Nonnull final Dataset<Row> dataset) {
    this.dataset = dataset;
  }


  @Nonnull
  public Dataset<Row> getDataset() {
    return dataset;
  }

  @Nonnull
  public DatasetAssert explain() {
    dataset.explain();
    return this;
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
  public DatasetAssert hasRows(@Nonnull final String expectedCsvPath) {
    assertDatasetAgainstCsv(expectedCsvPath, dataset);
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
  @SuppressWarnings("unused")
  public DatasetAssert debugAllRows() {
    dataset.collectAsList().forEach(System.out::println);
    return this;
  }

  @Nonnull
  @SuppressWarnings("unused")
  public DatasetAssert saveAllRowsToCsv(final String path) {
    dataset.coalesce(1).write().mode(SaveMode.Overwrite).csv(path);
    return this;
  }
}
