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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.classic.ExpressionUtils.column;
import static org.apache.spark.sql.classic.ExpressionUtils.expression;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.sql.TraceExpression;
import au.csiro.pathling.test.SpringBootUnitTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Layer B regression guard for issue #2594. For every public {@link ColumnRepresentation} method
 * that operates on its operand, asserts that wrapping the operand in a {@link TraceExpression}
 * produces exactly one trace fire per logical invocation (per row). Catches any future helper that
 * re-introduces a multi-reference {@code when(...).otherwise(...)} pattern over the operand.
 */
@SpringBootUnitTest
class ColumnRepresentationTraceTest {

  @Autowired SparkSession spark;

  private Logger traceLogger;
  private Level originalLevel;
  private ListAppender<ILoggingEvent> appender;

  @BeforeEach
  void setUp() {
    traceLogger = (Logger) LoggerFactory.getLogger(TraceExpression.class);
    originalLevel = traceLogger.getLevel();
    traceLogger.setLevel(Level.TRACE);
    appender = new ListAppender<>();
    appender.start();
    traceLogger.addAppender(appender);
  }

  @AfterEach
  void tearDown() {
    traceLogger.detachAppender(appender);
    traceLogger.setLevel(originalLevel);
    appender.stop();
  }

  // ---------------------------------------------------------------------------
  // Methods rewritten in this change — should fire exactly once per logical row.
  // ---------------------------------------------------------------------------

  @Test
  void count_array_singleFire() {
    runArray("count-array", ColumnRepresentation::count, 1, 3);
  }

  @Test
  void isEmpty_array_singleFire() {
    runArray("isEmpty-array", ColumnRepresentation::isEmpty, 1, 3);
  }

  @Test
  void last_array_singleFire() {
    runArray("last", ColumnRepresentation::last, 1, 3);
  }

  @Test
  void normaliseNull_array_singleFire() {
    runArray("normaliseNull", ColumnRepresentation::normaliseNull, 1, 3);
  }

  @Test
  void join_array_singleFire() {
    // join() requires a string array; uses a dedicated string-array dataset.
    runStringArray("join", c -> c.join(new DefaultRepresentation(lit(" "))), 1, 3);
  }

  @Test
  void aggregate_array_singleFire() {
    runArray("aggregate-array", c -> c.aggregate(0, Column::plus), 1, 3);
  }

  @Test
  void aggregate_scalar_singleFire() {
    runScalar("aggregate-scalar", c -> c.aggregate(0, Column::plus), 1, 3);
  }

  @Test
  void plural_array_singleFire() {
    runArray("plural-array", ColumnRepresentation::plural, 1, 3);
  }

  @Test
  void plural_scalar_singleFire() {
    runScalar("plural-scalar", ColumnRepresentation::plural, 1, 3);
  }

  @Test
  void singular_array_singleFire() {
    // Each row is a singleton array so size never exceeds 1 and raise_error is not triggered.
    runArrayOfSingleton("singular", ColumnRepresentation::singular, 1, 3);
  }

  @Test
  void filter_array_singleFire() {
    runArray("filter-array", c -> c.filter(x -> x.gt(0)), 1, 3);
  }

  @Test
  void filter_scalar_singleFire() {
    runScalar("filter-scalar", c -> c.filter(x -> x.gt(0)), 1, 3);
  }

  @Test
  void toArray_scalar_singleFire() {
    runScalar("toArray-scalar", ColumnRepresentation::toArray, 1, 3);
  }

  @Test
  void transform_scalar_singleFire() {
    runScalar("transform-scalar", c -> c.transform(Column::unary_$minus), 1, 3);
  }

  @Test
  void contains_array_element_singleFire() {
    runContains("contains-array-element", arrayDataset(1), 1);
    runContains("contains-array-element", arrayDataset(3), 3);
  }

  @Test
  void contains_scalar_element_singleFire() {
    runContains("contains-scalar-element", scalarDataset(1), 1);
    runContains("contains-scalar-element", scalarDataset(3), 3);
  }

  // ---------------------------------------------------------------------------
  // Methods that already use the operand once — sanity-guard against drift.
  // ---------------------------------------------------------------------------

  @Test
  void first_array_singleFire() {
    runArray("first", ColumnRepresentation::first, 1, 3);
  }

  @Test
  void orElse_singleFire() {
    runScalar("orElse", c -> c.orElse(0), 1, 3);
  }

  @Test
  void ensureSingular_singleFire() {
    runArrayOfSingleton("ensureSingular", c -> new DefaultRepresentation(c.ensureSingular()), 1, 3);
  }

  @Test
  void removeNulls_array_singleFire() {
    runArray("removeNulls", ColumnRepresentation::removeNulls, 1, 3);
  }

  @Test
  void count_scalar_singleFire() {
    runScalar("count-scalar", ColumnRepresentation::count, 1, 3);
  }

  @Test
  void isEmpty_scalar_singleFire() {
    runScalar("isEmpty-scalar", ColumnRepresentation::isEmpty, 1, 3);
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private void runArray(
      @Nonnull final String label,
      @Nonnull final Function<ColumnRepresentation, ColumnRepresentation> op,
      final long expectedSingleRowFires,
      final long expectedMultiRowFires) {
    runCase(arrayDataset(1), label + "-1", op, expectedSingleRowFires);
    runCase(arrayDataset(3), label + "-3", op, expectedMultiRowFires);
  }

  private void runStringArray(
      @Nonnull final String label,
      @Nonnull final Function<ColumnRepresentation, ColumnRepresentation> op,
      final long expectedSingleRowFires,
      final long expectedMultiRowFires) {
    runCase(stringArrayDataset(1), label + "-1", op, expectedSingleRowFires);
    runCase(stringArrayDataset(3), label + "-3", op, expectedMultiRowFires);
  }

  private void runArrayOfSingleton(
      @Nonnull final String label,
      @Nonnull final Function<ColumnRepresentation, ColumnRepresentation> op,
      final long expectedSingleRowFires,
      final long expectedMultiRowFires) {
    runCase(arrayDatasetOfSingleton(1), label + "-1", op, expectedSingleRowFires);
    runCase(arrayDatasetOfSingleton(3), label + "-3", op, expectedMultiRowFires);
  }

  private void runScalar(
      @Nonnull final String label,
      @Nonnull final Function<ColumnRepresentation, ColumnRepresentation> op,
      final long expectedSingleRowFires,
      final long expectedMultiRowFires) {
    runCase(scalarDataset(1), label + "-1", op, expectedSingleRowFires);
    runCase(scalarDataset(3), label + "-3", op, expectedMultiRowFires);
  }

  // Unlike runCase, the trace here is on the element argument, not the collection, matching the
  // let() boundary inside ColumnRepresentation.contains().
  private void runContains(
      @Nonnull final String label, @Nonnull final Dataset<Row> df, final long expected) {
    final int beforeCount = appender.list.size();
    final Column tracedElement = traceColumn(lit(1), label);
    final ColumnRepresentation element = new DefaultRepresentation(tracedElement);
    final ColumnRepresentation collection = new DefaultRepresentation(col("v"));
    final Column result = collection.contains(element, Column::equalTo).getValue();
    df.select(result.alias("r")).collect();
    final long fires = countTraceLogs(label, beforeCount);
    assertEquals(
        expected,
        fires,
        () -> "Expected " + expected + " trace fires for " + label + " but got " + fires);
  }

  private void runCase(
      @Nonnull final Dataset<Row> df,
      @Nonnull final String label,
      @Nonnull final Function<ColumnRepresentation, ColumnRepresentation> op,
      final long expected) {
    final int beforeCount = appender.list.size();
    final Column traced = traceColumn(col("v"), label);
    final ColumnRepresentation rep = new DefaultRepresentation(traced);
    final Column result = op.apply(rep).getValue();
    df.select(result.alias("r")).collect();
    final long fires = countTraceLogs(label, beforeCount);
    assertEquals(
        expected,
        fires,
        () -> "Expected " + expected + " trace fires for " + label + " but got " + fires);
  }

  private long countTraceLogs(@Nonnull final String label, final int fromIndex) {
    final String marker = "[trace:" + label + "]";
    return appender.list.subList(fromIndex, appender.list.size()).stream()
        .filter(event -> event.getFormattedMessage().contains(marker))
        .count();
  }

  @Nonnull
  private Dataset<Row> scalarDataset(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("v", DataTypes.IntegerType, false, Metadata.empty())
            });
    return spark.createDataFrame(
        IntStream.rangeClosed(1, rows).mapToObj(i -> RowFactory.create(i, i)).toList(), schema);
  }

  @Nonnull
  private Dataset<Row> arrayDataset(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "v", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty())
            });
    return spark.createDataFrame(
        IntStream.rangeClosed(1, rows)
            .mapToObj(i -> RowFactory.create(i, new Integer[] {i, i + 1}))
            .toList(),
        schema);
  }

  @Nonnull
  private Dataset<Row> stringArrayDataset(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "v", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
            });
    return spark.createDataFrame(
        IntStream.rangeClosed(1, rows)
            .mapToObj(i -> RowFactory.create(i, new String[] {"a" + i, "b" + i}))
            .toList(),
        schema);
  }

  @Nonnull
  private Dataset<Row> arrayDatasetOfSingleton(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "v", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty())
            });
    return spark.createDataFrame(
        IntStream.rangeClosed(1, rows)
            .mapToObj(i -> RowFactory.create(i, new Integer[] {i}))
            .toList(),
        schema);
  }

  @Nonnull
  private static Column traceColumn(@Nonnull final Column input, @Nonnull final String label) {
    return column(new TraceExpression(expression(input), label, "integer", null));
  }
}
