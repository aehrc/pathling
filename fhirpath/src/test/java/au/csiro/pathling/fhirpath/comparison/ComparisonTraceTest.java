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

package au.csiro.pathling.fhirpath.comparison;

import static org.apache.spark.sql.classic.ExpressionUtils.column;
import static org.apache.spark.sql.classic.ExpressionUtils.expression;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.TraceExpression;
import au.csiro.pathling.test.SpringBootUnitTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.util.List;
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
 * Layer-B regression guard for issue #2594. Asserts that wrapping operand columns in a {@link
 * TraceExpression} produces exactly one trace fire per row for each comparator that previously
 * referenced its inputs multiple times.
 */
@SpringBootUnitTest
class ComparisonTraceTest {

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
  // ArrayElementWiseColumnEquality — left and right each referenced twice:
  // once in zip_with() and once in size().
  // ---------------------------------------------------------------------------

  @Test
  void arrayElementWise_equalsTo_leftSingleFire() {
    final int before = appender.list.size();
    final Column tracedLeft = traceColumn(col("left"), "aew-left");
    final Column result =
        new ArrayElementWiseColumnEquality(DefaultComparator.getInstance())
            .equalsTo(tracedLeft, col("right"));
    intArrayDataset(1).select(result.alias("r")).collect();
    final long fires = countTraceLogs("aew-left", before);
    assertEquals(1, fires, () -> "left fired " + fires + "× (expected 1). See issue #2594.");
  }

  @Test
  void arrayElementWise_equalsTo_rightSingleFire() {
    final int before = appender.list.size();
    final Column tracedRight = traceColumn(col("right"), "aew-right");
    final Column result =
        new ArrayElementWiseColumnEquality(DefaultComparator.getInstance())
            .equalsTo(col("left"), tracedRight);
    intArrayDataset(1).select(result.alias("r")).collect();
    final long fires = countTraceLogs("aew-right", before);
    assertEquals(1, fires, () -> "right fired " + fires + "× (expected 1). See issue #2594.");
  }

  @Test
  void arrayElementWise_equalsTo_leftMultiRowSingleFire() {
    final int before = appender.list.size();
    final Column tracedLeft = traceColumn(col("left"), "aew-left-n");
    final Column result =
        new ArrayElementWiseColumnEquality(DefaultComparator.getInstance())
            .equalsTo(tracedLeft, col("right"));
    intArrayDataset(3).select(result.alias("r")).collect();
    final long fires = countTraceLogs("aew-left-n", before);
    assertEquals(3, fires, () -> "left fired " + fires + "× for 3 rows (expected 3). See #2594.");
  }

  // ---------------------------------------------------------------------------
  // QuantityComparator — left and right each referenced twice via normalizedValue()
  // and originalValue() field accesses.
  // ---------------------------------------------------------------------------

  @Test
  void quantityComparator_equalsTo_leftSingleFire() {
    final int before = appender.list.size();
    final Column qty = QuantityEncoding.encodeNumeric(lit(1));
    final Column tracedLeft = traceColumn(qty, "qty-left");
    final Column right = QuantityEncoding.encodeNumeric(lit(1));
    final Column result = QuantityComparator.getInstance().equalsTo(tracedLeft, right);
    singleRowDataset().select(result.alias("r")).collect();
    final long fires = countTraceLogs("qty-left", before);
    assertEquals(1, fires, () -> "left fired " + fires + "× (expected 1). See issue #2594.");
  }

  @Test
  void quantityComparator_equalsTo_rightSingleFire() {
    final int before = appender.list.size();
    final Column qty = QuantityEncoding.encodeNumeric(lit(1));
    final Column left = QuantityEncoding.encodeNumeric(lit(1));
    final Column tracedRight = traceColumn(qty, "qty-right");
    final Column result = QuantityComparator.getInstance().equalsTo(left, tracedRight);
    singleRowDataset().select(result.alias("r")).collect();
    final long fires = countTraceLogs("qty-right", before);
    assertEquals(1, fires, () -> "right fired " + fires + "× (expected 1). See issue #2594.");
  }

  // ---------------------------------------------------------------------------
  // TemporalComparator — left and right each referenced twice via the two
  // callUDF() calls inside getBounds() (one for low boundary, one for high).
  // ---------------------------------------------------------------------------

  @Test
  void temporalComparator_equalsTo_leftSingleFire() {
    final int before = appender.list.size();
    final Column tracedLeft = traceColumn(col("dt"), "temp-left");
    final Column result = TemporalComparator.forDateTime().equalsTo(tracedLeft, lit("2023-01-15"));
    datetimeDataset(1).select(result.alias("r")).collect();
    final long fires = countTraceLogs("temp-left", before);
    assertEquals(1, fires, () -> "left fired " + fires + "× (expected 1). See issue #2594.");
  }

  @Test
  void temporalComparator_equalsTo_rightSingleFire() {
    final int before = appender.list.size();
    final Column tracedRight = traceColumn(col("dt"), "temp-right");
    final Column result = TemporalComparator.forDateTime().equalsTo(lit("2023-01-15"), tracedRight);
    datetimeDataset(1).select(result.alias("r")).collect();
    final long fires = countTraceLogs("temp-right", before);
    assertEquals(1, fires, () -> "right fired " + fires + "× (expected 1). See issue #2594.");
  }

  @Test
  void temporalComparator_equalsTo_leftMultiRowSingleFire() {
    final int before = appender.list.size();
    final Column tracedLeft = traceColumn(col("dt"), "temp-left-n");
    final Column result = TemporalComparator.forDateTime().equalsTo(tracedLeft, lit("2023-01-15"));
    datetimeDataset(3).select(result.alias("r")).collect();
    final long fires = countTraceLogs("temp-left-n", before);
    assertEquals(3, fires, () -> "left fired " + fires + "× for 3 rows (expected 3). See #2594.");
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private long countTraceLogs(@Nonnull final String label, final int fromIndex) {
    final String marker = "[trace:" + label + "]";
    return appender.list.subList(fromIndex, appender.list.size()).stream()
        .filter(event -> event.getFormattedMessage().contains(marker))
        .count();
  }

  @Nonnull
  private Dataset<Row> singleRowDataset() {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty())
            });
    return spark.createDataFrame(List.of(RowFactory.create(1)), schema);
  }

  @Nonnull
  private Dataset<Row> intArrayDataset(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "left", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty()),
              new StructField(
                  "right", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty())
            });
    return spark.createDataFrame(
        java.util.stream.IntStream.rangeClosed(1, rows)
            .mapToObj(i -> RowFactory.create(i, new Integer[] {i, i + 1}, new Integer[] {i, i + 1}))
            .toList(),
        schema);
  }

  @Nonnull
  private Dataset<Row> datetimeDataset(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("dt", DataTypes.StringType, false, Metadata.empty())
            });
    return spark.createDataFrame(
        java.util.stream.IntStream.rangeClosed(1, rows)
            .mapToObj(i -> RowFactory.create(i, "2023-01-" + String.format("%02d", i)))
            .toList(),
        schema);
  }

  @Nonnull
  private static Column traceColumn(@Nonnull final Column input, @Nonnull final String label) {
    return column(new TraceExpression(expression(input), label, "value", null));
  }
}
