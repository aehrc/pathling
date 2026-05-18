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

package au.csiro.pathling.fhirpath.function.provider;

import static org.apache.spark.sql.classic.ExpressionUtils.column;
import static org.apache.spark.sql.classic.ExpressionUtils.expression;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.sql.TraceExpression;
import au.csiro.pathling.test.SpringBootUnitTest;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
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
 * Layer-B regression guard for issue #2594. Asserts that wrapping {@code value} in a {@link
 * TraceExpression} and passing it to {@link ValidationLogic#validateConversionToBoolean} produces
 * exactly one trace fire per row. Without the fix the STRING case references {@code value} three
 * times (two equality checks + the outer isNotNull), the INTEGER and DECIMAL cases reference it
 * twice each.
 */
@SpringBootUnitTest
class ValidationLogicTraceTest {

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

  @Test
  void validateConversionToBoolean_stringCase_singleFire() {
    final int before = appender.list.size();
    final Column tracedValue = traceColumn(col("v"), "vb-str");
    final Column result =
        ValidationLogic.validateConversionToBoolean(FhirPathType.STRING, tracedValue);
    stringDataset(1).select(result.alias("r")).collect();
    final long fires = countTraceLogs("vb-str", before);
    assertEquals(1, fires, () -> "value fired " + fires + "× (expected 1). See issue #2594.");
  }

  @Test
  void validateConversionToBoolean_stringCase_multiRowSingleFire() {
    final int before = appender.list.size();
    final Column tracedValue = traceColumn(col("v"), "vb-str-n");
    final Column result =
        ValidationLogic.validateConversionToBoolean(FhirPathType.STRING, tracedValue);
    stringDataset(3).select(result.alias("r")).collect();
    final long fires = countTraceLogs("vb-str-n", before);
    assertEquals(3, fires, () -> "value fired " + fires + "× for 3 rows (expected 3). See #2594.");
  }

  @Test
  void validateConversionToBoolean_integerCase_singleFire() {
    final int before = appender.list.size();
    final Column tracedValue = traceColumn(col("v").cast("integer"), "vb-int");
    final Column result =
        ValidationLogic.validateConversionToBoolean(FhirPathType.INTEGER, tracedValue);
    intDataset(1).select(result.alias("r")).collect();
    final long fires = countTraceLogs("vb-int", before);
    assertEquals(1, fires, () -> "value fired " + fires + "× (expected 1). See issue #2594.");
  }

  @Test
  void validateConversionToBoolean_decimalCase_singleFire() {
    final int before = appender.list.size();
    final Column tracedValue = traceColumn(col("v").cast("double"), "vb-dec");
    final Column result =
        ValidationLogic.validateConversionToBoolean(FhirPathType.DECIMAL, tracedValue);
    intDataset(1).select(result.alias("r")).collect();
    final long fires = countTraceLogs("vb-dec", before);
    assertEquals(1, fires, () -> "value fired " + fires + "× (expected 1). See issue #2594.");
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
  private Dataset<Row> stringDataset(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("v", DataTypes.StringType, false, Metadata.empty())
            });
    return spark.createDataFrame(
        java.util.stream.IntStream.rangeClosed(1, rows)
            .mapToObj(i -> RowFactory.create(i, "true"))
            .toList(),
        schema);
  }

  @Nonnull
  private Dataset<Row> intDataset(final int rows) {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("v", DataTypes.IntegerType, false, Metadata.empty())
            });
    return spark.createDataFrame(
        java.util.stream.IntStream.rangeClosed(1, rows)
            .mapToObj(i -> RowFactory.create(i, 1))
            .toList(),
        schema);
  }

  @Nonnull
  private static Column traceColumn(@Nonnull final Column input, @Nonnull final String label) {
    return column(new TraceExpression(expression(input), label, "value", null));
  }
}
