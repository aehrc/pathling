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
 * Layer B regression guard for issue #2594. For every {@link QuantityValue} method that references
 * its {@code quantityColumn} operand, asserts that wrapping the operand in a {@link
 * TraceExpression} produces exactly one trace fire per row. Catches any future implementation that
 * re-introduces a multi-reference pattern over the Quantity struct column.
 */
@SpringBootUnitTest
class QuantityValueTraceTest {

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
  void toUnit_singleFire() {
    // QuantityValue.toUnit() references quantityColumn 5× in its when().otherwise() expression:
    // literal.unit(), isUcum(), isCalendarDuration(), callUDF(quantityColumn), and the value
    // branch. Without let()-wrapping, a traced operand fires 5× per row.
    final int beforeCount = appender.list.size();
    final Column tracedQty = traceColumn(QuantityEncoding.encodeNumeric(lit(1)), "toUnit");
    final Column result = QuantityValue.of(tracedQty).toUnit(lit("1"));
    singleRowDataset().select(result.alias("r")).collect();
    final long fires = countTraceLogs("toUnit", beforeCount);
    assertEquals(
        1, fires, () -> "Expected 1 trace fire for toUnit but got " + fires + ". See issue #2594.");
  }

  @Test
  void convertibleToUnit_singleFire() {
    // QuantityValue.convertibleToUnit() references quantityColumn 5× in its when() expression:
    // literal.unit(), isUcum(), isCalendarDuration(), callUDF(quantityColumn), and
    // quantityColumn.isNotNull(). Without let()-wrapping, a traced operand fires 5× per row.
    final int beforeCount = appender.list.size();
    final Column tracedQty =
        traceColumn(QuantityEncoding.encodeNumeric(lit(1)), "convertibleToUnit");
    final Column result = QuantityValue.of(tracedQty).convertibleToUnit(lit("1"));
    singleRowDataset().select(result.alias("r")).collect();
    final long fires = countTraceLogs("convertibleToUnit", beforeCount);
    assertEquals(
        1,
        fires,
        () ->
            "Expected 1 trace fire for convertibleToUnit but got " + fires + ". See issue #2594.");
  }

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
  private static Column traceColumn(@Nonnull final Column input, @Nonnull final String label) {
    return column(new TraceExpression(expression(input), label, "Quantity", null));
  }
}
