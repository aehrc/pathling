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

package au.csiro.pathling.sql;

import static au.csiro.pathling.sql.SqlFunctions.let;
import static org.apache.spark.sql.classic.ExpressionUtils.column;
import static org.apache.spark.sql.classic.ExpressionUtils.expression;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Tests for {@link SqlFunctions#let(Column, java.util.function.UnaryOperator)}: identity behaviour,
 * multi-reference correctness, and single-fire semantics over a {@link TraceExpression} operand.
 */
@SpringBootUnitTest
class SqlFunctionsLetTest {

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
  void let_identityBody_returnsOperandValue_singleRow() {
    final Dataset<Row> df = spark.range(1).toDF("id").withColumn("v", lit(7));
    final Row result = df.select(let(col("v"), x -> x).alias("r")).first();
    assertEquals(7, result.getInt(0));
  }

  @Test
  void let_identityBody_returnsOperandValue_multiRow() {
    final List<Row> rows =
        df3().select(let(col("v"), x -> x).alias("r")).orderBy("r").collectAsList();
    assertEquals(List.of(1, 2, 3), rows.stream().map(r -> r.getInt(0)).toList());
  }

  @Test
  void let_multiReferenceBody_producesCorrectResult_singleRow() {
    final Dataset<Row> df = spark.range(1).toDF("id").withColumn("v", lit(5));
    // Body references x twice: x + x = 2*v. With let, x is materialised and referenced twice
    // without re-evaluating the operand.
    final Row result = df.select(let(col("v"), x -> x.plus(x)).alias("r")).first();
    assertEquals(10, result.getInt(0));
  }

  @Test
  void let_multiReferenceBody_producesCorrectResult_multiRow() {
    final List<Row> rows =
        df3().select(let(col("v"), x -> x.plus(x)).alias("r")).orderBy("r").collectAsList();
    assertEquals(List.of(2, 4, 6), rows.stream().map(r -> r.getInt(0)).toList());
  }

  @Test
  void let_overTraceExpression_firesExactlyOncePerRow_multiReferenceBody() {
    final Column traced = traceColumn(col("v"), "trace-multi");
    df3().select(let(traced, x -> x.plus(x)).alias("r")).collect();
    // Three rows × one fire each. Without let, the body's two references to x would each
    // re-evaluate the trace, doubling the count.
    assertEquals(3L, countTraceLogs("trace-multi"));
  }

  @Test
  void let_overTraceExpression_firesExactlyOncePerRow_singleRow() {
    final Dataset<Row> df = df3().limit(1);
    final Column traced = traceColumn(col("v"), "trace-single");
    df.select(let(traced, x -> x.plus(x)).alias("r")).collect();
    assertEquals(1L, countTraceLogs("trace-single"));
  }

  @Test
  void let_nullValue_propagatesNull() {
    final Dataset<Row> df = spark.range(1).toDF("id").withColumn("v", lit(null).cast("integer"));
    final Row result = df.select(let(col("v"), x -> x).alias("r")).first();
    assertTrue(result.isNullAt(0), "let(null, x -> x) should return null.");
  }

  @Test
  void let_nullValue_bodyReceivesNull() {
    // x.isNull() inside the body returns true (cast to 1) only if the body was invoked with x
    // bound to null, confirming that let() does not short-circuit on a SQL null.
    final Dataset<Row> df = spark.range(1).toDF("id").withColumn("v", lit(null).cast("integer"));
    final Row result = df.select(let(col("v"), x -> x.isNull().cast("integer")).alias("r")).first();
    assertEquals(1, result.getInt(0));
  }

  private long countTraceLogs(@Nonnull final String label) {
    final String marker = "[trace:" + label + "]";
    return appender.list.stream()
        .filter(event -> event.getFormattedMessage().contains(marker))
        .count();
  }

  @Nonnull
  private Dataset<Row> df3() {
    final StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("v", DataTypes.IntegerType, false, Metadata.empty())
            });
    return spark.createDataFrame(
        List.of(RowFactory.create(1, 1), RowFactory.create(2, 2), RowFactory.create(3, 3)), schema);
  }

  @Nonnull
  private static Column traceColumn(@Nonnull final Column input, @Nonnull final String label) {
    // The collector is null — we count fires via the SLF4J trace logger to avoid Spark
    // serialization issues with mutable collector state.
    return column(new TraceExpression(expression(input), label, "integer", null));
  }
}
