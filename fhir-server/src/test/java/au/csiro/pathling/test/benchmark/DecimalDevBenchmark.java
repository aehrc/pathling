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

package au.csiro.pathling.test.benchmark;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.jmh.AbstractJmhSpringBootState;
import au.csiro.pathling.sql.types.FlexiDecimal;
import au.csiro.pathling.test.builders.DatasetBuilder;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;

/**
 * Benchmarks for the {@link FlexiDecimal} class.
 *
 * @author Piotr Szul
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Tag("UnitTest")
@Fork(0)
@Warmup(iterations = 3)
@Measurement(iterations = 7)
public class DecimalDevBenchmark {

  private static final int ROWS = 100_000;
  private static final BigDecimal LEFT_DECIMAL = new BigDecimal(
      "12345678901234567890123456.123456");
  private static final BigDecimal RIGHT_DECIMAL = new BigDecimal("0.12345678901234567890123456");

  @State(Scope.Benchmark)
  @ActiveProfiles("unit-test")
  public static class DatasetState extends AbstractJmhSpringBootState {

    @Autowired
    SparkSession spark;

    Dataset<Row> dataset;


    @Setup(Level.Trial)
    public void setUp() {

      DatasetBuilder datasetBuilder = new DatasetBuilder(spark)
          .withColumn("leftDecimal", DecimalCustomCoder.decimalType())
          .withColumn("rightDecimal", DecimalCustomCoder.decimalType())
          .withColumn("leftFlexiDecimal", FlexiDecimal.DATA_TYPE)
          .withColumn("rightFlexiDecimal", FlexiDecimal.DATA_TYPE);

      for (int i = 0; i < ROWS; i++) {
        datasetBuilder = datasetBuilder.withRow(
            LEFT_DECIMAL,
            RIGHT_DECIMAL,
            FlexiDecimal.toValue(LEFT_DECIMAL),
            FlexiDecimal.toValue(RIGHT_DECIMAL)
        );
      }
      dataset = datasetBuilder.build().cache();
    }

    @Nonnull
    Column col(@Nonnull final String name) {
      return dataset.col(name);
    }

    @Nonnull
    List<Row> collectQuery(@Nonnull final Column col) {
      return dataset.select(col).collectAsList();
    }
  }

  @Benchmark
  public void multiply_decimal_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(ds.col("leftDecimal").multiply(ds.col("rightDecimal"))));
  }

  @Benchmark
  public void add_decimal_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(ds.col("leftDecimal").plus(ds.col("rightDecimal"))));
  }

  @Benchmark
  public void equals_decimal_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(ds.col("leftDecimal").equalTo(ds.col("rightDecimal"))));
  }

  @Benchmark
  public void lt_decimal_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(ds.col("leftDecimal").lt(ds.col("rightDecimal"))));
  }

  @Benchmark
  public void multiply_flexiDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexiDecimal.multiply(ds.col("leftFlexiDecimal"), ds.col("rightFlexiDecimal"))));
  }

  @Benchmark
  public void add_flexiDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexiDecimal.plus(ds.col("leftFlexiDecimal"), ds.col("rightFlexiDecimal"))));
  }

  @Benchmark
  public void equals_flexiDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexiDecimal.equals(ds.col("leftFlexiDecimal"), ds.col("rightFlexiDecimal"))));
  }

  @Benchmark
  public void lt_flexiDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexiDecimal.lt(ds.col("leftFlexiDecimal"), ds.col("rightFlexiDecimal"))));
  }

}
