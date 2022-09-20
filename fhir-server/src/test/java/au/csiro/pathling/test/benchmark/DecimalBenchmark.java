package au.csiro.pathling.test.benchmark;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.aggregate.AggregateRequestBuilder;
import au.csiro.pathling.aggregate.AggregateResponse;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.jmh.AbstractJmhSpringBootState;
import au.csiro.pathling.sql.types.FlexDecimal;
import au.csiro.pathling.sql.types.UcumDecimal;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Tag("UnitTest")
@Fork(0)
@Warmup(iterations = 3)
@Measurement(iterations = 7)
public class DecimalBenchmark {

  private static final int ROWS = 1000000;
  private static final BigDecimal LEFT_DECIMAL = new BigDecimal("1493493840938434300000.123456");
  private static final BigDecimal RIGHT_DECIMAL = new BigDecimal("999999999999138409380.123456");

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
          .withColumn("leftFlexDecimal", FlexDecimal.DATA_TYPE)
          .withColumn("rightFlexDecimal", FlexDecimal.DATA_TYPE)
          .withColumn("leftUcumDecimal", UcumDecimal.DATA_TYPE)
          .withColumn("rightUcumDecimal", UcumDecimal.DATA_TYPE);

      for (int i = 0; i < ROWS; i++) {
        datasetBuilder = datasetBuilder.withRow(
            LEFT_DECIMAL,
            RIGHT_DECIMAL,
            FlexDecimal.toRow(LEFT_DECIMAL),
            FlexDecimal.toRow(RIGHT_DECIMAL),
            LEFT_DECIMAL.toString(),
            RIGHT_DECIMAL.toString()
        );
      }
      dataset = datasetBuilder.build().cache();
    }

    @Nonnull
    Column col(String name) {
      return dataset.col(name);
    }

    @Nonnull
    List<Row> collectQuery(Column col) {
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
  public void multiply_flexDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexDecimal.multiply(ds.col("leftFlexDecimal"), ds.col("rightFlexDecimal"))));
  }

  @Benchmark
  public void add_flexDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexDecimal.plus(ds.col("leftFlexDecimal"), ds.col("rightFlexDecimal"))));
  }

  @Benchmark
  public void equals_flexDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexDecimal.equals(ds.col("leftFlexDecimal"), ds.col("rightFlexDecimal"))));
  }

  @Benchmark
  public void lt_flexDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        FlexDecimal.lt(ds.col("leftFlexDecimal"), ds.col("rightFlexDecimal"))));
  }


  @Benchmark
  public void multiply_ucumDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        UcumDecimal.multiply(ds.col("leftUcumDecimal"), ds.col("rightUcumDecimal"))));
  }

  @Benchmark
  public void add_ucumDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        UcumDecimal.plus(ds.col("leftUcumDecimal"), ds.col("rightUcumDecimal"))));
  }

  @Benchmark
  public void equals_ucumDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        UcumDecimal.equals(ds.col("leftUcumDecimal"), ds.col("rightUcumDecimal"))));
  }

  @Benchmark
  public void lt_ucumDec_Benchmark(final Blackhole bh,
      final DatasetState ds) {
    bh.consume(ds.collectQuery(
        UcumDecimal.lt(ds.col("leftUcumDecimal"), ds.col("rightUcumDecimal"))));
  }
}
