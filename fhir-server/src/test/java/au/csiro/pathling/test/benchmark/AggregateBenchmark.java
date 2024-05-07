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

import static org.mockito.Mockito.mock;

import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.aggregate.AggregateRequestBuilder;
import au.csiro.pathling.aggregate.AggregateResponse;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.jmh.AbstractJmhSpringBootState;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Tag("UnitTest")
@Fork(0)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
public class AggregateBenchmark {

  @State(Scope.Benchmark)
  @ActiveProfiles("unit-test")
  public static class AggregateState extends AbstractJmhSpringBootState {

    @Autowired
    SparkSession spark;

    @Autowired
    TerminologyServiceFactory terminologyServiceFactory;

    @Autowired
    QueryConfiguration configuration;

    @Autowired
    FhirContext fhirContext;

    @Autowired
    IParser jsonParser;

    @Autowired
    FhirEncoders fhirEncoders;

    AggregateExecutor executor;
    Database database;

    void mockResource(final ResourceType... resourceTypes) {
      TestHelpers.mockResource(database, spark, resourceTypes);
    }

    @Setup(Level.Trial)
    public void setUp() {
      SharedMocks.resetAll();
      database = mock(Database.class);

      SharedMocks.resetAll();
      mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
          ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
          ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION,
          ResourceType.CAREPLAN);

      executor = new AggregateExecutor(configuration, fhirContext, spark, database,
          Optional.of(terminologyServiceFactory));
    }

    public AggregateResponse execute(@Nonnull final AggregateRequest query) {
      return executor.execute(query);
    }
  }

  @Benchmark
  public void simpleAggregation_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("count()")
        .build();
    bh.consume(executor.execute(request));
  }

  @Benchmark
  public void simpleAggregationAndGrouping_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("count()")
        .withGrouping("class.code")
        .build();
    bh.consume(executor.execute(request));
  }

  @Benchmark
  public void simpleAggregationGroupingAndFilter_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("count()")
        .withGrouping("class.code")
        .withFilter("status = 'finished'")
        .build();
    bh.consume(executor.execute(request));
  }

  @Benchmark
  public void complexAggregation_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("reverseResolve(Condition.encounter).count()")
        .build();
    bh.consume(executor.execute(request));
  }

  @Benchmark
  public void complexAggregationAndGrouping_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("reverseResolve(Condition.encounter).count()")
        .withGrouping("reverseResolve(Condition.encounter).where($this.onsetDateTime > @2010 and "
            + "$this.onsetDateTime < @2011).verificationStatus.coding.code")
        .build();
    bh.consume(executor.execute(request));
  }

  @Benchmark
  public void complexAggregationGroupingAndFilter_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("reverseResolve(Condition.encounter).count()")
        .withGrouping("reverseResolve(Condition.encounter).where($this.onsetDateTime > @2010 and "
            + "$this.onsetDateTime < @2011).verificationStatus.coding.code")
        .withFilter("serviceProvider.resolve().name = 'ST ELIZABETH\\'S MEDICAL CENTER'")
        .build();
    bh.consume(executor.execute(request));
  }


  @Benchmark
  public void multipleAggregations_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("count()")
        .withAggregation("reasonCode.count()")
        .withAggregation("reverseResolve(Condition.encounter).count()")
        .build();
    bh.consume(executor.execute(request));
  }

  @Benchmark
  public void multipleAggregationsAndGroupings_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("count()")
        .withAggregation("reasonCode.count()")
        .withAggregation("reverseResolve(Condition.encounter).count()")
        .withGrouping("class.code")
        .withGrouping("reasonCode.coding.display")
        .withGrouping("reverseResolve(Condition.encounter).where($this.onsetDateTime > @2010 and "
            + "$this.onsetDateTime < @2011).verificationStatus.coding.code")
        .build();
    bh.consume(executor.execute(request));
  }

  @Benchmark
  public void multipleAggregationsGroupingsAndFilters_Benchmark(final Blackhole bh,
      final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.ENCOUNTER)
        .withAggregation("count()")
        .withAggregation("reasonCode.count()")
        .withAggregation("reverseResolve(Condition.encounter).count()")
        .withGrouping("class.code")
        .withGrouping("reasonCode.coding.display")
        .withGrouping("reverseResolve(Condition.encounter).where($this.onsetDateTime > @2010 and "
            + "$this.onsetDateTime < @2011).verificationStatus.coding.code")
        .withFilter("status = 'finished'")
        .withFilter("serviceProvider.resolve().name = 'ST ELIZABETH\\'S MEDICAL CENTER'")
        .build();
    bh.consume(executor.execute(request));
  }

}
