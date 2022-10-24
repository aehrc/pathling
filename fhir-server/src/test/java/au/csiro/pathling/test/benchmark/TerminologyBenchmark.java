/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.benchmark;

import static org.mockito.Mockito.mock;

import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.aggregate.AggregateRequestBuilder;
import au.csiro.pathling.aggregate.AggregateResponse;
import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.jmh.AbstractJmhSpringBootState;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
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
@Fork(0)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
public class TerminologyBenchmark {

  @State(Scope.Benchmark)
  @ActiveProfiles("core,server,integration-test")
  public static class TerminologyState extends AbstractJmhSpringBootState {

    @Autowired
    SparkSession spark;

    @Autowired
    TerminologyService terminologyService;

    @Autowired
    TerminologyServiceFactory terminologyServiceFactory;

    @Autowired
    Configuration configuration;

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
          ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION, ResourceType.QUESTIONNAIRE,
          ResourceType.CAREPLAN);

      executor = new AggregateExecutor(configuration, fhirContext, spark, database,
          Optional.of(terminologyServiceFactory));
    }

    public AggregateResponse execute(@Nonnull final AggregateRequest query) {
      return executor.execute(query);
    }
  }

  @Benchmark
  public void memberOfSnomedImplicit_Benchmark(final Blackhole bh,
      final TerminologyBenchmark.TerminologyState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.CONDITION)
        .withAggregation("count()")
        // * : << 363698007|Finding site| = << 80891009|Structure of heart|
        .withGrouping(
            "code.coding.memberOf('http://snomed.info/sct?fhir_vs=ecl/*%20%3A%20%3C%3C%20363698007%20%3D%20%3C%3C%2080891009%20')")
        .build();
    bh.consume(executor.execute(request));
  }

}
