package au.csiro.pathling.test.bechmark;

import static org.mockito.Mockito.mock;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.aggregate.AggregateRequestBuilder;
import au.csiro.pathling.aggregate.AggregateResponse;
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
@Warmup(iterations = 1, batchSize = 1)
@Measurement(iterations = 3, batchSize = 1)
public class AggregateBenchmark {

  @State(Scope.Benchmark)
  @ActiveProfiles("unit-test")
  public static class AggregateState extends AbstractJmhSpringBootState {

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
    ResourceType subjectResource;
    Database database;
    AggregateResponse response = null;

    void mockResource(final ResourceType... resourceTypes) {
      TestHelpers.mockResource(database, spark, resourceTypes);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
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
  public void simpleQuery(final Blackhole bh, final AggregateState executor) {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.PATIENT)
        .withAggregation("count()")
        .withGrouping("gender")
        .build();

    bh.consume(executor.execute(request));
  }


  @Benchmark
  public void queryWithMultipleGroupingsAndMembership(final Blackhole bh,
      final AggregateState executor) {
    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.PATIENT)
        .withAggregation("count()")
        .withGrouping("name.prefix contains 'Mrs.'")
        .withGrouping("name.given contains 'Karina848'")
        .build();

    bh.consume(executor.execute(request));
  }

}
