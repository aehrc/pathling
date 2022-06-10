package au.csiro.pathling.test.bechmark;

import static org.mockito.Mockito.when;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.jmh.AbstractJmhSpringBootState;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.mockito.Mockito;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Tag("UnitTest")
@Fork(0)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class ParserBenchmark {

  @State(Scope.Benchmark)
  @ActiveProfiles("unit-test")
  public static class ParserState extends AbstractJmhSpringBootState {

    @Autowired
    protected SparkSession spark;
    @Autowired
    protected FhirContext fhirContext;
    @Autowired
    protected TerminologyServiceFactory terminologyServiceFactory;
    @Autowired
    protected TerminologyService terminologyService;
    @Autowired
    protected FhirEncoders fhirEncoders;

    @MockBean
    protected Database database;

    void mockResource(final ResourceType... resourceTypes) {
      for (final ResourceType resourceType : resourceTypes) {
        final Dataset<Row> dataset = TestHelpers.getDatasetForResourceType(spark, resourceType);
        when(database.read(resourceType)).thenReturn(dataset);
      }
    }

    @Setup(Level.Trial)
    public void setUp() {
      database = Mockito.mock(Database.class);
      SharedMocks.resetAll();
      mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
          ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
          ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION, ResourceType.QUESTIONNAIRE,
          ResourceType.CAREPLAN);
    }

    public FhirPath parse(final String expression) {
      return parse(expression, ResourceType.PATIENT);
    }

    public FhirPath parse(final String expression, final ResourceType subjectResourceType) {

      // TODO: Do we need to pre-create and cash all required parsers
      // For all resources ???
      final ResourcePath subjectResource = ResourcePath
          .build(fhirContext, database, subjectResourceType, subjectResourceType.toCode(), true);

      final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
          .terminologyClientFactory(terminologyServiceFactory)
          .database(database)
          .inputContext(subjectResource)
          .groupingColumns(Collections.singletonList(subjectResource.getIdColumn()))
          .build();
      final Parser parser = new Parser(parserContext);
      return parser.parse(expression);
    }
  }

  @Benchmark
  public void queryWithWhere(final Blackhole bh, final ParserState parser) {
    bh.consume(parser.parse("where($this.name.given.first() = 'Karina848').gender"));
  }

  @Benchmark
  public void queryWithWhereAndMembership(final Blackhole bh, final ParserState parser) {
    bh.consume(parser.parse("where(name.where(use = 'official').first().given.first() in "
        + "name.where(use = 'maiden').first().given).gender"));
  }
}
