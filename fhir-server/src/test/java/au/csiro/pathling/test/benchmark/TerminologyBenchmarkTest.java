/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.benchmark;

import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.aggregate.AggregateRequestBuilder;
import au.csiro.pathling.aggregate.AggregateResponse;
import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import javax.annotation.Nonnull;
import java.util.Optional;

import static org.mockito.Mockito.mock;

@SpringBootTest
@ActiveProfiles({"core", "server", "benchmark"})
@TestPropertySource(properties = {"pathling.terminology.serverUrl=http://localhost:8081/fhir",
    "pathling.terminology.verboseLogging=true",
    "pathling.terminology.useLegacy=false"})
// @TestPropertySource(
//       properties = {"pathling.terminology.serverUrl=https://tx.ontoserver.csiro.au/fhir"})
public class TerminologyBenchmarkTest {

  @Autowired
  SparkSession spark;

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

  @MockBean
  Database database;

  AggregateExecutor defaultExecutor;

  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(database, spark, resourceTypes);
  }

  @BeforeEach
  public void setUp() {
    SharedMocks.resetAll();
    //database = mock(Database.class);
    SharedMocks.resetAll();
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
        ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
        ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION, ResourceType.QUESTIONNAIRE,
        ResourceType.CAREPLAN);

    defaultExecutor = new AggregateExecutor(configuration, fhirContext, spark, database,
        Optional.of(terminologyServiceFactory));

  }


  public AggregateResponse execute(@Nonnull final AggregateRequest query) {
    return defaultExecutor.execute(query);
  }

  // @Benchmark
  // public void memberOfSnomedImplicit_Benchmark(final Blackhole bh,
  //     final TerminologyBenchmark.TerminologyState executor) {
  //
  //   final AggregateRequest request = new AggregateRequestBuilder(ResourceType.CONDITION)
  //       .withAggregation("count()")
  //       // * : << 363698007|Finding site| = << 80891009|Structure of heart|
  //       .withGrouping(
  //           "code.coding.memberOf('http://snomed.info/sct?fhir_vs=ecl/*%20%3A%20%3C%3C%20363698007%20%3D%20%3C%3C%2080891009%20')")
  //       .build();
  //   bh.consume(executor.execute(request));
  // }


  @Test
  public void memberOfLoincImplicit_Benchmark() throws Exception {

    System.out.println(2_000_000);
    
    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.OBSERVATION)
        .withAggregation("count()")
        .withFilter("code.coding.memberOf('http://loinc.org/vs/LP14885-5') contains true")
        .build();
    execute(request);
    //Thread.sleep(1000000);
  }


  @Test
  public void memberOfSnomed() throws Exception {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.CONDITION)
        .withAggregation("count()")
        .withFilter("code.coding.memberOf('http://snomed.info/sct?fhir_vs=ecl/*%20%3A%20%3C%3C%20363698007%20%3D%20%3C%3C%2080891009%20') contains true")
        .build();
    execute(request);
    //Thread.sleep(1000000);
  }



  @Test
  public void memberOfLoincImplicit_Benchmark_withShuffle() throws Exception {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.OBSERVATION)
        .withAggregation("count()")
        .withFilter(
            "code.where($this.coding.count() > 0).coding.memberOf('http://loinc.org/vs/LP14885-5') contains true")
        .build();
    execute(request);
    Thread.sleep(1000000);
  }


  @Test
  public void memberOfLoincImplicitReverse_Benchmark() throws Exception {

    final AggregateRequest request = new AggregateRequestBuilder(ResourceType.PATIENT)
        .withAggregation("count()")
        .withFilter(
            "reverseResolve(Observation.subject).code.coding.memberOf('http://loinc.org/vs/LP14885-5') contains true")
        .build();
    execute(request);
    Thread.sleep(1000000);
  }

  // @Benchmark
  // public void complexExpression_default_Benchmark(final Blackhole bh,
  //     final TerminologyBenchmark.TerminologyState executor) {
  //
  //   final AggregateRequest request = new AggregateRequestBuilder(ResourceType.PATIENT)
  //       .withAggregation("count()")
  //       .withGrouping(
  //           "reverseResolve(MedicationRequest.subject).medicationCodeableConcept.coding.memberOf('http://snomed.info/sct?fhir_vs=ecl/(%3C%3C%20416897008%20%7B%7B%20%2B%20HISTORY-MAX%20%7D%7D)') contains true")
  //       .withGrouping(
  //           "reverseResolve(Condition.subject).code.coding.memberOf('http://snomed.info/sct?fhir_vs=ecl/((%3C%3C%2064572001%20%3A%20(%3C%3C%20363698007%20%3D%20%3C%3C%2039607008%20%2C%20%3C%3C%20370135005%20%3D%20%3C%3C%20441862004%20))%20%7B%7B%20%2B%20HISTORY-MOD%20%7D%7D)') contains true")
  //       .withFilter(
  //           "reverseResolve(Condition.subject).code.coding.memberOf('http://snomed.info/sct?fhir_vs=ecl/((%3C%3C%2064572001%20%3A%20(%3C%3C%20363698007%20%3D%20%3C%3C%2039352004%20%2C%20%3C%3C%20370135005%20%3D%20%3C%3C%20263680009%20))%20%7B%7B%20%2B%20HISTORY-MOD%20%7D%7D)') contains true")
  //       .withFilter(
  //           "reverseResolve(Condition.subject).code.coding.memberOf('http://snomed.info/sct?fhir_vs=ecl/((%3C%3C%2064572001%20%3A%20(%3C%3C%20363698007%20%3D%20%3C%3C%2039607008%20%2C%20%3C%3C%20263502005%20%3D%20%3C%3C%2090734009%20))%20%7B%7B%20%2B%20HISTORY-MOD%20%7D%7D)') contains true")
  //       .build();
  //   bh.consume(executor.execute(request));
  // }
}