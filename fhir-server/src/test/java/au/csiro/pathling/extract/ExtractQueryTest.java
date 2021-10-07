/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResultWriter;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
@ExtendWith(TimingExtension.class)
class ExtractQueryTest {

  @Autowired
  private Configuration configuration;

  @Autowired
  private FhirContext fhirContext;

  @Autowired
  private SparkSession spark;

  @Autowired
  private TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  protected IParser jsonParser;

  private ResourceType subjectResource;

  private ResourceReader resourceReader;

  private ExtractExecutor executor;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    resourceReader = mock(ResourceReader.class);
    final ResultWriter resultWriter = mock(ResultWriter.class);
    final ResultRegistry resultRegistry = mock(ResultRegistry.class);
    executor = new ExtractExecutor(configuration, fhirContext, spark, resourceReader,
        Optional.ofNullable(terminologyServiceFactory), resultWriter, resultRegistry);
  }

  @Test
  void simpleQuery() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("gender")
        .withColumn("name.given.first()")
        .withColumn("reverseResolve(Condition.subject).count()")
        .withFilter("gender = 'female'")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/simpleQuery.csv");
  }

  @Test
  void multipleResolves() {
    subjectResource = ResourceType.ENCOUNTER;
    mockResourceReader(ResourceType.ENCOUNTER, ResourceType.ORGANIZATION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("serviceProvider.resolve().id")
        .withColumn("serviceProvider.resolve().name")
        .withColumn("serviceProvider.resolve().address.postalCode")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multipleResolves.csv");
  }

  @Test
  void multipleReverseResolves() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("reverseResolve(Condition.subject).id")
        .withColumn("reverseResolve(Condition.subject).code.coding.system")
        .withColumn("reverseResolve(Condition.subject).code.coding.code")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multipleReverseResolves.csv");
  }

  @Test
  void multiplePolymorphicResolves() {
    subjectResource = ResourceType.DIAGNOSTICREPORT;
    mockResourceReader(ResourceType.DIAGNOSTICREPORT, ResourceType.PATIENT);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("subject.resolve().ofType(Patient).id")
        .withColumn("subject.resolve().ofType(Patient).gender")
        .withColumn("subject.resolve().ofType(Patient).name.given")
        .withColumn("subject.resolve().ofType(Patient).name.family")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multiplePolymorphicResolves.csv");
  }

  @Test
  void literalColumn() {
    subjectResource = ResourceType.CONDITION;
    mockResourceReader(ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("19")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/literalColumn.csv");
  }

  @Test
  void codingColumn() {
    subjectResource = ResourceType.CONDITION;
    mockResourceReader(ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("code.coding")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/codingColumn.csv");
  }

  @Test
  void codingLiteralColumn() {
    subjectResource = ResourceType.CONDITION;
    mockResourceReader(ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn(
            "http://snomed.info/sct|'46,2'|http://snomed.info/sct/32506021000036107/version/20201231")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/codingLiteralColumn.csv");
  }

  @Test
  void multipleFilters() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("gender")
        .withColumn("name.given.first()")
        .withColumn("reverseResolve(Condition.subject).count()")
        .withFilter("gender = 'female'")
        .withFilter("reverseResolve(Condition.subject).count() >= 10")
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multipleFilters.csv");
  }

  @Test
  void limit() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("gender")
        .withFilter("gender = 'female'")
        .withLimit(3)
        .build();

    final Dataset<Row> result = executor.buildQuery(request);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/limit.csv");
  }

  @Test
  void emptyColumn() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT);

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new ExtractRequestBuilder(subjectResource)
            .withColumn("id")
            .withColumn("")
            .withFilter("gender = 'female'")
            .build());
    assertEquals("Column expression cannot be blank", error.getMessage());
  }

  @Test
  void emptyFilter() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT);

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new ExtractRequestBuilder(subjectResource)
            .withColumn("id")
            .withFilter("")
            .build());
    assertEquals("Filter expression cannot be blank", error.getMessage());
  }

  @Test
  void noColumns() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT);

    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> new ExtractRequestBuilder(subjectResource)
            .withFilter("gender = 'female'")
            .build());
    assertEquals("Query must have at least one column expression", error.getMessage());
  }

  @Test
  void nonPositiveLimit() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.PATIENT);
    final List<Integer> limits = Arrays.asList(0, -1);

    for (final Integer limit : limits) {
      final InvalidUserInputError error = assertThrows(
          InvalidUserInputError.class,
          () -> new ExtractRequestBuilder(subjectResource)
              .withColumn("id")
              .withLimit(limit)
              .build());
      assertEquals("Limit must be greater than zero", error.getMessage());
    }
  }

  private void mockResourceReader(final ResourceType... resourceTypes) {
    TestHelpers.mockResourceReader(resourceReader, spark, resourceTypes);
  }

}
