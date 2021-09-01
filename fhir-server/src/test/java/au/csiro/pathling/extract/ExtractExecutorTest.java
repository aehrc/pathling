/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResultWriter;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
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
class ExtractExecutorTest {

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

  private ResultWriter resultWriter;

  private ExtractExecutor executor;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    resourceReader = mock(ResourceReader.class);
    resultWriter = mock(ResultWriter.class);
    executor = new ExtractExecutor(configuration, fhirContext, spark, resourceReader,
        Optional.ofNullable(terminologyServiceFactory), resultWriter);
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
        .hasRows(spark, "responses/ExtractExecutorTest/simpleQuery.csv");
  }

  private void mockResourceReader(final ResourceType... resourceTypes) {
    TestHelpers.mockResourceReader(resourceReader, spark, resourceTypes);
  }

}
