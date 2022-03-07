/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.assertions.FhirPathAssertion;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
@Tag("UnitTest")
@ExtendWith(TimingExtension.class)
public class AbstractParserTest {

  @Autowired
  protected SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @MockBean
  protected Database database;

  Parser parser;

  @BeforeEach
  void setUp() throws IOException {
    SharedMocks.resetAll();
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
        ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
        ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION, ResourceType.QUESTIONNAIRE);

    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, database, ResourceType.PATIENT, ResourceType.PATIENT.toCode(), true);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .database(database)
        .inputContext(subjectResource)
        .groupingColumns(Collections.singletonList(subjectResource.getIdColumn()))
        .build();
    parser = new Parser(parserContext);
  }

  void mockResource(final ResourceType... resourceTypes)
      throws MalformedURLException {
    for (final ResourceType resourceType : resourceTypes) {
      final File parquetFile =
          new File("src/test/resources/test-data/parquet/" + resourceType.toCode() + ".parquet");
      final URL parquetUrl = new URL("file://" + parquetFile.getAbsoluteFile().toPath());
      assertNotNull(parquetUrl);
      final Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
      when(database.read(resourceType)).thenReturn(dataset);
    }
  }

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  protected FhirPathAssertion assertThatResultOf(@Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, database, resourceType, resourceType.toCode(), true);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .database(database)
        .inputContext(subjectResource)
        .build();
    final Parser resourceParser = new Parser(parserContext);
    return assertThat(resourceParser.parse(expression));
  }

}
