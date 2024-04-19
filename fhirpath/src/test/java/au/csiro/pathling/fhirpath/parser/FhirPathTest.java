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

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.FhirPathAssertion;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootUnitTest
public class FhirPathTest {

  @Autowired
  protected SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  TerminologyService terminologyService;

  @Autowired
  FhirEncoders fhirEncoders;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @MockBean
  DataSource dataSource;


  @SuppressWarnings("SameParameterValue")
  @Nonnull
  protected FhirPathAssertion assertThatResultOf(@Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, dataSource, resourceType, resourceType.toCode(), true);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .database(dataSource)
        .inputContext(subjectResource)
        .build();
    final Parser resourceParser = new Parser(parserContext);
    return assertThat(resourceParser.parse(expression));
  }


  void withResources(@Nonnull final Resource resources) {

    // group resources by type
    // and then encode them into a dataset and setup the mock datasorce

    Stream.of(resources).collect(Collectors.groupingBy(Resource::getResourceType))
        .forEach((resourceType, resourcesOfType) -> {
          final ResourceType resourceTypeEnum = ResourceType.fromCode(resourceType.name());
          final Dataset<Row> dataset = spark.createDataset(resourcesOfType,
                  fhirEncoders.of(resourceTypeEnum.toCode()))
              .toDF();
          when(dataSource.read(resourceTypeEnum)).thenReturn(dataset.cache());
        });
  }


  @Test
  void testTraversalIntoReferenceIdentifier() {
    withResources(
        new Condition()
            .setSubject(new Reference().setIdentifier(new Identifier().setValue("value")))
            .setId("001")
    );
    assertThatResultOf(ResourceType.CONDITION, "subject.identifier.value")
        .selectResult()
        .hasRows(RowFactory.create("001", "value"));
  }

}
