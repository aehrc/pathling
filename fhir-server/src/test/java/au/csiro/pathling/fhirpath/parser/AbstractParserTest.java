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

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.TimingExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@SpringBootUnitTest
@ExtendWith(TimingExtension.class)
public class AbstractParserTest {

  // @Autowired
  // protected SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // @Autowired
  // TerminologyService terminologyService;
  //
  // @Autowired
  // FhirEncoders fhirEncoders;
  //
  // @Autowired
  // TerminologyServiceFactory terminologyServiceFactory;
  //
  // @MockBean
  // protected DataSource dataSource;
  //
  // Parser parser;
  //
  // @BeforeEach
  // void setUp() {
  //   SharedMocks.resetAll();
  //   mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
  //       ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
  //       ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION, ResourceType.QUESTIONNAIRE,
  //       ResourceType.CAREPLAN);
  //
  //   final ResourceCollection subjectResource = ResourceCollection
  //       .build(fhirContext, dataSource, ResourceType.PATIENT, ResourceType.PATIENT.toCode());
  //
  //   final EvaluationContext evaluationContext = new EvaluationContextBuilder(spark, fhirContext)
  //       .terminologyServiceFactory(terminologyServiceFactory)
  //       .dataset(dataSource)
  //       .inputContext(subjectResource)
  //       .groupingColumns(Collections.singletonList(subjectResource.getIdColumn()))
  //       .build();
  //   parser = new Parser(evaluationContext);
  // }
  //
  // void mockResource(final ResourceType... resourceTypes) {
  //   for (final ResourceType resourceType : resourceTypes) {
  //     final Dataset<Row> dataset = TestHelpers.getDatasetForResourceType(spark, resourceType);
  //     when(dataSource.read(resourceType)).thenReturn(dataset);
  //   }
  // }
  //
  // @SuppressWarnings("SameParameterValue")
  // @Nonnull
  // protected FhirPathAssertion assertThatResultOf(@Nonnull final ResourceType resourceType,
  //     @Nonnull final String expression) {
  //   final ResourceCollection subjectResource = ResourceCollection
  //       .build(fhirContext, dataSource, resourceType, resourceType.toCode());
  //
  //   final EvaluationContext evaluationContext = new EvaluationContextBuilder(spark, fhirContext)
  //       .terminologyClientFactory(terminologyServiceFactory)
  //       .database(dataSource)
  //       .inputContext(subjectResource)
  //       .build();
  //   final Parser resourceParser = new Parser(evaluationContext);
  //   return assertThat(resourceParser.evaluate(expression, context));
  // }
  //
  // @SuppressWarnings("SameParameterValue")
  // FhirPathAssertion assertThatResultOf(final String expression) {
  //   return assertThat(parser.evaluate(expression, context));
  // }

}
