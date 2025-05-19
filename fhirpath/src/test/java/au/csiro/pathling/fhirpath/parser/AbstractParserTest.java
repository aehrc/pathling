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
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluators.SingleEvaluatorProvider;
import au.csiro.pathling.fhirpath.execution.FhirpathExecutor;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.assertions.FhirPathAssertion;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootUnitTest
@ExtendWith(TimingExtension.class)
public class AbstractParserTest {

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
  private DataSource dataSource;

  FhirpathExecutor executor;

  ResourceType subjectResource;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
        ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
        ResourceType.DIAGNOSTICREPORT, ResourceType.ORGANIZATION, ResourceType.QUESTIONNAIRE,
        ResourceType.CAREPLAN);

    setSubjectResource(ResourceType.PATIENT);
    executor = createExecutor();
  }


  @Nonnull
  protected FhirpathExecutor createExecutor() {
    return FhirpathExecutor.of(new Parser(), new SingleEvaluatorProvider(fhirContext,
        StaticFunctionRegistry.getInstance(), Map.of(), dataSource));
  }


  @SuppressWarnings("SameParameterValue")
  void setSubjectResource(@Nonnull final ResourceType resourceType) {
    subjectResource = resourceType;
  }

  protected void setDataSource(@Nonnull final DataSource dataSource) {
    this.dataSource = dataSource;
    this.executor = createExecutor();
  }

  void mockResource(final ResourceType... resourceTypes) {
    for (final ResourceType resourceType : resourceTypes) {
      final Dataset<Row> dataset = TestHelpers.getDatasetForResourceType(spark, resourceType);
      when(dataSource.read(resourceType)).thenReturn(dataset);
    }
  }

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  protected FhirPathAssertion assertThatResultOf(@Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    setSubjectResource(resourceType);
    return assertThat(executor.evaluate(subjectResource, expression));
  }

  @SuppressWarnings("SameParameterValue")
  FhirPathAssertion assertThatResultOf(final String expression) {
    return assertThat(executor.evaluate(subjectResource, expression));
  }

}
