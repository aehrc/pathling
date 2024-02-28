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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class ExecutorIntegrationTest {

  @Autowired
  QueryConfiguration configuration;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  SparkSession spark;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  IParser jsonParser;

  @Autowired
  FhirEncoders fhirEncoders;

  @MockBean
  CacheableDatabase dataSource;

  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(dataSource, spark, resourceTypes);
  }


  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.OBSERVATION,
        ResourceType.MEDICATIONREQUEST);
  }

  @Test
  void testComplexSigleResourceQuery() {

    dataSource.read(ResourceType.PATIENT).select("id", "gender", "name")
        .show(false);

    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where(gender='female').name.where(family.where($this='Oberbrunner298').exists()).given.join(',')");
    System.out.println(path.toExpression());

    final FhirPathExecutor executor = new SingleFhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Dataset<Row> result = executor.execute(path);
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
  }


  @Test
  void testSimpleReverseResolve() {
    dataSource.read(ResourceType.PATIENT).select("id", "gender", "name")
        .show(false);

    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "reverseResolve(Observation.subject).code.coding.count()");
    System.out.println(path.toExpression());

    final FhirPathExecutor executor = new MultiFhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Dataset<Row> result = executor.execute(path);
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
  }


  @Test
  void testComplexReverseResolve() {

    dataSource.read(ResourceType.PATIENT).select("id", "gender", "name")
        .show(false);

    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "reverseResolve(Observation.subject).code.coding.count() > reverseResolve(Condition.subject).code.coding.count()");
    System.out.println(path.toExpression());

    final FhirPathExecutor executor = new MultiFhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Dataset<Row> result = executor.execute(path);
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
  }


}
