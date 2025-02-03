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

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.Collections;
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
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@Tag("Tranche1")
@ExtendWith(TimingExtension.class)
class AggregateQueryExecutorTest {

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
  DataSource dataSource;

  AggregateQueryExecutor executor;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    executor = new AggregateQueryExecutor(configuration, fhirContext, spark, dataSource,
        Optional.ofNullable(terminologyServiceFactory));
  }

  @Test
  void simpleQueryWithLabels() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequest(
        subjectResource,
        List.of(ExpressionWithLabel.of("count()", "patient_count")),
        List.of(ExpressionWithLabel.of("gender", "patient_gender")),
        Collections.emptyList()
    );

    final Dataset<Row> result = executor.buildQuery(request).getDataset();
    assertArrayEquals(new String[]{"patient_gender", "patient_count"},
        result.columns());
    assertThat(result)
        .hasRows(spark, "responses/AggregateQueryExecutorTest/simpleQuery.tsv");
  }

  @Test
  void simpleQueryWithNoLabels() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender")
        .build();

    final Dataset<Row> result = executor.buildQuery(request).getDataset();
    //assertTrue(Stream.of(result.columns()).allMatch(Strings::looksLikeAlias));
    assertThat(result)
        .hasRows(spark, "responses/AggregateQueryExecutorTest/simpleQuery.tsv");
  }

  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(dataSource, spark, resourceTypes);
  }

}
