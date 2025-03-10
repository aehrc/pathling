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

package au.csiro.pathling.extract;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode_outer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.helpers.TestHelpers;
import au.csiro.pathling.utilities.Strings;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
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
class ExtractQueryTest {

  @Autowired
  QueryConfiguration configuration;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  SparkSession spark;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @MockBean
  CacheableDatabase dataSource;

  ResourceType subjectResource;


  ExtractQueryExecutor executor;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    executor = new ExtractQueryExecutor(configuration, fhirContext, spark, dataSource,
        Optional.ofNullable(terminologyServiceFactory));
  }

  @Test
  void simpleQueryWithAliases() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequest(subjectResource,
        List.of(
            ExpressionWithLabel.withExpressionAsLabel("id"),
            ExpressionWithLabel.withExpressionAsLabel("gender"),
            ExpressionWithLabel.of("name.given.first()", "given_name"),
            ExpressionWithLabel.of("reverseResolve(Condition.subject).count().toString()",
                "condition_count")
        ),
        List.of("gender = 'female'"),
        Optional.empty()
    );
    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertArrayEquals(new String[]{"id", "gender", "given_name", "condition_count"},
        result.columns());
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/simpleQueryWithAliases.tsv");
  }

  @Test
  void multipleResolves() {
    subjectResource = ResourceType.ENCOUNTER;
    mockResource(ResourceType.ENCOUNTER, ResourceType.ORGANIZATION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("serviceProvider.resolve().id")
        .withColumn("serviceProvider.resolve().name")
        .withColumn("serviceProvider.resolve().address.postalCode")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);

    assertTrue(Stream.of(result.columns()).allMatch(Strings::looksLikeAlias));
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multipleResolves.tsv");
  }

  @Test
  void multipleReverseResolves() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("reverseResolve(Condition.subject).id")
        .withColumn("reverseResolve(Condition.subject).code.coding.system")
        .withColumn("reverseResolve(Condition.subject).code.coding.code")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multipleReverseResolves.tsv");
  }

  @Test
  void multiplePolymorphicResolves() {
    subjectResource = ResourceType.DIAGNOSTICREPORT;
    mockResource(ResourceType.DIAGNOSTICREPORT, ResourceType.PATIENT);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("subject.resolve().ofType(Patient).id")
        .withColumn("subject.resolve().ofType(Patient).gender")
        .withColumn("subject.resolve().ofType(Patient).name.given")
        .withColumn("subject.resolve().ofType(Patient).name.family")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multiplePolymorphicResolves.tsv");
  }

  @Test
  void literalColumn() {
    subjectResource = ResourceType.CONDITION;
    mockResource(ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("19")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/literalColumn.tsv");
  }

  @Test
  void resolveAndCodingLiteralColumn() {
    subjectResource = ResourceType.DIAGNOSTICREPORT;
    mockResource(ResourceType.DIAGNOSTICREPORT, ResourceType.OBSERVATION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("result.resolve().code.coding.system")
        .withColumn("http://snomed.info/sct|372823004")
        .withLimit(10)
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/resolveAndCodingLiteralColumn.tsv");
  }

  @Test
  void codingColumn() {
    subjectResource = ResourceType.CONDITION;
    mockResource(ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("code.coding")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/codingColumn.tsv");
  }

  @Test
  void codingLiteralColumn() {
    subjectResource = ResourceType.CONDITION;
    mockResource(ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn(
            "http://snomed.info/sct|'46,2'|http://snomed.info/sct/32506021000036107/version/20201231")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/codingLiteralColumn.tsv");
  }

  @Test
  void multipleFilters() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("gender")
        .withColumn("name.given.first()")
        .withColumn("reverseResolve(Condition.subject).count()")
        .withFilter("gender = 'female'")
        .withFilter("reverseResolve(Condition.subject).count() >= 10")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multipleFilters.tsv");
  }

  @Test
  void limit() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id", "id_col")
        .withColumn("gender", "gender_col")
        .withFilter("gender = 'female'")
        .withLimit(3)
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/limit.tsv");
  }

  @Test
  void eliminatesTrailingNulls() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource, ResourceType.CONDITION);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("reverseResolve(Condition.subject).code.coding.code.where($this = '72892002')")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/eliminatesTrailingNulls.tsv");
  }

  @Test
  void combineResultInSecondFilter() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withFilter("gender = 'male'")
        .withFilter("(name.given combine name.family).empty().not()")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/combineResultInSecondFilter.tsv");
  }

  @Test
  void whereInMultipleColumns() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("identifier.where(system = 'https://github.com/synthetichealth/synthea').value")
        .withColumn("identifier.where(system = 'http://hl7.org/fhir/sid/us-ssn').value")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/whereInMultipleColumns.tsv");
  }

  @Test
  void multipleNonSingularColumnsWithDifferentTypes() {
    subjectResource = ResourceType.ENCOUNTER;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("type.coding.display")
        .withColumn("type.coding")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark,
            "responses/ExtractQueryTest/multipleNonSingularColumnsWithDifferentTypes.tsv");
  }

  @Test
  void linkedUnnesting() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("name.given")
        .withColumn("name.family")
        .withColumn("name.use")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/linkedUnnesting.tsv");
  }

  @Test
  void multipleIndependentUnnestings() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("name.given")
        .withColumn("name.family")
        .withColumn("maritalStatus.coding.system")
        .withColumn("maritalStatus.coding.code")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/multipleIndependentUnnestings.tsv");
  }

  @Test
  void toleranceOfColumnOrdering() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("name.family")
        .withColumn("name.given")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/toleranceOfColumnOrdering1.tsv");

    final ExtractRequest request2 = new ExtractRequestBuilder(subjectResource)
        .withColumn("name.given")
        .withColumn("id")
        .build();

    final Dataset<Row> result2 = executor.buildQuery(request2);
    assertThat(result2)
        .hasRows(spark, "responses/ExtractQueryTest/toleranceOfColumnOrdering2.tsv");
  }

  @Test
  void emptyColumn() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT);

    final IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> new ExtractRequestBuilder(subjectResource)
            .withColumn("id")
            .withColumn("")
            .withFilter("gender = 'female'")
            .build());
    assertEquals("Column expression cannot be blank", error.getMessage());
  }


  @Test
  void nonStringCoercibleColumn() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT);

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> executor.buildQuery(
            new ExtractRequestBuilder(subjectResource)
                .withColumn("id")
                .withColumn("name")
                .build(), ProjectionConstraint.FLAT
        )
    );
    assertEquals("Cannot render one of the columns in flat mode", error.getMessage());
  }

  @Test
  void emptyFilter() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT);

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
    mockResource(ResourceType.PATIENT);

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
    mockResource(ResourceType.PATIENT);
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

  @Test
  void structuredResult() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.PATIENT);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id", "id")
        .withColumn("name", "name_struct")
        .build();

    Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.UNCONSTRAINED);
    result = result.select(
        col("id"),
        explode_outer(col("name_struct").getField("given")).as("given")
    );

    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/structuredResult.tsv");
  }

  @Test
  void combineWithLiterals() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("'foo' combine 'bar'")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/combineWithLiterals.tsv");
  }

  @Test
  void combineWithUnequalCardinalities() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("name.given")
        .withColumn("name.family combine 'Smith'")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/combineWithUnequalCardinalities.tsv");
  }

  @Test
  void singularizeExpressions() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("name.family = 'Oberbrunner298'")
        .withColumn("name.family.first()")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/singularizeExpression.tsv");
  }


  @Test
  void aggregations() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final ExtractRequest request = new ExtractRequestBuilder(subjectResource)
        .withColumn("id")
        .withColumn("name.family")
        .withColumn("name.first().family")
        .withColumn("name.count()")
        .build();

    final Dataset<Row> result = executor.buildQuery(request, ProjectionConstraint.FLAT);
    assertThat(result)
        .hasRows(spark, "responses/ExtractQueryTest/aggregations.tsv");
  }


  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(dataSource, spark, resourceTypes);
  }

}
