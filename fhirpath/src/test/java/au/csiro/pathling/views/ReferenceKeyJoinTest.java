/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.views;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.utilities.Streams;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the join that resource keys and reference keys feed, over the reference fixtures in {@code
 * viewTests/references.json}. The view test format cannot express a join, so this class runs one
 * view producing resource keys and another producing reference keys through the real view executor,
 * and joins their results.
 *
 * <p>The primitives are covered by the view test cases in the same file. What is pinned here is
 * that a resolvable reference joins its target, and that a reference whose target is absent, a
 * reference to a resource type not present, a versioned reference and an absent reference all join
 * nothing.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class ReferenceKeyJoinTest {

  private static final String FIXTURE = "/viewTests/references.json";

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired Gson gson;

  private FhirViewExecutor executor;

  @BeforeEach
  void setUp() throws IOException {
    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, fhirEncoders, readFixtureResources());
    executor = new FhirViewExecutor(fhirEncoders.getContext(), dataSource);
  }

  @Test
  void singularReferenceJoinsOnlyItsResolvableTarget() {
    final Dataset<Row> patients = run(resourceKeyView("Patient")).alias("p");
    final Dataset<Row> observations =
        run("""
        {
          "resource": "Observation",
          "select": [
            {
              "column": [
                { "name": "obs_id", "path": "id" },
                { "name": "ref_key", "path": "subject.getReferenceKey()" }
              ]
            }
          ]
        }
        """)
            .alias("o");

    final List<String> joined =
        observations
            .join(patients, observations.col("ref_key").equalTo(patients.col("key")), "left_outer")
            .select("o.obs_id", "p.target_id")
            .collectAsList()
            .stream()
            .map(ReferenceKeyJoinTest::pair)
            .toList();

    // Only the resolvable reference finds its target. A target that is absent, a type that is not
    // present, a versioned reference (whose key keeps its version under the current rule) and an
    // absent reference all join nothing.
    assertThat(joined)
        .containsExactlyInAnyOrder("o1->p1", "o2->null", "o3->null", "o4->null", "o5->null");
  }

  @Test
  void repeatingReferenceJoinsEachResolvableTarget() {
    final Dataset<Row> targets =
        run(resourceKeyView("Patient"))
            .unionByName(run(resourceKeyView("Practitioner")))
            .alias("t");
    final Dataset<Row> performers =
        run("""
        {
          "resource": "Observation",
          "select": [
            { "column": [ { "name": "obs_id", "path": "id" } ] },
            {
              "forEach": "performer",
              "column": [ { "name": "ref_key", "path": "getReferenceKey()" } ]
            }
          ]
        }
        """)
            .alias("r");

    final List<String> joined =
        performers
            .join(targets, performers.col("ref_key").equalTo(targets.col("key")), "left_outer")
            .select("r.ref_key", "t.key")
            .collectAsList()
            .stream()
            .map(ReferenceKeyJoinTest::pair)
            .toList();

    // Each target of the repeating element is joined independently, across target types. The
    // target that is absent and the logical reference without a reference string join nothing.
    assertThat(joined)
        .containsExactlyInAnyOrder(
            "Practitioner/pr1->Practitioner/pr1",
            "Patient/p2->Patient/p2",
            "Practitioner/pr-missing->null",
            "null->null");
  }

  @Test
  void innerJoinCountsOneRowPerResolvableReference() {
    final Dataset<Row> targets =
        run(resourceKeyView("Patient")).unionByName(run(resourceKeyView("Practitioner")));
    final Dataset<Row> references =
        run(
            """
            {
              "resource": "Observation",
              "select": [
                {
                  "unionAll": [
                    { "column": [ { "name": "ref_key", "path": "subject.getReferenceKey()" } ] },
                    {
                      "forEach": "performer",
                      "column": [ { "name": "ref_key", "path": "getReferenceKey()" } ]
                    }
                  ]
                }
              ]
            }
            """);

    // One subject and two performers resolve, so the inner join yields exactly three rows.
    assertThat(
            references.join(targets, references.col("ref_key").equalTo(targets.col("key"))).count())
        .isEqualTo(3L);
  }

  @Nonnull
  private static String resourceKeyView(@Nonnull final String resourceType) {
    return """
    {
      "resource": "%s",
      "select": [
        {
          "column": [
            { "name": "target_id", "path": "id" },
            { "name": "key", "path": "getResourceKey()" }
          ]
        }
      ]
    }
    """
        .formatted(resourceType);
  }

  @Nonnull
  private Dataset<Row> run(@Nonnull final String viewJson) {
    return executor.buildQuery(gson.fromJson(viewJson, FhirView.class));
  }

  @Nonnull
  private static String pair(@Nonnull final Row row) {
    return row.get(0) + "->" + row.get(1);
  }

  @Nonnull
  private List<IBaseResource> readFixtureResources() throws IOException {
    final IParser parser = fhirEncoders.getContext().newJsonParser();
    try (final InputStream input = getClass().getResourceAsStream(FIXTURE)) {
      final JsonNode fixture = new ObjectMapper().readTree(input);
      return Streams.streamOf(fixture.get("resources").elements())
          .map(resource -> (IBaseResource) parser.parseResource(resource.toString()))
          .toList();
    }
  }
}
