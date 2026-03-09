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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TypedValue;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test for the FHIRPath repeatAll() function via the library API.
 *
 * <p>Evaluates repeatAll() against a Questionnaire JSON resource with nested items to verify that
 * all nesting levels are traversed correctly.
 *
 * @author Piotr Szul
 */
public class EvaluateRepeatAllTest {

  private static SparkSession spark;
  private static PathlingContext pathling;

  private static final String QUESTIONNAIRE_JSON =
      """
      {
        "resourceType": "Questionnaire",
        "id": "test-questionnaire",
        "status": "draft",
        "item": [
          {
            "linkId": "1",
            "type": "group",
            "item": [
              {
                "linkId": "1.1",
                "type": "group",
                "item": [
                  {
                    "linkId": "1.1.1",
                    "type": "display"
                  }
                ]
              }
            ]
          },
          {
            "linkId": "2",
            "type": "display"
          }
        ]
      }
      """;

  /** Set up Spark and PathlingContext with maxNestingLevel=3 to support deeply nested items. */
  @BeforeAll
  static void setUpAll() {
    spark = TestHelpers.spark();
    final EncodingConfiguration encodingConfig =
        EncodingConfiguration.builder().maxNestingLevel(3).build();
    pathling = PathlingContext.builder(spark).encodingConfiguration(encodingConfig).build();
  }

  /** Tear down Spark. */
  @AfterAll
  static void tearDownAll() {
    spark.stop();
  }

  @Test
  void evaluateRepeatAllLinkId() {
    // Evaluating repeatAll(item).linkId should return linkIds from all nesting levels.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Questionnaire", QUESTIONNAIRE_JSON, "repeatAll(item).linkId");

    assertNotNull(result);
    assertEquals(4, result.getResults().size());

    final Set<Object> linkIds =
        new HashSet<>(result.getResults().stream().map(TypedValue::getValue).toList());
    assertEquals(Set.of("1", "2", "1.1", "1.1.1"), linkIds);
  }

  @Test
  void evaluateRepeatAllCount() {
    // Evaluating repeatAll(item).count() should return the total number of items across all levels.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Questionnaire", QUESTIONNAIRE_JSON, "repeatAll(item).count()");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue countValue = result.getResults().getFirst();
    assertEquals("integer", countValue.getType());
    assertEquals(4, countValue.getValue());
  }

  @Test
  void itemsTraverseDeeperThanDepthLimit() {
    // Questionnaire items use different SQL types at each nesting level (due to schema
    // truncation), so each level is a cross-type traversal that does not consume depth budget.
    // With maxUnboundTraversalDepth=1, all 4 items across 3 nesting levels should still be
    // returned.
    final PathlingContext shallowDepthPathling =
        PathlingContext.builder(spark)
            .encodingConfiguration(EncodingConfiguration.builder().maxNestingLevel(3).build())
            .queryConfiguration(QueryConfiguration.builder().maxUnboundTraversalDepth(1).build())
            .build();

    final SingleInstanceEvaluationResult result =
        shallowDepthPathling.evaluateFhirPath(
            "Questionnaire", QUESTIONNAIRE_JSON, "repeatAll(item).linkId");

    assertNotNull(result);
    assertEquals(4, result.getResults().size());

    final Set<Object> linkIds =
        new HashSet<>(result.getResults().stream().map(TypedValue::getValue).toList());
    assertEquals(Set.of("1", "2", "1.1", "1.1.1"), linkIds);
  }

  @Test
  void extensionsLimitedByDepth() {
    // Extensions have the same SQL type at every nesting level, so each level is a same-type
    // traversal that consumes depth budget. With maxUnboundTraversalDepth=1, only extensions up
    // to depth 1 should be returned — fewer than the 3 total levels in the data.
    final String patientJson =
        """
        {
          "resourceType": "Patient",
          "id": "test-patient",
          "extension": [
            {
              "url": "http://example.com/ext1",
              "extension": [
                {
                  "url": "http://example.com/ext2",
                  "extension": [
                    {
                      "url": "http://example.com/ext3",
                      "valueString": "deep"
                    }
                  ]
                }
              ]
            }
          ]
        }
        """;

    // First verify baseline: with default depth (10), all 3 extensions are returned.
    final SingleInstanceEvaluationResult baselineResult =
        pathling.evaluateFhirPath("Patient", patientJson, "repeatAll(extension).url");
    assertNotNull(baselineResult);
    assertEquals(3, baselineResult.getResults().size());

    // Now with maxUnboundTraversalDepth=1, fewer extensions should be returned.
    final PathlingContext shallowDepthPathling =
        PathlingContext.builder(spark)
            .encodingConfiguration(EncodingConfiguration.builder().maxNestingLevel(3).build())
            .queryConfiguration(QueryConfiguration.builder().maxUnboundTraversalDepth(1).build())
            .build();

    final SingleInstanceEvaluationResult limitedResult =
        shallowDepthPathling.evaluateFhirPath("Patient", patientJson, "repeatAll(extension).url");

    assertNotNull(limitedResult);
    final Set<Object> urls =
        new HashSet<>(limitedResult.getResults().stream().map(TypedValue::getValue).toList());
    // With maxDepth=1: ext1 at depth 0 + ext2 at depth 1 = 2 extensions. ext3 is excluded
    // because reaching it would require depth 2.
    assertEquals(
        Set.of("http://example.com/ext1", "http://example.com/ext2"),
        urls,
        "Expected only 2 extensions with maxUnboundTraversalDepth=1");
  }
}
