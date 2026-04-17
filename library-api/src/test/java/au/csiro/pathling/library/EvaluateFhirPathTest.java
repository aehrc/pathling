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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.evaluation.ResultGroup;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TypedValue;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for the single-resource FHIRPath evaluation method on PathlingContext.
 *
 * @author John Grimes
 */
public class EvaluateFhirPathTest {

  private static SparkSession spark;
  private static PathlingContext pathling;

  private static final String PATIENT_JSON =
      """
      {
        "resourceType": "Patient",
        "id": "example",
        "active": true,
        "gender": "male",
        "birthDate": "1990-01-01",
        "name": [
          {
            "use": "official",
            "family": "Smith",
            "given": ["John", "James"]
          },
          {
            "use": "nickname",
            "family": "Smith",
            "given": ["Johnny"]
          }
        ],
        "telecom": [
          {
            "system": "phone",
            "value": "555-1234"
          }
        ],
        "address": [
          {
            "city": "Melbourne",
            "state": "VIC"
          }
        ]
      }
      """;

  /** Set up Spark and PathlingContext once for all tests. */
  @BeforeAll
  static void setUpAll() {
    spark = TestHelpers.spark();
    pathling = PathlingContext.create(spark);
  }

  /** Tear down Spark. */
  @AfterAll
  static void tearDownAll() {
    spark.stop();
  }

  /**
   * Helper to get the results from the first (and typically only) ResultGroup in a non-context
   * evaluation.
   */
  @SuppressWarnings("SameParameterValue")
  private static ResultGroup getSingleGroup(
      @SuppressWarnings("SameParameterValue") final SingleInstanceEvaluationResult result) {
    assertEquals(1, result.getResultGroups().size());
    final ResultGroup group = result.getResultGroups().getFirst();
    assertNull(group.getContextKey());
    return group;
  }

  // ========== Simple expression evaluation ==========

  @Test
  void evaluateStringExpression() {
    // Evaluating name.family should return the family names as strings.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.family");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(2, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("string", first.getType());
    assertEquals("Smith", first.getValue());
  }

  @Test
  void evaluateMultipleValues() {
    // Evaluating name.given should return all given names.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.given");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(3, group.getResults().size());

    final List<Object> values = group.getResults().stream().map(TypedValue::getValue).toList();
    assertTrue(values.contains("John"));
    assertTrue(values.contains("James"));
    assertTrue(values.contains("Johnny"));
  }

  @Test
  void evaluateBooleanExpression() {
    // Evaluating active should return a boolean.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "active");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("boolean", first.getType());
    assertEquals(true, first.getValue());
  }

  @Test
  void evaluateEmptyResult() {
    // Evaluating a path that matches nothing should return an empty list.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "multipleBirthBoolean");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertTrue(group.getResults().isEmpty());
  }

  @Test
  void evaluateComplexType() {
    // Evaluating name should return complex HumanName types.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(2, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("HumanName", first.getType());
    assertNotNull(first.getValue());
  }

  @Test
  void evaluateDateExpression() {
    // Evaluating birthDate should return a date.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "birthDate");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("date", first.getType());
    assertEquals("1990-01-01", first.getValue());
  }

  @Test
  void evaluateWithReturnType() {
    // The return type should be inferred correctly.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.family");

    assertEquals("string", result.getExpectedReturnType());
  }

  @Test
  void evaluateBooleanReturnType() {
    // Boolean expressions should have boolean return type.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "active");

    assertEquals("boolean", result.getExpectedReturnType());
  }

  @Test
  void evaluateComplexReturnType() {
    // Complex type expressions should have the complex type as return type.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name");

    assertEquals("HumanName", result.getExpectedReturnType());
  }

  // ========== Context expression evaluation ==========

  @Test
  void evaluateWithContextExpression() {
    // When a context expression is provided, each context element produces its own ResultGroup.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "given", "name", null);

    assertNotNull(result);
    assertEquals(2, result.getResultGroups().size());

    // First name entry has given names "John" and "James".
    final ResultGroup group0 = result.getResultGroups().get(0);
    assertEquals("name[0]", group0.getContextKey());
    final List<Object> values0 = group0.getResults().stream().map(TypedValue::getValue).toList();
    assertTrue(values0.contains("John"));
    assertTrue(values0.contains("James"));

    // Second name entry has given name "Johnny".
    final ResultGroup group1 = result.getResultGroups().get(1);
    assertEquals("name[1]", group1.getContextKey());
    assertEquals(1, group1.getResults().size());
    assertEquals("Johnny", group1.getResults().getFirst().getValue());

    // Without trace() calls, each group should have empty traces.
    for (final ResultGroup group : result.getResultGroups()) {
      assertTrue(group.getTraces().isEmpty());
    }
  }

  @Test
  void contextEvaluationWithMultiSegmentPath() {
    // A multi-segment context expression should produce keys like "telecom[0]".
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "value", "telecom", null);

    assertNotNull(result);
    assertEquals(1, result.getResultGroups().size());

    final ResultGroup group = result.getResultGroups().getFirst();
    assertEquals("telecom[0]", group.getContextKey());
    assertEquals(1, group.getResults().size());
    assertEquals("555-1234", group.getResults().getFirst().getValue());
  }

  @Test
  void contextEvaluationWithTraceIsolation() {
    // Traces should be scoped to each context element.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath(
            "Patient", PATIENT_JSON, "trace('trc').given.first()", "name", null);

    assertNotNull(result);
    assertEquals(2, result.getResultGroups().size());

    // Each group should have its own trace entries.
    for (final ResultGroup group : result.getResultGroups()) {
      assertFalse(group.getTraces().isEmpty(), "Each group should have trace output");
      assertEquals("trc", group.getTraces().getFirst().getLabel());
    }
  }

  @Test
  void nullContextProducesNoResultGroups() {
    // When the context expression evaluates to null, zero groups are returned.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "value", "communication", null);

    assertNotNull(result);
    assertTrue(result.getResultGroups().isEmpty());
  }

  @Test
  void emptyArrayContextProducesNoResultGroups() {
    // When the context expression evaluates to an empty array, zero groups are returned.
    final String patientWithEmptyContact =
        """
        {
          "resourceType": "Patient",
          "id": "empty-contact",
          "contact": []
        }
        """;
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", patientWithEmptyContact, "name.text", "contact", null);

    assertNotNull(result);
    assertTrue(result.getResultGroups().isEmpty());
  }

  @Test
  void scalarContextProducesUnindexedKey() {
    // When the context expression evaluates to a scalar, the key has no index.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "toString()", "gender", null);

    assertNotNull(result);
    assertEquals(1, result.getResultGroups().size());

    final ResultGroup group = result.getResultGroups().getFirst();
    assertEquals("gender", group.getContextKey());
    assertFalse(group.getResults().isEmpty());
  }

  // ========== Error cases ==========

  @Test
  void invalidExpressionThrowsException() {
    // A syntactically invalid expression should throw.
    assertThrows(
        Exception.class, () -> pathling.evaluateFhirPath("Patient", PATIENT_JSON, "!!invalid!!"));
  }

  @Test
  void evaluateGenderCode() {
    // Evaluating gender should return a string code value.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "gender");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("code", first.getType());
    assertEquals("male", first.getValue());
  }

  @Test
  void evaluateWithFhirPathFunction() {
    // Evaluating with a FHIRPath function like count().
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.count()");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("integer", first.getType());
    assertEquals(2, first.getValue());
  }

  @Test
  void evaluateExistsFunction() {
    // Evaluating exists() should return boolean.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.exists()");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertFalse(group.getResults().isEmpty());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("boolean", first.getType());
    assertEquals(true, first.getValue());
  }

  @Test
  void evaluateWhereFunction() {
    // Evaluating where() to filter.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.where(use = 'official').family");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("string", first.getType());
    assertEquals("Smith", first.getValue());
  }

  // ========== Sanitisation tests ==========

  private static final String OBSERVATION_JSON =
      """
      {
        "resourceType": "Observation",
        "id": "bp-example",
        "status": "final",
        "code": {
          "coding": [
            {
              "system": "http://loinc.org",
              "code": "85354-9"
            }
          ]
        },
        "valueQuantity": {
          "value": 120.0,
          "unit": "mmHg",
          "system": "http://unitsofmeasure.org",
          "code": "mm[Hg]"
        }
      }
      """;

  @Test
  void quantityResultExcludesSyntheticFields() {
    // Evaluating a Quantity expression should return JSON without synthetic fields.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Observation", OBSERVATION_JSON, "value.ofType(Quantity)");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertFalse(group.getResults().isEmpty());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("Quantity", first.getType());

    final String json = (String) first.getValue();
    assertNotNull(json);
    assertFalse(json.contains("value_scale"), "JSON should not contain value_scale");
    assertFalse(
        json.contains("_value_canonicalized"), "JSON should not contain _value_canonicalized");
    assertFalse(
        json.contains("_code_canonicalized"), "JSON should not contain _code_canonicalized");
    assertFalse(json.contains("_fid"), "JSON should not contain _fid");
  }

  @Test
  void complexTypeResultExcludesFidField() {
    // Any complex type result should not contain the _fid field.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name");

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertFalse(group.getResults().isEmpty());

    for (final TypedValue tv : group.getResults()) {
      final String json = (String) tv.getValue();
      assertNotNull(json);
      assertFalse(json.contains("_fid"), "JSON should not contain _fid");
    }
  }

  // ========== Variable resolution tests ==========

  @Test
  void evaluateWithStringVariable() {
    // A string variable should be resolvable in expressions.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath(
            "Patient", PATIENT_JSON, "%greeting", null, Map.of("greeting", "hello"));

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("string", first.getType());
    assertEquals("hello", first.getValue());
  }

  @Test
  void evaluateWithIntegerVariable() {
    // An integer variable should be resolvable in expressions.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "%count", null, Map.of("count", 42));

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("integer", first.getType());
    assertEquals(42, first.getValue());
  }

  @Test
  void evaluateWithBooleanVariable() {
    // A boolean variable should be resolvable in expressions.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "%flag", null, Map.of("flag", true));

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());

    final TypedValue first = group.getResults().getFirst();
    assertEquals("boolean", first.getType());
    assertEquals(true, first.getValue());
  }

  @Test
  void evaluateWithNoVariables() {
    // When no variables are provided, evaluation should proceed with default environment
    // variables only.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "active", null, null);

    assertNotNull(result);
    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());
    assertEquals(true, group.getResults().getFirst().getValue());
  }
}
