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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.FhirPathResult.TypedValue;
import java.util.List;
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

  // ========== Simple expression evaluation ==========

  @Test
  void evaluateStringExpression() {
    // Evaluating name.family should return the family names as strings.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.family");

    assertNotNull(result);
    assertEquals(2, result.getResults().size());

    final TypedValue first = result.getResults().get(0);
    assertEquals("string", first.getType());
    assertEquals("Smith", first.getValue());
  }

  @Test
  void evaluateMultipleValues() {
    // Evaluating name.given should return all given names.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.given");

    assertNotNull(result);
    assertEquals(3, result.getResults().size());

    final List<Object> values = result.getResults().stream().map(TypedValue::getValue).toList();
    assertTrue(values.contains("John"));
    assertTrue(values.contains("James"));
    assertTrue(values.contains("Johnny"));
  }

  @Test
  void evaluateBooleanExpression() {
    // Evaluating active should return a boolean.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "active");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().get(0);
    assertEquals("boolean", first.getType());
    assertEquals(true, first.getValue());
  }

  @Test
  void evaluateEmptyResult() {
    // Evaluating a path that matches nothing should return an empty list.
    // Use multipleBirth rather than deceased, as deceased is a choice type that requires ofType().
    final FhirPathResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "multipleBirthBoolean");

    assertNotNull(result);
    assertTrue(result.getResults().isEmpty());
  }

  @Test
  void evaluateComplexType() {
    // Evaluating name should return complex HumanName types.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name");

    assertNotNull(result);
    assertEquals(2, result.getResults().size());

    final TypedValue first = result.getResults().get(0);
    assertEquals("HumanName", first.getType());
    // The value should be a JSON string for complex types.
    assertNotNull(first.getValue());
  }

  @Test
  void evaluateDateExpression() {
    // Evaluating birthDate should return a date.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "birthDate");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().get(0);
    assertEquals("date", first.getType());
    assertEquals("1990-01-01", first.getValue());
  }

  @Test
  void evaluateWithReturnType() {
    // The return type should be inferred correctly.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.family");

    assertEquals("string", result.getExpectedReturnType());
  }

  @Test
  void evaluateBooleanReturnType() {
    // Boolean expressions should have boolean return type.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "active");

    assertEquals("boolean", result.getExpectedReturnType());
  }

  @Test
  void evaluateComplexReturnType() {
    // Complex type expressions should have the complex type as return type.
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name");

    assertEquals("HumanName", result.getExpectedReturnType());
  }

  // ========== Context expression evaluation ==========

  @Test
  void evaluateWithContextExpression() {
    // When a context expression is provided, the main expression is composed with the context.
    // For name.given, this returns all given names across all name entries.
    final FhirPathResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "given", "name", null);

    assertNotNull(result);
    // The composed path name.given returns all given names.
    assertEquals(3, result.getResults().size());

    final List<Object> values = result.getResults().stream().map(TypedValue::getValue).toList();
    assertTrue(values.contains("John"));
    assertTrue(values.contains("James"));
    assertTrue(values.contains("Johnny"));
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
    final FhirPathResult result = pathling.evaluateFhirPath("Patient", PATIENT_JSON, "gender");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().get(0);
    assertEquals("code", first.getType());
    assertEquals("male", first.getValue());
  }

  @Test
  void evaluateWithFhirPathFunction() {
    // Evaluating with a FHIRPath function like count().
    final FhirPathResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.count()");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().get(0);
    assertEquals("integer", first.getType());
    assertEquals(2, first.getValue());
  }

  @Test
  void evaluateExistsFunction() {
    // Evaluating exists() should return boolean.
    final FhirPathResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.exists()");

    assertNotNull(result);
    assertFalse(result.getResults().isEmpty());

    final TypedValue first = result.getResults().get(0);
    assertEquals("boolean", first.getType());
    assertEquals(true, first.getValue());
  }

  @Test
  void evaluateWhereFunction() {
    // Evaluating where() to filter.
    final FhirPathResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.where(use = 'official').family");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().get(0);
    assertEquals("string", first.getType());
    assertEquals("Smith", first.getValue());
  }
}
