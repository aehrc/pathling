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

import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TypedValue;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.family");

    assertNotNull(result);
    assertEquals(2, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("string", first.getType());
    assertEquals("Smith", first.getValue());
  }

  @Test
  void evaluateMultipleValues() {
    // Evaluating name.given should return all given names.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.given");

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
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "active");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("boolean", first.getType());
    assertEquals(true, first.getValue());
  }

  @Test
  void evaluateEmptyResult() {
    // Evaluating a path that matches nothing should return an empty list.
    // Use multipleBirth rather than deceased, as deceased is a choice type that requires ofType().
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "multipleBirthBoolean");

    assertNotNull(result);
    assertTrue(result.getResults().isEmpty());
  }

  @Test
  void evaluateComplexType() {
    // Evaluating name should return complex HumanName types.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name");

    assertNotNull(result);
    assertEquals(2, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("HumanName", first.getType());
    // The value should be a JSON string for complex types.
    assertNotNull(first.getValue());
  }

  @Test
  void evaluateDateExpression() {
    // Evaluating birthDate should return a date.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "birthDate");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
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
    // When a context expression is provided, the main expression is composed with the context.
    // For name.given, this returns all given names across all name entries.
    final SingleInstanceEvaluationResult result =
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
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "gender");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("code", first.getType());
    assertEquals("male", first.getValue());
  }

  @Test
  void evaluateWithFhirPathFunction() {
    // Evaluating with a FHIRPath function like count().
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.count()");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("integer", first.getType());
    assertEquals(2, first.getValue());
  }

  @Test
  void evaluateExistsFunction() {
    // Evaluating exists() should return boolean.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.exists()");

    assertNotNull(result);
    assertFalse(result.getResults().isEmpty());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("boolean", first.getType());
    assertEquals(true, first.getValue());
  }

  @Test
  void evaluateWhereFunction() {
    // Evaluating where() to filter.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name.where(use = 'official').family");

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
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
  void quantityResultCarriesOnlyTheElementsOfAQuantity() {
    // Evaluating a Quantity expression should return JSON holding the elements of the Quantity the
    // resource carries, and nothing the storage layout adds, such as its annotations.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Observation", OBSERVATION_JSON, "value.ofType(Quantity)");

    assertNotNull(result);
    assertFalse(result.getResults().isEmpty());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("Quantity", first.getType());

    final String json = (String) first.getValue();
    assertNotNull(json);
    final JsonObject quantity = JsonParser.parseString(json).getAsJsonObject();
    assertEquals(Set.of("value", "unit", "system", "code"), quantity.keySet());
    assertEquals("mmHg", quantity.get("unit").getAsString());
    assertEquals("mm[Hg]", quantity.get("code").getAsString());
  }

  @Test
  void quantityResultIsTheSameWithAContextExpression() {
    // A context expression must not change how a result is rendered, so a Quantity reached through
    // a context is rendered as it is without one.
    final SingleInstanceEvaluationResult withoutContext =
        pathling.evaluateFhirPath("Observation", OBSERVATION_JSON, "value.ofType(Quantity)");
    final SingleInstanceEvaluationResult withContext =
        pathling.evaluateFhirPath(
            "Observation", OBSERVATION_JSON, "$this", "value.ofType(Quantity)", null);

    assertEquals(
        withoutContext.getResults().stream().map(TypedValue::getValue).toList(),
        withContext.getResults().stream().map(TypedValue::getValue).toList());
  }

  @Test
  void complexTypeResultExcludesStorageFields() {
    // Any complex type result should hold only the elements of its type, with nothing the storage
    // layout adds, such as the previous layout's _fid or the Parquet on FHIR layout's metadata
    // groups and annotations, all of which begin with an underscore.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "name");

    assertNotNull(result);
    assertFalse(result.getResults().isEmpty());

    for (final TypedValue tv : result.getResults()) {
      final String json = (String) tv.getValue();
      assertNotNull(json);
      assertFalse(json.contains("_fid"), "JSON should not contain _fid");
      assertTrue(
          fieldNames(JsonParser.parseString(json)).stream().noneMatch(name -> name.startsWith("_")),
          "JSON should not contain a storage field: " + json);
    }
  }

  // ========== Rendering complex results as FHIR JSON (T100j) ==========

  private static final String LOCATION_JSON =
      """
      {
        "resourceType": "Location",
        "id": "loc",
        "position": {
          "longitude": -83.6945691,
          "latitude": 42.2565,
          "altitude": 0.5
        }
      }
      """;

  private static final String EXTENDED_OBSERVATION_JSON =
      """
      {
        "resourceType": "Observation",
        "id": "extended",
        "status": "final",
        "code": {"text": "x"},
        "extension": [
          {"url": "urn:weight", "valueDecimal": 72.25},
          {"url": "urn:note", "valueString": "n"}
        ],
        "valueQuantity": {
          "extension": [{"url": "urn:device", "valueString": "scale"}],
          "value": 120.0,
          "comparator": "<",
          "unit": "mmHg",
          "system": "http://unitsofmeasure.org",
          "code": "mm[Hg]"
        }
      }
      """;

  @Test
  void decimalsOfAStructureAreNumbersWithTheirSourceText() {
    // A structure returned whole holds its decimals as stored, which is text, and the JSON gives
    // each back as a number with the text of the source.
    final JsonObject position =
        singleObject(pathling.evaluateFhirPath("Location", LOCATION_JSON, "position"));

    assertEquals(Set.of("longitude", "latitude", "altitude"), position.keySet());
    assertNumber("-83.6945691", position.get("longitude"));
    assertNumber("42.2565", position.get("latitude"));
    assertNumber("0.5", position.get("altitude"));
  }

  @Test
  void quantityIsRenderedAsStoredWithItsExtensions() {
    // A quantity is rendered from the stored element, not from the structure the engine computes
    // with, so its value has the text of the source and its extensions are kept.
    final JsonObject quantity =
        singleObject(
            pathling.evaluateFhirPath(
                "Observation", EXTENDED_OBSERVATION_JSON, "value.ofType(Quantity)"));

    assertEquals(
        Set.of("extension", "value", "comparator", "unit", "system", "code"), quantity.keySet());
    assertNumber("120.0", quantity.get("value"));
    assertEquals("<", quantity.get("comparator").getAsString());
    final JsonObject extension = quantity.getAsJsonArray("extension").get(0).getAsJsonObject();
    assertEquals("urn:device", extension.get("url").getAsString());
    assertEquals("scale", extension.get("valueString").getAsString());
  }

  @Test
  void extensionIsRenderedWithItsTypedValue() {
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath(
            "Observation", EXTENDED_OBSERVATION_JSON, "extension.where(url = 'urn:weight')");
    final JsonObject extension = singleObject(result);

    assertEquals("Extension", result.getResults().getFirst().getType());
    assertEquals(Set.of("url", "valueDecimal"), extension.keySet());
    assertNumber("72.25", extension.get("valueDecimal"));
  }

  @Test
  void complexResultsAreRenderedTheSameWithAContextExpression() {
    for (final String expression :
        List.of("value.ofType(Quantity)", "extension", "code", "value.ofType(Quantity) | 7 'g'")) {
      final SingleInstanceEvaluationResult withoutContext =
          pathling.evaluateFhirPath("Observation", EXTENDED_OBSERVATION_JSON, expression);
      final SingleInstanceEvaluationResult withContext =
          pathling.evaluateFhirPath(
              "Observation", EXTENDED_OBSERVATION_JSON, "$this", expression, null);
      assertEquals(
          withoutContext.getResults().stream().map(TypedValue::getValue).toList(),
          withContext.getResults().stream().map(TypedValue::getValue).toList(),
          expression);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "value.ofType(Quantity)",
        "7.5 'mg'",
        "value.ofType(Quantity).toQuantity('mm[Hg]')",
        "(value.ofType(Quantity) | 7 'g').first()"
      })
  void quantityResultCarriesNoCanonicalAnnotation(@Nonnull final String expression) {
    // A quantity, stored or built by the engine, carries its canonical annotations, which belong to
    // the layout and not to FHIR, so the rendered result leaves them out.
    final JsonObject quantity =
        singleObject(pathling.evaluateFhirPath("Observation", OBSERVATION_JSON, expression));

    assertFalse(quantity.has("__canonical"));
    assertFalse(quantity.has("__canonical_exact"));
    assertTrue(quantity.has("code"));
  }

  @Test
  void systemQuantityIsRenderedAsAQuantity() {
    // A quantity the engine builds, such as a literal, is not stored. It is rendered with the
    // elements of a Quantity, its value a number, and nothing of the structure the engine computes
    // with.
    final JsonObject quantity =
        singleObject(pathling.evaluateFhirPath("Patient", PATIENT_JSON, "7.5 'mg'"));

    assertEquals(Set.of("value", "unit", "system", "code"), quantity.keySet());
    assertNumber("7.5", quantity.get("value"));
    assertEquals("mg", quantity.get("code").getAsString());
  }

  @Test
  void systemQuantityIsRenderedWithTheScaleOfItsValue() {
    // A System quantity holds its value as text (FR-059), so the value is rendered with the digits
    // it was given, without a scale companion.
    final JsonObject quantity =
        singleObject(pathling.evaluateFhirPath("Patient", PATIENT_JSON, "1.50 'mg'"));

    assertEquals(Set.of("value", "unit", "system", "code"), quantity.keySet());
    assertNumber("1.50", quantity.get("value"));
  }

  @Test
  void numberPromotedToAQuantityIsRenderedAsAQuantity() {
    // A quantity built from the result of an arithmetic operation has the same fixed shape as a
    // literal, and is rendered as a quantity. Its scale is not known, so its value has no trailing
    // fractional zeros.
    final JsonObject quantity =
        singleObject(pathling.evaluateFhirPath("Patient", PATIENT_JSON, "(1 + 2).toQuantity()"));

    assertEquals(Set.of("value", "unit", "system", "code"), quantity.keySet());
    assertNumber("3", quantity.get("value"));
    assertEquals("1", quantity.get("code").getAsString());
  }

  @Test
  void unionOfAStoredAndASystemQuantityIsRenderedAsQuantities() {
    // The stored quantity meets a System quantity, so both are System quantities (decision 82).
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath(
            "Observation", EXTENDED_OBSERVATION_JSON, "value.ofType(Quantity) | 7 'g'");

    assertEquals(2, result.getResults().size());
    for (final TypedValue value : result.getResults()) {
      final JsonObject quantity =
          JsonParser.parseString((String) value.getValue()).getAsJsonObject();
      assertTrue(quantity.get("value").getAsJsonPrimitive().isNumber(), quantity.toString());
      assertTrue(
          fieldNames(quantity).stream()
              .noneMatch(name -> name.startsWith("_") || name.endsWith("_scale")),
          quantity.toString());
    }
  }

  @Nonnull
  private static JsonObject singleObject(@Nonnull final SingleInstanceEvaluationResult result) {
    assertEquals(1, result.getResults().size(), result.getResults().toString());
    final Object value = result.getResults().getFirst().getValue();
    assertTrue(value instanceof String, String.valueOf(value));
    return JsonParser.parseString((String) value).getAsJsonObject();
  }

  /** Asserts that a JSON value is a number whose text is the given text. */
  private static void assertNumber(@Nonnull final String text, @Nonnull final JsonElement value) {
    assertTrue(value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber(), value.toString());
    assertEquals(text, value.toString());
  }

  /** Collects the names of every field of a JSON value, at any depth. */
  @Nonnull
  private static List<String> fieldNames(@Nonnull final JsonElement element) {
    if (element.isJsonObject()) {
      return element.getAsJsonObject().entrySet().stream()
          .flatMap(
              entry ->
                  Stream.concat(Stream.of(entry.getKey()), fieldNames(entry.getValue()).stream()))
          .toList();
    } else if (element.isJsonArray()) {
      return element.getAsJsonArray().asList().stream()
          .flatMap(item -> fieldNames(item).stream())
          .toList();
    } else {
      return List.of();
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
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("string", first.getType());
    assertEquals("hello", first.getValue());
  }

  @Test
  void evaluateWithIntegerVariable() {
    // An integer variable should be resolvable in expressions.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "%count", null, Map.of("count", 42));

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
    assertEquals("integer", first.getType());
    assertEquals(42, first.getValue());
  }

  @Test
  void evaluateWithBooleanVariable() {
    // A boolean variable should be resolvable in expressions.
    final SingleInstanceEvaluationResult result =
        pathling.evaluateFhirPath("Patient", PATIENT_JSON, "%flag", null, Map.of("flag", true));

    assertNotNull(result);
    assertEquals(1, result.getResults().size());

    final TypedValue first = result.getResults().getFirst();
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
    assertEquals(1, result.getResults().size());
    assertEquals(true, result.getResults().getFirst().getValue());
  }
}
