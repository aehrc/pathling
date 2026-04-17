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

package au.csiro.pathling.fhirpath.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TraceResult;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Integration tests for {@link SingleInstanceEvaluator} that exercise the full evaluate lifecycle
 * including trace collection.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class SingleInstanceEvaluatorIntegrationTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  private Dataset<Row> patientDf;
  private FhirContext fhirContext;

  @BeforeEach
  void setUp() {
    final Patient patient = new Patient();
    patient.setId("Patient/1");
    patient.setGender(AdministrativeGender.MALE);
    patient.setActive(true);
    patient
        .addName()
        .setUse(HumanName.NameUse.OFFICIAL)
        .setFamily("Smith")
        .addGiven("John")
        .addGiven("James");

    final ObjectDataSource dataSource = new ObjectDataSource(spark, encoders, List.of(patient));
    patientDf = dataSource.read("Patient");
    fhirContext = encoders.getContext();
  }

  /** Helper to get the single ResultGroup from a non-context evaluation result. */
  private static ResultGroup getSingleGroup(final SingleInstanceEvaluationResult result) {
    assertEquals(1, result.getResultGroups().size());
    final ResultGroup group = result.getResultGroups().getFirst();
    assertNull(group.getContextKey());
    return group;
  }

  @Test
  void evaluateSimpleExpressionWithTrace() {
    // Evaluating a traced primitive expression should return both the result and trace entries.
    final SingleInstanceEvaluationResult result =
        SingleInstanceEvaluator.evaluate(
            patientDf, "Patient", fhirContext, "Patient.gender.trace('gender')", null, null);

    final ResultGroup group = getSingleGroup(result);

    // The result should contain the gender value.
    assertEquals(1, group.getResults().size());
    assertEquals("male", group.getResults().get(0).getValue());

    // The traces should contain one entry for the 'gender' label.
    assertEquals(1, group.getTraces().size());
    final TraceResult trace = group.getTraces().get(0);
    assertEquals("gender", trace.getLabel());
    assertEquals(1, trace.getValues().size());
    assertEquals("male", trace.getValues().get(0).getValue());
  }

  @Test
  void evaluateWithMultipleTraces() {
    // An expression with multiple trace() calls should capture all trace labels.
    final SingleInstanceEvaluationResult result =
        SingleInstanceEvaluator.evaluate(
            patientDf,
            "Patient",
            fhirContext,
            "Patient.name.where(use = 'official').given.trace('given').first().trace('first')",
            null,
            null);

    final ResultGroup group = getSingleGroup(result);

    // The result should be the first given name.
    assertEquals(1, group.getResults().size());
    assertEquals("John", group.getResults().get(0).getValue());

    // There should be two trace groups.
    assertEquals(2, group.getTraces().size());

    final TraceResult givenTrace = group.getTraces().get(0);
    assertEquals("given", givenTrace.getLabel());
    // The patient has two given names.
    assertEquals(2, givenTrace.getValues().size());

    final TraceResult firstTrace = group.getTraces().get(1);
    assertEquals("first", firstTrace.getLabel());
    assertEquals(1, firstTrace.getValues().size());
    assertEquals("John", firstTrace.getValues().get(0).getValue());
  }

  @Test
  void evaluateWithoutTrace() {
    // An expression without trace() should return an empty traces list.
    final SingleInstanceEvaluationResult result =
        SingleInstanceEvaluator.evaluate(
            patientDf, "Patient", fhirContext, "Patient.active", null, null);

    final ResultGroup group = getSingleGroup(result);
    assertEquals(1, group.getResults().size());
    assertEquals(true, group.getResults().get(0).getValue());
    assertTrue(group.getTraces().isEmpty());
  }

  @Test
  void evaluateTraceOnComplexType() {
    // Tracing a complex type should produce a JSON string without synthetic fields.
    final SingleInstanceEvaluationResult result =
        SingleInstanceEvaluator.evaluate(
            patientDf, "Patient", fhirContext, "Patient.name.first().trace('name')", null, null);

    final ResultGroup group = getSingleGroup(result);

    // The trace should contain a JSON representation of the HumanName.
    assertEquals(1, group.getTraces().size());
    final TraceResult trace = group.getTraces().get(0);
    assertEquals("name", trace.getLabel());
    assertEquals(1, trace.getValues().size());

    final Object traceValue = trace.getValues().get(0).getValue();
    assertInstanceOf(String.class, traceValue);
    final String json = (String) traceValue;
    assertTrue(json.contains("Smith"));
    // Synthetic fields should not appear in the JSON output.
    assertFalse(json.contains("_fid"));
  }
}
