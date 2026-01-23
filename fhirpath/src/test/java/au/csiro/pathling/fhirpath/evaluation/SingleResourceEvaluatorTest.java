/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.Parser;
import ca.uhn.fhir.context.FhirContext;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SingleResourceEvaluator} and related classes.
 */
class SingleResourceEvaluatorTest {

  private FhirContext fhirContext;
  private Parser parser;

  @BeforeEach
  void setUp() {
    fhirContext = FhirContext.forR4();
    parser = new Parser();
  }

  @Nested
  class BuilderTests {

    @Test
    void create_createsEvaluator() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .build();

      assertNotNull(evaluator);
      assertEquals(ResourceType.PATIENT, evaluator.getSubjectResource());
    }

    @Test
    void defaultCrossResourceStrategy_isFail() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .build();

      // Attempting to evaluate an expression that references a foreign resource should fail
      final FhirPath fhirPath = parser.parse("Observation");
      assertThrows(UnsupportedOperationException.class,
          () -> evaluator.evaluate(fhirPath));
    }

    @Test
    void emptyStrategy_allowsForeignResourceReferences() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
          .build();

      // Evaluating an expression that references a foreign resource should not throw
      final FhirPath fhirPath = parser.parse("Observation");
      final Collection result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      // The result should have the correct type but a null column
      assertTrue(result.getFhirType().isPresent());
      assertEquals("Observation", result.getFhirType().get().toCode());
    }
  }

  @Nested
  class EvaluationTests {

    @Test
    void evaluate_simpleFieldPath_returnsCollection() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .build();

      final FhirPath fhirPath = parser.parse("Patient.name");
      final Collection result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      assertTrue(result.getFhirType().isPresent());
      assertEquals("HumanName", result.getFhirType().get().toCode());
    }

    @Test
    void evaluate_subjectResourcePath_returnsSubjectResource() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .build();

      final FhirPath fhirPath = parser.parse("Patient");
      final Collection result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      assertTrue(result.getFhirType().isPresent());
      assertEquals("Patient", result.getFhirType().get().toCode());
    }

    @Test
    void evaluate_nestedPath_returnsCorrectType() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .build();

      final FhirPath fhirPath = parser.parse("Patient.name.family");
      final Collection result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      assertTrue(result.getFhirType().isPresent());
      assertEquals("string", result.getFhirType().get().toCode());
    }

    @Test
    void evaluate_booleanExpression_returnsBooleanCollection() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .build();

      final FhirPath fhirPath = parser.parse("Patient.active");
      final Collection result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      assertTrue(result.getFhirType().isPresent());
      assertEquals("boolean", result.getFhirType().get().toCode());
    }
  }

  @Nested
  class CrossResourceStrategyTests {

    @Test
    void failStrategy_throwsOnForeignResource() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .withCrossResourceStrategy(CrossResourceStrategy.FAIL)
          .build();

      final FhirPath fhirPath = parser.parse("Observation");

      final UnsupportedOperationException exception = assertThrows(
          UnsupportedOperationException.class,
          () -> evaluator.evaluate(fhirPath));

      assertTrue(exception.getMessage().contains("Observation"));
      assertTrue(exception.getMessage().contains("Patient"));
    }

    @Test
    void emptyStrategy_returnEmptyCollectionForForeignResource() {
      final SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
          .build();

      final FhirPath fhirPath = parser.parse("Observation");
      final Collection result = evaluator.evaluate(fhirPath);

      assertNotNull(result);
      // Should have correct type
      assertTrue(result.getFhirType().isPresent());
      assertEquals("Observation", result.getFhirType().get().toCode());
    }

    @Test
    void subjectResource_resolvedRegardlessOfStrategy() {
      final SingleResourceEvaluator evaluatorFail = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .withCrossResourceStrategy(CrossResourceStrategy.FAIL)
          .build();

      final SingleResourceEvaluator evaluatorEmpty = SingleResourceEvaluatorBuilder
          .create(ResourceType.PATIENT, fhirContext)
          .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
          .build();

      final FhirPath fhirPath = parser.parse("Patient");

      // Both should work for the subject resource
      final Collection resultFail = evaluatorFail.evaluate(fhirPath);
      final Collection resultEmpty = evaluatorEmpty.evaluate(fhirPath);

      assertNotNull(resultFail);
      assertNotNull(resultEmpty);
      assertEquals("Patient", resultFail.getFhirType().orElseThrow().toCode());
      assertEquals("Patient", resultEmpty.getFhirType().orElseThrow().toCode());
    }
  }

  @Nested
  class ResourceResolverTests {

    @Test
    void defaultResolver_resolveSubjectResource_returnsResourceCollection() {
      final ResourceResolver resolver = new FhirResourceResolver(
          ResourceType.PATIENT, fhirContext, CrossResourceStrategy.FAIL);

      final var result = resolver.resolveSubjectResource();

      assertNotNull(result);
      assertEquals("Patient", result.getFhirType().orElseThrow().toCode());
    }

    @Test
    void defaultResolver_resolveResource_subjectCode_returnsResource() {
      final ResourceResolver resolver = new FhirResourceResolver(
          ResourceType.PATIENT, fhirContext, CrossResourceStrategy.FAIL);

      final var result = resolver.resolveResource("Patient");

      assertTrue(result.isPresent());
      assertEquals("Patient", result.get().getFhirType().orElseThrow().toCode());
    }

    @Test
    void defaultResolver_resolveResource_foreignCode_failStrategy_throws() {
      final ResourceResolver resolver = new FhirResourceResolver(
          ResourceType.PATIENT, fhirContext, CrossResourceStrategy.FAIL);

      assertThrows(UnsupportedOperationException.class,
          () -> resolver.resolveResource("Observation"));
    }

    @Test
    void defaultResolver_resolveResource_foreignCode_emptyStrategy_returnsEmptyCollection() {
      final ResourceResolver resolver = new FhirResourceResolver(
          ResourceType.PATIENT, fhirContext, CrossResourceStrategy.EMPTY);

      final var result = resolver.resolveResource("Observation");

      assertTrue(result.isPresent());
      assertEquals("Observation", result.get().getFhirType().orElseThrow().toCode());
    }

    @Test
    void defaultResolver_resolveResource_unknownCode_returnsEmpty() {
      // Unknown/invalid FHIR resource codes return Optional.empty() regardless of strategy
      // This allows the parser to distinguish between field names (e.g., "CustomField")
      // and cross-resource references
      final ResourceResolver resolverEmpty = new FhirResourceResolver(
          ResourceType.PATIENT, fhirContext, CrossResourceStrategy.EMPTY);
      final ResourceResolver resolverFail = new FhirResourceResolver(
          ResourceType.PATIENT, fhirContext, CrossResourceStrategy.FAIL);

      assertTrue(resolverEmpty.resolveResource("UnknownType").isEmpty());
      assertTrue(resolverFail.resolveResource("UnknownType").isEmpty());
    }

    @Test
    void defaultResolver_getSubjectResource_returnsConfiguredType() {
      final ResourceResolver resolver = new FhirResourceResolver(
          ResourceType.CONDITION, fhirContext, CrossResourceStrategy.FAIL);

      assertEquals(ResourceType.CONDITION, resolver.getSubjectResource());
    }
  }
}
