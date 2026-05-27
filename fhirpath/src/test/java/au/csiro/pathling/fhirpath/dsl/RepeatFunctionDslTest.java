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

package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemComponent;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemType;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseItemAnswerComponent;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseItemComponent;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseStatus;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.DynamicTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Tests for the FHIRPath repeat() function.
 *
 * <p>Focuses on the differences from repeatAll(): equality-based deduplication and handling of
 * self-referential primitive traversal.
 */
@Import(RepeatFunctionDslTest.Config.class)
public class RepeatFunctionDslTest extends FhirPathDslTestBase {

  /**
   * Provides a FhirEncoders bean with maxNestingLevel=3 to support deeply nested items in test
   * Questionnaire resources.
   */
  @TestConfiguration
  static class Config {

    @Bean
    @Nonnull
    FhirEncoders fhirEncoders() {
      return FhirEncoders.forR4()
          .withExtensionsEnabled(true)
          .withAllOpenTypes()
          .withMaxNestingLevel(3)
          .getOrCreate();
    }
  }

  /**
   * Creates a Questionnaire with items nested 3 levels deep.
   *
   * <p>Structure:
   *
   * <pre>
   *   item "1" (group)
   *     item "1.1" (group)
   *       item "1.1.1" (display)
   *   item "2" (display)
   * </pre>
   */
  private static Questionnaire createQuestionnaire() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("test-questionnaire");
    questionnaire.setStatus(PublicationStatus.DRAFT);

    final QuestionnaireItemComponent item1 = questionnaire.addItem();
    item1.setLinkId("1");
    item1.setType(QuestionnaireItemType.GROUP);

    final QuestionnaireItemComponent item1Sub1 = item1.addItem();
    item1Sub1.setLinkId("1.1");
    item1Sub1.setType(QuestionnaireItemType.GROUP);

    final QuestionnaireItemComponent item1Sub1Sub1 = item1Sub1.addItem();
    item1Sub1Sub1.setLinkId("1.1.1");
    item1Sub1Sub1.setType(QuestionnaireItemType.DISPLAY);

    final QuestionnaireItemComponent item2 = questionnaire.addItem();
    item2.setLinkId("2");
    item2.setType(QuestionnaireItemType.DISPLAY);

    return questionnaire;
  }

  private static Patient createPatient() {
    final Patient patient = new Patient();
    patient.setId("test-patient");
    patient.addName().setFamily("Smith");
    patient.addName().setFamily("Jones");
    patient.setGender(AdministrativeGender.MALE);
    patient.setMaritalStatus(
        new CodeableConcept(new Coding("http://example.com/cs", "M", "Married")));

    patient.addExtension(
        new Extension("http://example.com/simple", new StringType("simple-value")));

    final Extension parent = new Extension("http://example.com/parent");
    parent.addExtension(new Extension("http://example.com/child", new StringType("child-value")));
    patient.addExtension(parent);

    return patient;
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatBasicTreeTraversal() {
    return builder()
        .withResource(createQuestionnaire())
        .group("repeat() basic traversal")
        .testEquals(
            List.of("1", "2", "1.1", "1.1.1"),
            "repeat(item).linkId",
            "repeat(item).linkId returns linkIds from all nesting levels")
        .testEquals(
            4, "repeat(item).count()", "repeat(item).count() returns total items across all levels")
        .group("repeat() with filtering")
        .testEquals(
            List.of("1", "1.1"),
            "repeat(item).where(type = 'group').linkId",
            "repeat(item).where(type = 'group') filters items from all nesting levels")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatPrimitiveDedup() {
    return builder()
        .withResource(createPatient())
        .group("repeat() primitive self-referential deduplication")
        .testEquals(
            "male",
            "gender.repeat($this)",
            "repeat($this) on a primitive returns the deduplicated value")
        .testEquals(
            1,
            "gender.repeat($this).count()",
            "repeat($this) on a primitive deduplicates to a single value")
        .testEquals(
            "someValue",
            "gender.repeat('someValue')",
            "repeat('literal') on a primitive returns the constant deduplicated")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatComplexTypeNoDedup() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("dup-questionnaire");
    questionnaire.setStatus(PublicationStatus.DRAFT);

    final QuestionnaireItemComponent item1 = questionnaire.addItem();
    item1.setLinkId("A");
    item1.setType(QuestionnaireItemType.GROUP);

    final QuestionnaireItemComponent item1Child = item1.addItem();
    item1Child.setLinkId("B");
    item1Child.setType(QuestionnaireItemType.DISPLAY);

    final QuestionnaireItemComponent item2 = questionnaire.addItem();
    item2.setLinkId("B");
    item2.setType(QuestionnaireItemType.DISPLAY);

    return builder()
        .withResource(questionnaire)
        .group("repeat() complex type - no dedup applied")
        .testEquals(
            List.of("A", "B", "B"),
            "repeat(item).linkId",
            "repeat(item).linkId preserves items with the same linkId from different nesting"
                + " levels because complex types are not deduplicated")
        .testEquals(
            3, "repeat(item).count()", "repeat(item).count() includes all items without dedup")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatEmptyInput() {
    final Questionnaire empty = new Questionnaire();
    empty.setId("empty-questionnaire");
    empty.setStatus(PublicationStatus.DRAFT);

    return builder()
        .withResource(empty)
        .group("repeat() empty input")
        .testEmpty("repeat(item)", "repeat() on a resource with no items returns empty collection")
        .testEquals(0, "repeat(item).count()", "repeat(item).count() returns 0 for empty input")
        .group("repeat() empty literal")
        .testEmpty(
            "{}.repeat($this)", "repeat() on the FHIRPath empty literal returns empty collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatExtensionTraversal() {
    return builder()
        .withResource(createPatient())
        .group("repeat() extension traversal")
        .testEquals(
            List.of(
                "http://example.com/simple",
                "http://example.com/parent",
                "http://example.com/child"),
            "repeat(extension).url",
            "repeat(extension) recursively collects all extensions including nested"
                + " sub-extensions")
        .build();
  }

  /**
   * Creates an Observation with a populated value[x] choice element (Quantity), for testing choice
   * type interactions with repeat().
   *
   * @return an Observation resource with a Quantity value
   */
  private static Observation createObservation() {
    final Observation observation = new Observation();
    observation.setId("test-observation");
    observation.setStatus(Observation.ObservationStatus.FINAL);
    observation.setValue(new Quantity(42.0).setUnit("mg").setSystem("http://unitsofmeasure.org"));
    return observation;
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatChoiceTypeExpressions() {
    return builder()
        .withResource(createObservation())
        .group("repeat() choice type — indeterminate type guard")
        .testError(
            "indeterminate FHIR type",
            "value.repeat($this)",
            "repeat($this) on a choice type raises an indeterminate type error")
        .group("repeat() choice type — ofType resolves and terminates")
        .testEquals(
            42.0,
            "repeat(value.ofType(Quantity)).value",
            "repeat(value.ofType(Quantity)) returns the Quantity value (level_1 empty, early"
                + " return)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatSelfReferentialComplex() {
    return builder()
        .withResource(createPatient())
        .group("repeat() self-referential complex expressions")
        .testEquals(
            1,
            "repeat($this).count()",
            "repeat($this) on a resource returns the resource (dedup collapses depth-limited"
                + " copies)")
        .testEquals(
            "male",
            "name.repeat(%resource).gender",
            "repeat(%resource) returns the patient's gender (dedup collapses to single resource)")
        .build();
  }

  /**
   * Creates a QuestionnaireResponse with items nested through both item and answer.item paths.
   *
   * <p>Structure:
   *
   * <pre>
   *   item "1" (Demographics)
   *     item "1.1" (Name)
   *       answer "Jane Doe"
   *         item "1.1.1" (Name confirmed)
   *           answer true
   *     item "1.2" (Date of Birth)
   *       answer "1990-01-15"
   *   item "2" (Medical History)
   *     item "2.1" (Conditions)
   *       answer true
   *         item "2.1.1" (Condition details)
   *           answer "Type 2 Diabetes"
   * </pre>
   */
  private static QuestionnaireResponse createQuestionnaireResponse() {
    final QuestionnaireResponse qr = new QuestionnaireResponse();
    qr.setId("example");
    qr.setStatus(QuestionnaireResponseStatus.COMPLETED);

    // item "1" (Demographics).
    final QuestionnaireResponseItemComponent item1 = qr.addItem();
    item1.setLinkId("1");
    item1.setText("Demographics");

    // item "1.1" (Name).
    final QuestionnaireResponseItemComponent item1Sub1 = item1.addItem();
    item1Sub1.setLinkId("1.1");
    item1Sub1.setText("Name");

    // answer "Jane Doe" with nested item "1.1.1".
    final QuestionnaireResponseItemAnswerComponent answer1Sub1 = item1Sub1.addAnswer();
    answer1Sub1.setValue(new StringType("Jane Doe"));
    final QuestionnaireResponseItemComponent item1Sub1Sub1 = answer1Sub1.addItem();
    item1Sub1Sub1.setLinkId("1.1.1");
    item1Sub1Sub1.setText("Name confirmed");
    item1Sub1Sub1.addAnswer().setValue(new BooleanType(true));

    // item "1.2" (Date of Birth).
    final QuestionnaireResponseItemComponent item1Sub2 = item1.addItem();
    item1Sub2.setLinkId("1.2");
    item1Sub2.setText("Date of Birth");
    item1Sub2.addAnswer().setValue(new DateType("1990-01-15"));

    // item "2" (Medical History).
    final QuestionnaireResponseItemComponent item2 = qr.addItem();
    item2.setLinkId("2");
    item2.setText("Medical History");

    // item "2.1" (Conditions).
    final QuestionnaireResponseItemComponent item2Sub1 = item2.addItem();
    item2Sub1.setLinkId("2.1");
    item2Sub1.setText("Conditions");

    // answer true with nested item "2.1.1".
    final QuestionnaireResponseItemAnswerComponent answer2Sub1 = item2Sub1.addAnswer();
    answer2Sub1.setValue(new BooleanType(true));
    final QuestionnaireResponseItemComponent item2Sub1Sub1 = answer2Sub1.addItem();
    item2Sub1Sub1.setLinkId("2.1.1");
    item2Sub1Sub1.setText("Condition details");
    item2Sub1Sub1.addAnswer().setValue(new StringType("Type 2 Diabetes"));

    return qr;
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatUnionTraversal() {
    return builder()
        .withResource(createQuestionnaireResponse())
        .group("repeat() with union expression on QuestionnaireResponse")
        .testEquals(
            List.of("1", "2", "1.1", "1.2", "2.1", "1.1.1", "2.1.1"),
            "repeat(item | answer.item).linkId",
            "repeat(item | answer.item).linkId collects linkIds from items nested through both"
                + " item and answer.item paths")
        .testEquals(
            7,
            "repeat(item | answer.item).count()",
            "repeat(item | answer.item).count() returns total items from all paths and levels")
        .testEquals(
            List.of(
                "Demographics",
                "Medical History",
                "Name",
                "Date of Birth",
                "Conditions",
                "Name confirmed",
                "Condition details"),
            "repeat(item | answer.item).text",
            "repeat(item | answer.item).text collects text from all items")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatChainedOperations() {
    return builder()
        .withResource(createQuestionnaire())
        .group("repeat() chained operations")
        .testEquals(
            List.of("1", "2", "1.1", "1.1.1"),
            "repeat(item).linkId",
            "repeat(item).linkId chains path navigation after repeat")
        .testEquals(
            List.of("2", "1.1.1"),
            "repeat(item).where(type = 'display').linkId",
            "repeat(item).where().linkId chains where and path after repeat")
        .build();
  }

  /**
   * Verifies that repeat($this) works correctly over Quantity literal collections produced by
   * combine(). This was broken because Quantity literal structs contained VOID-typed fields which
   * are incompatible with to_variant_object() used by variantTransformTree.
   *
   * @see <a href="https://github.com/aehrc/pathling/issues/2588">#2588</a>
   */
  @FhirPathTest
  public Stream<DynamicTest> testRepeatQuantityLiterals() {
    return builder()
        .withSubject(sb -> sb)
        .group("repeat($this) over combined Quantity literals (#2588)")
        .testEquals(
            2,
            "(1 year).combine(12 months).repeat($this).count()",
            "repeat($this) returns both indefinite calendar durations (non-comparable units)")
        .testEquals(
            1,
            "(3 'min').combine(180 seconds).repeat($this).count()",
            "repeat($this) deduplicates equivalent canonicalisable quantities (3 min = 180 s)")
        .build();
  }
}
