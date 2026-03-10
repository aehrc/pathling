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
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
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
 * Tests for the FHIRPath repeatAll() function.
 *
 * <p>Uses a Questionnaire resource with nested items to verify recursive traversal behaviour. With
 * the default maxNestingLevel of 3, the schema supports items nested 4 levels deep (root item plus
 * 3 additional levels).
 */
@Import(RepeatAllFunctionDslTest.Config.class)
public class RepeatAllFunctionDslTest extends FhirPathDslTestBase {

  /**
   * Provides a FhirEncoders bean with maxNestingLevel=3 to support deeply nested items in test
   * Questionnaire resources. Overrides the default bean which uses maxNestingLevel=0.
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
   * Creates a test Questionnaire with items nested to the specified depth.
   *
   * <p>Structure:
   *
   * <pre>
   *   item "1" (group)
   *     item "1.1" (group)
   *       item "1.1.1" (display)
   *   item "2" (display)
   * </pre>
   *
   * @return a Questionnaire resource
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

  /**
   * Creates a Questionnaire with no items (for empty input testing).
   *
   * @return a Questionnaire resource with no items
   */
  private static Questionnaire createEmptyQuestionnaire() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("empty-questionnaire");
    questionnaire.setStatus(PublicationStatus.DRAFT);
    return questionnaire;
  }

  /**
   * Creates a Patient with all fields needed by the patient projection tests: two names, gender,
   * marital status, and nested extensions.
   *
   * <p>Extension structure:
   *
   * <pre>
   *   Patient
   *     name: Smith, Jones
   *     gender: MALE
   *     maritalStatus: M (Married)
   *     extension "http://example.com/simple" = "simple-value"
   *     extension "http://example.com/parent"
   *       extension "http://example.com/child" = "child-value"
   * </pre>
   *
   * @return a Patient resource with all fields populated
   */
  private static Patient createPatient() {
    final Patient patient = new Patient();
    patient.setId("test-patient");
    patient.addName().setFamily("Smith");
    patient.addName().setFamily("Jones");
    patient.setGender(AdministrativeGender.MALE);
    patient.setMaritalStatus(
        new CodeableConcept(new Coding("http://example.com/cs", "M", "Married")));

    // Simple leaf extension.
    patient.addExtension(
        new Extension("http://example.com/simple", new StringType("simple-value")));

    // Complex extension containing a nested sub-extension.
    final Extension parent = new Extension("http://example.com/parent");
    parent.addExtension(new Extension("http://example.com/child", new StringType("child-value")));
    patient.addExtension(parent);

    return patient;
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllQuestionnaireTraversal() {
    return builder()
        .withResource(createQuestionnaire())
        .group("repeatAll() basic traversal")
        .testEquals(
            List.of("1", "2", "1.1", "1.1.1"),
            "repeatAll(item).linkId",
            "repeatAll(item).linkId returns linkIds from all nesting levels")
        .testEquals(
            4,
            "repeatAll(item).count()",
            "repeatAll(item).count() returns total items across all levels")
        .group("repeatAll() with filtering")
        .testEquals(
            List.of("1", "1.1"),
            "repeatAll(item).where(type = 'group').linkId",
            "repeatAll(item).where(type = 'group') filters items from all nesting levels")
        .group("repeatAll() computed empty input")
        .testEmpty(
            "item.where(linkId = 'nonexistent').repeatAll(item)",
            "repeatAll() on a computed empty collection returns empty")
        .group("repeatAll() $index not defined")
        .testError(
            "repeatAll($index)", "$index is not available within repeatAll projection expression")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllNonRecursiveProjections() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                        "name",
                        n1 -> n1.string("family", "Smith"),
                        n2 -> n2.string("family", "Jones"))
                    .string("gender", "male")
                    .element(
                        "maritalStatus",
                        ms -> ms.elementArray("coding", c -> c.string("code", "M"))))
        .group("repeatAll() non-recursive projection")
        .testEquals(
            List.of("Smith", "Jones"),
            "repeatAll(name).family",
            "repeatAll(name).family returns same result as select() for non-recursive fields")
        .group("repeatAll() singular primitive projection")
        .testEquals(
            "male",
            "repeatAll(gender)",
            "repeatAll(gender) returns a singular primitive value like select()")
        .group("repeatAll() singular complex projection")
        .testEquals(
            "M",
            "repeatAll(maritalStatus).coding.code",
            "repeatAll(maritalStatus) returns a singular complex value with sub-elements intact")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllInfiniteRecursionAndExtensions() {
    return builder()
        .withResource(createPatient())
        .group("repeatAll() primitive self-referential detection")
        .testError(
            "self-referential type that cannot terminate",
            "gender.repeatAll($this)",
            "repeatAll($this) on a primitive raises a static self-referential error")
        .testError(
            "self-referential type that cannot terminate",
            "gender.repeatAll('someValue')",
            "repeatAll('literal') on a primitive raises a static self-referential error")
        .testError(
            "self-referential type that cannot terminate",
            "gender.repeatAll(length())",
            "repeatAll(length()) on a primitive raises a static self-referential error")
        .group("repeatAll() complex infinite recursion detection")
        .testError(
            "Recursive traversal exceeded maximum depth",
            "name.repeatAll(first()).count()",
            "repeatAll(first()) raises an error for non-Extension same-type depth exhaustion")
        .testError(
            "self-referential type that cannot terminate",
            "repeatAll($this)",
            "repeatAll($this) on a resource raises a static self-referential error")
        .group("repeatAll() extension traversal")
        .testEquals(
            List.of(
                "http://example.com/simple",
                "http://example.com/parent",
                "http://example.com/child"),
            "repeatAll(extension).url",
            "repeatAll(extension) recursively collects all extensions including nested"
                + " sub-extensions")
        .group("repeatAll() choice type via extension value")
        .testEquals(
            List.of("simple-value", "child-value"),
            "repeatAll(extension).value.ofType(string)",
            "repeatAll(extension).value.ofType(string) extracts choice-typed values from all"
                + " extensions")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllEmptyInput() {
    return builder()
        .withResource(createEmptyQuestionnaire())
        .group("repeatAll() empty input")
        .testEmpty(
            "repeatAll(item)", "repeatAll() on a resource with no items returns empty collection")
        .testEquals(
            0, "repeatAll(item).count()", "repeatAll(item).count() returns 0 for empty input")
        .group("repeatAll() empty literal")
        .testEmpty(
            "{}.repeatAll($this)",
            "repeatAll() on the FHIRPath empty literal returns empty collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllSingleLevel() {
    // Create a Questionnaire with only top-level items (no nesting).
    final Questionnaire flat = new Questionnaire();
    flat.setId("flat-questionnaire");
    flat.setStatus(PublicationStatus.DRAFT);
    flat.addItem().setLinkId("a").setType(QuestionnaireItemType.DISPLAY);
    flat.addItem().setLinkId("b").setType(QuestionnaireItemType.DISPLAY);
    flat.addItem().setLinkId("c").setType(QuestionnaireItemType.DISPLAY);

    return builder()
        .withResource(flat)
        .group("repeatAll() single-level traversal")
        .testEquals(
            List.of("a", "b", "c"),
            "repeatAll(item).linkId",
            "repeatAll(item).linkId on flat items returns all top-level linkIds")
        .build();
  }

  /**
   * Creates a Questionnaire where items at different nesting levels share the same linkId, to
   * verify that repeatAll preserves duplicates.
   *
   * <p>Structure:
   *
   * <pre>
   *   item "A" (group)
   *     item "B" (display)
   *   item "B" (display)
   * </pre>
   *
   * @return a Questionnaire resource with duplicate linkIds
   */
  private static Questionnaire createQuestionnaireWithDuplicates() {
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

    return questionnaire;
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllDuplicatesPreserved() {
    return builder()
        .withResource(createQuestionnaireWithDuplicates())
        .group("repeatAll() duplicate preservation")
        .testEquals(
            List.of("A", "B", "B"),
            "repeatAll(item).linkId",
            "repeatAll(item).linkId preserves duplicate linkId values from different nesting"
                + " levels")
        .testEquals(
            3,
            "repeatAll(item).count()",
            "repeatAll(item).count() includes duplicate items in the total")
        .build();
  }

  /**
   * Creates a QuestionnaireResponse with items nested through answer elements, to verify that
   * repeatAll can traverse through intermediate types (Item -> Answer -> Item).
   *
   * <p>Structure:
   *
   * <pre>
   *   item "root"
   *     answer
   *       item "nested-1"
   *         answer
   *           item "nested-2"
   * </pre>
   *
   * @return a QuestionnaireResponse resource with cross-type nesting
   */
  private static QuestionnaireResponse createQuestionnaireResponse() {
    final QuestionnaireResponse qr = new QuestionnaireResponse();
    qr.setId("cross-type-qr");
    qr.setStatus(QuestionnaireResponseStatus.COMPLETED);

    final QuestionnaireResponseItemComponent rootItem = qr.addItem();
    rootItem.setLinkId("root");

    final QuestionnaireResponseItemAnswerComponent answer1 = rootItem.addAnswer();
    final QuestionnaireResponseItemComponent nestedItem = answer1.addItem();
    nestedItem.setLinkId("nested-1");

    final QuestionnaireResponseItemAnswerComponent answer2 = nestedItem.addAnswer();
    final QuestionnaireResponseItemComponent deepItem = answer2.addItem();
    deepItem.setLinkId("nested-2");

    return qr;
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllCrossTypeTraversal() {
    return builder()
        .withResource(createQuestionnaireResponse())
        .group("repeatAll() cross-type traversal")
        .testEquals(
            List.of("nested-1", "nested-2"),
            "item.repeatAll(answer.item).linkId",
            "repeatAll(answer.item) traverses through answer elements to reach nested items")
        .build();
  }

  /**
   * Creates a Questionnaire where items at different nesting levels carry extensions, to verify
   * that repeatAll preserves extensions through the Variant round-trip.
   *
   * <p>Structure:
   *
   * <pre>
   *   item "1" (group, extension "ext-1")
   *     item "1.1" (display, extension "ext-1.1")
   *   item "2" (display, extension "ext-2")
   * </pre>
   *
   * @return a Questionnaire resource with extensions on items
   */
  private static Questionnaire createQuestionnaireWithExtensions() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("ext-questionnaire");
    questionnaire.setStatus(PublicationStatus.DRAFT);

    final QuestionnaireItemComponent item1 = questionnaire.addItem();
    item1.setLinkId("1");
    item1.setType(QuestionnaireItemType.GROUP);
    item1.addExtension(new Extension("http://example.com/ext", new StringType("ext-1")));

    final QuestionnaireItemComponent item1Sub1 = item1.addItem();
    item1Sub1.setLinkId("1.1");
    item1Sub1.setType(QuestionnaireItemType.DISPLAY);
    item1Sub1.addExtension(new Extension("http://example.com/ext", new StringType("ext-1.1")));

    final QuestionnaireItemComponent item2 = questionnaire.addItem();
    item2.setLinkId("2");
    item2.setType(QuestionnaireItemType.DISPLAY);
    item2.addExtension(new Extension("http://example.com/ext", new StringType("ext-2")));

    return questionnaire;
  }

  /**
   * Creates an Observation with a populated value[x] choice element (Quantity), for testing choice
   * type interactions with repeatAll().
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
  public Stream<DynamicTest> testRepeatAllChoiceTypeExpressions() {
    return builder()
        .withResource(createObservation())
        .group("repeatAll() choice type — indeterminate type guard")
        .testError(
            "indeterminate FHIR type",
            "value.repeatAll($this)",
            "repeatAll($this) on a choice type raises an indeterminate type error")
        .group("repeatAll() choice type — ofType on mixed collection")
        .testError(
            "Must have a fhirType or a definition",
            "value.repeatAll(ofType(Quantity)).count()",
            "repeatAll(ofType(Quantity)) on a choice type fails during type probing")
        .group("repeatAll() choice type — polymorphic traversal")
        .testError(
            "polymorphic",
            "repeatAll(value)",
            "repeatAll(value) on Observation fails on MixedCollection traversal")
        .group("repeatAll() choice type — first() on mixed collection")
        .testError(
            "Must have a fhirType or a definition",
            "value.repeatAll(first())",
            "repeatAll(first()) on a choice type fails during type probing")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllResourceLevelDegenerate() {
    return builder()
        .withResource(createPatient())
        .group("repeatAll() resource-level degenerate expressions")
        .testError(
            "self-referential type that cannot terminate",
            "name.repeatAll(%resource).gender",
            "repeatAll(%resource) on a resource raises a static self-referential error")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRepeatAllExtensionsPreserved() {
    return builder()
        .withResource(createQuestionnaireWithExtensions())
        .group("repeatAll() extension preservation")
        .testEquals(
            List.of("ext-1", "ext-2", "ext-1.1"),
            "repeatAll(item).extension('http://example.com/ext').value.ofType(string)",
            "repeatAll(item) preserves extensions from all nesting levels through Variant"
                + " round-trip")
        .build();
  }
}
