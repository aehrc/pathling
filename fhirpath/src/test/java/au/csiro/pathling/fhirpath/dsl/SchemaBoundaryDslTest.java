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
import java.util.Set;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemComponent;
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
 * Tests FHIRPath navigation beyond the parts of a resource that the encoder includes in its schema.
 *
 * <p>The encoder only encodes recursive elements, such as {@code Questionnaire.item}, to the
 * configured maximum nesting level. With a maximum nesting level of 3, four levels of {@code item}
 * are encoded, and a fifth level of {@code item} is absent from the schema even when the resource
 * contains it. Navigating to an element beyond that level must behave as if the element were empty,
 * so that every function sees an empty collection.
 *
 * <p>Choice types that are excluded from the encoded open types are a configuration matter rather
 * than a depth limit, and selecting them must still fail.
 *
 * @author John Grimes
 */
@Import(SchemaBoundaryDslTest.Config.class)
public class SchemaBoundaryDslTest extends FhirPathDslTestBase {

  private static final String EXTENSION_URL = "http://example.com/ext";

  private static final String UCUM = "http://unitsofmeasure.org";

  /**
   * Provides a FhirEncoders bean with a maximum nesting level of 3 and a restricted set of open
   * types, which excludes {@code integer}.
   */
  @TestConfiguration
  static class Config {

    @Bean
    @Nonnull
    FhirEncoders fhirEncoders() {
      return FhirEncoders.forR4()
          .withExtensionsEnabled(true)
          .withOpenTypes(Set.of("string"))
          .withMaxNestingLevel(3)
          .getOrCreate();
    }
  }

  /**
   * Creates a Questionnaire with items nested five levels deep, one level deeper than the encoder
   * includes, and an extension with a string value. Every item has a LOINC code and an initial
   * Quantity value.
   *
   * @return the Questionnaire
   */
  @Nonnull
  private static Questionnaire createQuestionnaire() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("deep-questionnaire");
    questionnaire.setStatus(PublicationStatus.ACTIVE);
    questionnaire.addExtension(new Extension(EXTENSION_URL, new StringType("ext-value")));

    QuestionnaireItemComponent parent = addItem(questionnaire.addItem(), "1");
    for (final String linkId : List.of("1.1", "1.1.1", "1.1.1.1", "1.1.1.1.1")) {
      parent = addItem(parent.addItem(), linkId);
    }
    return questionnaire;
  }

  /**
   * Populates a Questionnaire item with a link ID, a LOINC code and an initial Quantity value.
   *
   * @param item the item to populate
   * @param linkId the link ID of the item
   * @return the populated item
   */
  @Nonnull
  private static QuestionnaireItemComponent addItem(
      @Nonnull final QuestionnaireItemComponent item, @Nonnull final String linkId) {
    item.setLinkId(linkId);
    item.addCode(new Coding("http://loinc.org", "L5", null));
    item.addInitial()
        .setValue(new Quantity().setValue(20).setUnit("mg").setSystem(UCUM).setCode("mg"));
    return item;
  }

  @FhirPathTest
  public Stream<DynamicTest> testNavigationBeyondEncodedDepth() {
    return builder()
        .withResource(createQuestionnaire())
        .group("Control: the deepest encoded level")
        .testEquals(
            "1.1.1.1",
            "item.item.item.item.linkId",
            "The fourth level of item is encoded and returns its value")
        .testTrue(
            "item.item.item.item.code = http://loinc.org|L5",
            "Coding equality at the deepest encoded level")
        .testTrue(
            "item.item.item.item.initial.value.ofType(Quantity) > 5 'mg'",
            "Quantity comparison at the deepest encoded level")
        .testEquals(
            "Quantity",
            "item.item.item.item.initial.value.type().name",
            "type() at the deepest encoded level")
        .group("Navigation beyond the encoded depth")
        .testEmpty("item.item.item.item.item", "The fifth level of item is empty")
        .testEmpty(
            "item.item.item.item.item.linkId", "A primitive below the fifth level of item is empty")
        .testEmpty(
            "item.item.item.item.item.item.linkId",
            "Navigating several levels beyond the encoded depth is empty")
        .testEmpty(
            "item.item.item.item.item.linkId.first()", "first() of the empty collection is empty")
        .group("Functions on navigation beyond the encoded depth")
        .testFalse("item.item.item.item.item.exists()", "exists() is false")
        .testTrue("item.item.item.item.item.empty()", "empty() is true")
        .testEquals(0, "item.item.item.item.item.count()", "count() is zero")
        .testEmpty("item.item.item.item.item.linkId = 'x'", "Comparison with empty is empty")
        .testFalse(
            "item.item.item.item.item.extension('" + EXTENSION_URL + "').exists()",
            "An extension below the encoded depth does not exist")
        .testEmpty(
            "item.item.item.item.item.code = http://loinc.org|L5", "Coding equality is empty")
        .testFalse(
            "item.item.item.item.item.code.where($this = http://loinc.org|L5).exists()",
            "A Coding filter matches nothing")
        .testEquals(0, "item.item.item.item.item.code.distinct().count()", "distinct() is empty")
        .testTrue("item.item.item.item.item.code.isDistinct()", "isDistinct() is true")
        .testEmpty(
            "item.item.item.item.item.initial.value.ofType(Quantity) > 5 'mg'",
            "Quantity comparison is empty")
        .testEmpty(
            "item.item.item.item.item.initial.value.ofType(Quantity) = 20 'mg'",
            "Quantity equality is empty")
        .testEmpty(
            "item.item.item.item.item.initial.value.type().name", "type() of a choice is empty")
        .testEmpty(
            "item.item.item.item.item.repeat(item).linkId", "repeat() of the empty collection")
        .testEmpty(
            "item.item.item.item.item.repeatAll(item).linkId",
            "repeatAll() of the empty collection")
        .testEmpty("item.item.item.item.item.code.display()", "A terminology function is empty")
        .testEmpty(
            "item.item.item.item.item.code.memberOf('http://example.org/vs')",
            "memberOf() is empty")
        .group("Expressions that combine encoded and unencoded levels")
        .testEquals(
            "1.1.1.1",
            "item.item.item.item.select(linkId | item.linkId)",
            "A union with an empty operand keeps the other operand")
        .testEquals(
            "1.1.1.1",
            "(item.item.item.item | item.item.item.item.item).linkId",
            "A union of encoded and unencoded items keeps the encoded items")
        .testEquals(
            1,
            "(item.item.item.item.code | item.item.item.item.item.code).count()",
            "A union of encoded and unencoded Codings keeps the encoded Codings")
        .testEquals(
            "1.1.1.1",
            "item.item.item.item.where(item.empty()).linkId",
            "A filter on the absence of a deeper level matches the deepest encoded items")
        .testEquals(
            0,
            "item.item.item.select(item.item).count()",
            "select() of a projection beyond the encoded depth is empty")
        .testFalse(
            "item.item.item.select(item.item).exists()",
            "exists() of a projection beyond the encoded depth is false")
        .testEmpty(
            "item.item.item.item.select(item.linkId)",
            "select() of a primitive beyond the encoded depth is empty")
        .testEquals(
            0,
            "item.item.item.item.select(text).count()",
            "select() of an encoded but absent element is empty")
        .testEquals(
            List.of("1", "1.1", "1.1.1", "1.1.1.1"),
            "repeat(item).linkId",
            "repeat() stops at the deepest encoded level")
        .testEquals(
            List.of("1", "1.1", "1.1.1", "1.1.1.1"),
            "repeatAll(item).linkId",
            "repeatAll() stops at the deepest encoded level")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testRecursionThroughAnotherElement() {
    // QuestionnaireResponse items recurse through answers as well as directly. Five levels of
    // item are nested through answers, one level deeper than the encoder includes.
    final QuestionnaireResponse response = new QuestionnaireResponse();
    response.setId("deep-response");
    response.setStatus(QuestionnaireResponseStatus.COMPLETED);
    QuestionnaireResponseItemComponent item = response.addItem().setLinkId("1");
    for (final String linkId : List.of("1.1", "1.1.1", "1.1.1.1", "1.1.1.1.1")) {
      final QuestionnaireResponseItemAnswerComponent answer = item.addAnswer();
      answer.setValue(new Coding("http://example.org", linkId, null));
      item = answer.addItem().setLinkId(linkId);
    }
    final String deepestEncoded = "item.answer.item.answer.item.answer.item";
    return builder()
        .withResource(response)
        .group("Recursion through answers")
        .testEquals(
            "1.1.1.1",
            deepestEncoded + ".linkId",
            "The fourth level of item is encoded and returns its value")
        .testEmpty(
            deepestEncoded + ".answer.item.linkId",
            "The fifth level of item, nested through an answer, is empty")
        .testFalse(
            deepestEncoded + ".answer.item.exists()",
            "exists() is false for the fifth level of item")
        .testFalse(
            deepestEncoded
                + ".answer.item.answer.value.ofType(Coding).where($this = http://example.org|x)"
                + ".exists()",
            "A Coding filter below the fifth level of item matches nothing")
        .testEquals(
            List.of("1", "1.1", "1.1.1", "1.1.1.1"),
            "repeat(item | answer.item).linkId",
            "repeat() through answers stops at the deepest encoded level")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testChoiceTypesExcludedFromOpenTypes() {
    final Questionnaire questionnaire = createQuestionnaire();
    questionnaire.addExtension(new Extension(EXTENSION_URL + "/int", new IntegerType(42)));
    return builder()
        .withResource(questionnaire)
        .group("Open types")
        .testEquals(
            "ext-value",
            "extension('" + EXTENSION_URL + "').value.ofType(string)",
            "An extension value of an encoded open type is returned")
        .testError(
            "extension('" + EXTENSION_URL + "/int').value.ofType(integer)",
            "Selecting an open type that is not encoded fails")
        .build();
  }
}
