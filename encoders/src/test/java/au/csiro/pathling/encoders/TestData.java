/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.encoders;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hl7.fhir.r4.model.Annotation;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Condition.ConditionStageComponent;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Device;
import org.hl7.fhir.r4.model.Device.DevicePropertyComponent;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Medication;
import org.hl7.fhir.r4.model.Medication.MedicationIngredientComponent;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Narrative;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Provenance;
import org.hl7.fhir.r4.model.Provenance.ProvenanceEntityRole;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemComponent;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseItemAnswerComponent;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseItemComponent;
import org.hl7.fhir.r4.model.Range;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.codesystems.ConditionVerStatus;
import org.hl7.fhir.utilities.xhtml.NodeType;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;
import org.joda.time.DateTime;

/**
 * Helper class to create data for testing purposes.
 */
public class TestData {

  public static final Date TEST_DATE = DateTime.parse("2020-05-09T12:13Z").toDate();

  public static final java.math.BigDecimal TEST_SMALL_DECIMAL = new java.math.BigDecimal("123.45");
  public static final java.math.BigDecimal TEST_VERY_BIG_DECIMAL =
      new java.math.BigDecimal("1234560123456789.123456");
  public static final java.math.BigDecimal TEST_VERY_SMALL_DECIMAL = new java.math.BigDecimal(
      "0.1234567");
  public static final java.math.BigDecimal TEST_VERY_SMALL_DECIMAL_SCALE_6 = new java.math.BigDecimal(
      "0.123457");

  /**
   * Returns a FHIR Condition for testing purposes.
   */
  public static Condition newCondition() {

    final Condition condition = new Condition();

    // Condition based on example from FHIR:
    // https://www.hl7.org/fhir/condition-example.json.html
    condition.setId("example");

    condition.setLanguage("en_US");

    // Narrative text
    final Narrative narrative = new Narrative();
    narrative.setStatusAsString("generated");
    narrative.setDivAsString("This data was generated for test purposes.");
    final XhtmlNode node = new XhtmlNode();
    node.setNodeType(NodeType.Text);
    node.setValue("Severe burn of left ear (Date: 24-May 2012)");
    condition.setText(narrative);

    condition.setSubject(new Reference("Patient/example").setDisplay("Here is a display for you."));

    final CodeableConcept verificationStatus = new CodeableConcept();
    verificationStatus.addCoding(new Coding(ConditionVerStatus.CONFIRMED.getSystem(),
        ConditionVerStatus.CONFIRMED.toCode(),
        ConditionVerStatus.CONFIRMED.getDisplay()));
    condition.setVerificationStatus(verificationStatus);

    // Condition code
    final CodeableConcept code = new CodeableConcept();
    code.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("39065001")
        .setDisplay("Severe");
    condition.setSeverity(code);

    // Severity code
    final CodeableConcept severity = new CodeableConcept();
    severity.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("24484000")
        .setDisplay("Burn of ear")
        .setUserSelected(true);
    condition.setSeverity(severity);

    // Onset date time
    final DateTimeType onset = new DateTimeType();
    onset.setValueAsString("2012-05-24");
    condition.setOnset(onset);

    return condition;
  }

  public static Condition conditionWithReferencesWithIdentifiers() {
    final Condition condition = new Condition();
    condition.setId("withReferencesWithIdentifiers");
    final Coding typeCoding = new Coding("http://terminology.hl7.org/CodeSystem/v2-0203", "MR",
        null);
    final CodeableConcept typeConcept = new CodeableConcept(typeCoding);
    condition.setSubject(
        new Reference("Patient/example")
            .setDisplay("Display name")
            .setIdentifier(
                new Identifier()
                    .setType(typeConcept)
                    .setSystem("https://fhir.example.com/identifiers/mrn")
                    .setValue("urn:id")
                    .setAssigner(new Reference("Organization/001"))
            )
    );
    return condition;
  }

  public static Condition conditionWithIdentifiersWithReferences() {
    final Condition condition = new Condition();
    condition.setId("withIdentifiersWithReferences");
    final Coding typeCoding = new Coding("http://terminology.hl7.org/CodeSystem/v2-0203", "MR",
        null);
    final CodeableConcept typeConcept = new CodeableConcept(typeCoding);
    condition
        .addIdentifier()
        .setType(typeConcept)
        .setSystem("https://fhir.example.com/identifiers/mrn")
        .setValue("urn:id01")
        .setAssigner(new Reference("Organization/001")
            .setIdentifier(new Identifier().setValue("urn:id02")
                .setAssigner(new Reference("Organization/002"))));
    return condition;
  }


  public static Condition conditionWithVersion() {
    final Condition condition = new Condition();
    final IdType id = new IdType("Condition", "with-version", "1");
    condition.setIdElement(id);
    return condition;
  }


  /**
   * Returns a FHIR Observation for testing purposes.
   */
  public static Observation newObservation() {

    // Observation based on https://www.hl7.org/FHIR/observation-example-bloodpressure.json.html
    final Observation observation = new Observation();

    observation.setId("blood-pressure");

    final Identifier identifier = observation.addIdentifier();
    identifier.setSystem("urn:ietf:rfc:3986");
    identifier.setValue("urn:uuid:187e0c12-8dd2-67e2-99b2-bf273c878281");

    observation.setStatus(Observation.ObservationStatus.FINAL);

    final Quantity quantity = new Quantity();
    quantity.setValue(TEST_SMALL_DECIMAL);
    quantity.setUnit("mm[Hg]");
    quantity.setComparator(QuantityComparator.LESS_THAN);
    observation.setValue(quantity);
    observation.setIssued(TEST_DATE);

    observation.addReferenceRange().getLow().setValue(TEST_VERY_SMALL_DECIMAL);
    observation.getReferenceRange().get(0).getHigh().setValue(TEST_VERY_BIG_DECIMAL);

    return observation;
  }

  public static Observation newUcumObservation() {
    final Observation observation = new Observation();
    observation.setId("weight");

    final Quantity quantity = new Quantity();
    quantity.setValue(76);
    quantity.setUnit("kg");
    quantity.setSystem("http://unitsofmeasure.org");
    quantity.setCode("kg");

    observation.setValue(quantity);

    return observation;
  }

  public static Device newDevice() {
    final Device device = new Device();
    device.setId("some-device");

    final DevicePropertyComponent property = new DevicePropertyComponent();
    property.setType(
        new CodeableConcept(new Coding("urn:example:abc", "12345", "Some property")));
    final Quantity quantity1 = new Quantity(null, 1.0, "http://unitsofmeasure.org", "mm", "mm");
    final Quantity quantity2 = new Quantity(null, 2.0, "http://unitsofmeasure.org", "mm", "mm");
    final List<Quantity> quantities = List.of(quantity1, quantity2);
    property.setValueQuantity(quantities);
    final List<DevicePropertyComponent> properties = List.of(property);
    device.setProperty(properties);

    return device;
  }

  /**
   * Returns a FHIR Patient for testing purposes.
   */
  public static Patient newPatient() {

    final Patient patient = new Patient();

    patient.setId("test-patient");
    patient.setMultipleBirth(new IntegerType(1));

    return patient;
  }

  /**
   * Returns a FHIR medication to be contained to a medication request for testing purposes.
   */
  public static Medication newMedication() {

    final Medication medication = new Medication();

    medication.setId("test-med");

    final MedicationIngredientComponent ingredient = new MedicationIngredientComponent();

    final CodeableConcept item = new CodeableConcept();
    item.addCoding()
        .setSystem("test/ingredient/system")
        .setCode("test-code");

    ingredient.setItem(item);

    medication.addIngredient(ingredient);

    return medication;
  }

  /**
   * Returns a FHIR Provenance to be contained to a medication request for testing purposes.
   */
  public static Provenance newProvenance() {

    final Provenance provenance = new Provenance();

    provenance.setId("test-provenance");

    provenance.setTarget(ImmutableList.of(new Reference("test-target")));

    provenance.getEntityFirstRep()
        .setRole(ProvenanceEntityRole.SOURCE)
        .setWhat(new Reference("test-entity"));

    return provenance;
  }

  /**
   * Returns a FHIR medication request for testing purposes.
   */
  public static MedicationRequest newMedRequest() {

    final MedicationRequest medReq = new MedicationRequest();

    medReq.setId("test-med");

    // Medication code
    final CodeableConcept med = new CodeableConcept();
    med.addCoding()
        .setSystem("http://www.nlm.nih.gov/research/umls/rxnorm")
        .setCode("582620")
        .setDisplay("Nizatidine 15 MG/ML Oral Solution [Axid]");

    med.setText("Nizatidine 15 MG/ML Oral Solution [Axid]");

    medReq.setMedication(med);

    final Annotation annotation = new Annotation();

    annotation.setText("Test medication note.");

    annotation.setAuthor(
        new Reference("Provider/example")
            .setDisplay("Example provider."));

    medReq.addNote(annotation);

    // Add contained resources
    medReq.addContained(newMedication());
    medReq.addContained(newProvenance());

    return medReq;
  }

  /**
   * Returns a FHIR Coverage resource for testing purposes.
   */
  public static Encounter newEncounter() {
    final Encounter encounter = new Encounter();

    final Coding classCoding = new Coding();
    classCoding.setSystem("http://terminology.hl7.org/CodeSystem/v3-ActCode");
    classCoding.setCode("AMB");
    encounter.setClass_(classCoding);

    return encounter;
  }


  /**
   * Returns a FHIR Questionnaire resource for testing purposes.
   */
  public static Questionnaire newQuestionnaire() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("Questionnaire/1");
    final QuestionnaireItemComponent item = questionnaire.addItem();
    item.addEnableWhen()
        .setAnswer(new DecimalType(TEST_VERY_SMALL_DECIMAL_SCALE_6));
    item.addInitial()
        .setValue(new DecimalType(TEST_VERY_BIG_DECIMAL));
    // This nested item will be discarded on import, as we currently skip recursive elements.
    final QuestionnaireItemComponent nestedItem = item.addItem();
    nestedItem.addInitial()
        .setValue(new DecimalType(TEST_SMALL_DECIMAL));
    return questionnaire;
  }

  /**
   * Returns a FHIR QuestionnaireResponse resource for testing purposes.
   */
  public static QuestionnaireResponse newQuestionnaireResponse() {
    final QuestionnaireResponse questionnaireResponse = new QuestionnaireResponse();
    questionnaireResponse.setId("QuestionnaireResponse/1");
    final QuestionnaireResponseItemComponent item = questionnaireResponse.addItem();

    final QuestionnaireResponseItemAnswerComponent answer1 =
        new QuestionnaireResponseItemAnswerComponent();
    answer1.setValue(new DecimalType(TEST_VERY_SMALL_DECIMAL_SCALE_6));
    final QuestionnaireResponseItemAnswerComponent answer2 =
        new QuestionnaireResponseItemAnswerComponent();
    answer2.setValue(new DecimalType(TEST_VERY_BIG_DECIMAL));

    item.addAnswer(answer1);
    item.addAnswer(answer2);
    return questionnaireResponse;
  }


  private static List<QuestionnaireItemComponent> newNestedItems(final int nestingLevel,
      final int noChildren, final String parentId) {

    return IntStream.range(0, noChildren)
        .mapToObj(i -> {
          final QuestionnaireItemComponent item = new QuestionnaireItemComponent();
          final String thisItemId = parentId + i;
          item.setLinkId("Item/" + thisItemId);
          if (nestingLevel > 0) {
            item.setItem(newNestedItems(nestingLevel - 1, noChildren, thisItemId + "."));
          }
          return item;
        })
        .collect(
            Collectors.toList());
  }

  /**
   * Returns a FHIR Questionnaire resource with nested Item elements for testing purposes.
   *
   * @param maxNestingLevel the number of nested levels. Zero indicates the the Item element is
   * present in the Questionnaire but with no nested items.
   * @param noChildren the number of Item elements at each nesting level.
   */
  public static Questionnaire newNestedQuestionnaire(final int maxNestingLevel,
      final int noChildren) {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("Questionnaire/1");
    questionnaire.setItem(newNestedItems(maxNestingLevel, noChildren, ""));
    return questionnaire;
  }

  /**
   * Returns a FHIR Questionnaire resource with nested Item elements for testing purposes.
   *
   * @param maxNestingLevel the number of nested levels. Zero indicates the the Item element is
   * present in the Questionnaire but with no nested items.
   */
  public static Questionnaire newNestedQuestionnaire(final int maxNestingLevel) {
    return newNestedQuestionnaire(maxNestingLevel, 1);
  }


  private static QuestionnaireResponseItemAnswerComponent newNestedResponseAnswer(
      final int nestingLevel) {
    final QuestionnaireResponseItemAnswerComponent answer = new QuestionnaireResponseItemAnswerComponent();
    answer.setId("AnswerLevel/" + nestingLevel);
    answer.setItem(Collections.singletonList(newNestedResponseItem(nestingLevel - 1)));
    return answer;
  }


  private static QuestionnaireResponseItemComponent newNestedResponseItem(final int nestingLevel) {
    final QuestionnaireResponseItemComponent item = new QuestionnaireResponseItemComponent();
    item.setLinkId("ItemLevel/" + nestingLevel);
    if (nestingLevel > 0) {
      item.setAnswer(Collections.singletonList(newNestedResponseAnswer(nestingLevel)));
      item.setItem(Collections.singletonList(newNestedResponseItem(nestingLevel - 1)));
    }
    return item;
  }

  /**
   * Returns a FHIR Questionnaire resource with nested Item elements for testing purposes.
   *
   * @param maxNestingLevel the number of nested levels. Zero indicates the the Item element is
   * present in the Questionnaire but with no nested items.
   */
  public static QuestionnaireResponse newNestedQuestionnaireResponse(final int maxNestingLevel) {
    final QuestionnaireResponse questionnaireResponse = new QuestionnaireResponse();
    questionnaireResponse.setId("QuestionnaireResponse/1");
    questionnaireResponse
        .setItem(Collections.singletonList(newNestedResponseItem(maxNestingLevel)));
    return questionnaireResponse;
  }


  public static Condition newConditionWithExtensions() {

    // Condition
    // - extension:
    //   - url: uuid:ext1
    //   - value: ext1
    // - extension:
    //    - url: uuid:ext2
    //    - value: 2
    // - extension:
    //  - url: uuid:ext4
    //  - extension
    //    - url: uuid:nested
    //    - value: nested
    // - identifier
    //     - id: uuid:identifier1
    //     - extension:
    //       - url: uuid:ext10
    //       - value: ext10
    //     - extension:
    //        - url: uuid:ext11
    //        - value: 11
    // - stage
    //     - type
    //       - extension
    //          - url: uuid:ext12
    //          - value: ext12

    final Condition conditionWithExtension = new Condition();
    conditionWithExtension.setId("someId");

    final Extension nestedExtension = new Extension("uuid:ext4");
    nestedExtension.addExtension(new Extension("uuid:nested", new StringType("nested")));

    conditionWithExtension.setExtension(Arrays.asList(
            new Extension("uuid:ext1", new StringType("ext1")),
            new Extension("uuid:ext2", new IntegerType(2)),
            nestedExtension
        )
    );

    conditionWithExtension.setMeta(new Meta().setVersionId("MetVersion"));

    conditionWithExtension.setOnset(new Range());
    conditionWithExtension
        .setSeverity(new CodeableConcept().addCoding(new Coding("sys", "code", "name")));

    final Identifier identifier = conditionWithExtension.getIdentifierFirstRep();
    identifier.setId("uuid:identifier1");

    identifier.setExtension(Arrays.asList(
            new Extension("uuid:ext10", new StringType("ext10")),
            new Extension("uuid:ext11", new IntegerType(11))
        )
    );

    final ConditionStageComponent stage = conditionWithExtension.getStageFirstRep();
    stage.getType().setExtension(
        Collections.singletonList(new Extension("uuid:ext12", new StringType("ext12")))
    );

    return conditionWithExtension;
  }

}
