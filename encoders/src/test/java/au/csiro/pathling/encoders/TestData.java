/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Date;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Medication.MedicationIngredientComponent;
import org.hl7.fhir.r4.model.Provenance.ProvenanceEntityRole;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemComponent;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseItemAnswerComponent;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseItemComponent;
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
    observation.setValue(quantity);
    observation.setIssued(TEST_DATE);

    observation.addReferenceRange().getLow().setValue(TEST_VERY_SMALL_DECIMAL);
    observation.getReferenceRange().get(0).getHigh().setValue(TEST_VERY_BIG_DECIMAL);

    return observation;
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


  private static QuestionnaireItemComponent newNestedItem(final int nestingLevel) {
    final QuestionnaireItemComponent item = new QuestionnaireItemComponent();
    item.setLinkId("ItemLevel/" + nestingLevel);
    if (nestingLevel > 0) {
      item.setItem(Collections.singletonList(newNestedItem(nestingLevel - 1)));
    }
    return item;
  }

  /**
   * Returns a FHIR Questionnaire resource with nested Item elements for testing purposes.
   *
   * @param maxNestingLevel the number of nested levels. Zero indicates the the Item element is
   * present in the Questionnaire but with no nested items.
   */
  public static Questionnaire newNestedQuestionnaire(final int maxNestingLevel) {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("Questionnaire/1");
    questionnaire.setItem(Collections.singletonList(newNestedItem(maxNestingLevel)));
    return questionnaire;
  }

}
