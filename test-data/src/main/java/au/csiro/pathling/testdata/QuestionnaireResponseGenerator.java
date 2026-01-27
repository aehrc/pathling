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

package au.csiro.pathling.testdata;

import ca.uhn.fhir.context.FhirContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseItemComponent;
import org.hl7.fhir.r4.model.QuestionnaireResponse.QuestionnaireResponseStatus;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;

/**
 * Generates synthetic FHIR QuestionnaireResponse resources with nested items and answers.
 *
 * <p>This generator creates realistic QuestionnaireResponse resources featuring multiple levels of
 * nested items and various answer types including strings, integers, booleans, dates, codings, and
 * more.
 *
 * @author John Grimes
 */
public class QuestionnaireResponseGenerator {

  private static final String OUTPUT_FILE = "questionnaireresponses.ndjson";
  private static final int NUM_RESOURCES = 10000;
  private static final Random random = new Random(12345);

  private static final String[] QUESTIONNAIRE_IDS = {
    "patient-intake",
    "mental-health-screen",
    "medication-review",
    "symptom-assessment",
    "quality-of-life"
  };

  private static final String[] SYMPTOM_CODES = {
    "386661006", // Fever
    "49727002", // Cough
    "267036007", // Dyspnoea
    "25064002", // Headache
    "22253000" // Pain
  };

  private static final String[] SYMPTOM_DISPLAYS = {
    "Fever", "Cough", "Dyspnoea", "Headache", "Pain"
  };

  private static final String[] SEVERITY_CODES = {
    "255604002", // Mild
    "6736007", // Moderate
    "24484000" // Severe
  };

  private static final String[] SEVERITY_DISPLAYS = {"Mild", "Midgrade", "Severe"};

  /**
   * Main entry point for the generator.
   *
   * @param args command line arguments (optional: output file path, number of resources)
   * @throws IOException if there's an error writing to the output file
   */
  public static void main(final String[] args) throws IOException {
    final String outputFile = args.length > 0 ? args[0] : OUTPUT_FILE;
    final int numResources = args.length > 1 ? Integer.parseInt(args[1]) : NUM_RESOURCES;

    System.out.println("Generating " + numResources + " QuestionnaireResponse resources...");

    // Create parent directories if they don't exist.
    final File outputFileObj = new File(outputFile);
    final File parentDir = outputFileObj.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new IOException("Failed to create directory: " + parentDir.getAbsolutePath());
      }
    }

    final FhirContext fhirContext = FhirContext.forR4();
    final String baseUrl = "https://example.org/fhir";

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      for (int i = 0; i < numResources; i++) {
        final QuestionnaireResponse response = generateQuestionnaireResponse(i, baseUrl);
        final String json =
            fhirContext.newJsonParser().setPrettyPrint(false).encodeResourceToString(response);
        writer.write(json);
        writer.newLine();

        if ((i + 1) % 1000 == 0) {
          System.out.println("Generated " + (i + 1) + " resources...");
        }
      }
    }

    System.out.println("Successfully generated " + numResources + " resources to " + outputFile);
  }

  /**
   * Generates a single QuestionnaireResponse resource with nested items.
   *
   * @param index the index of the resource being generated
   * @param baseUrl the base URL for resource references
   * @return a populated QuestionnaireResponse resource
   */
  private static QuestionnaireResponse generateQuestionnaireResponse(
      final int index, final String baseUrl) {
    final QuestionnaireResponse response = new QuestionnaireResponse();
    response.setId(UUID.randomUUID().toString());
    response.setStatus(QuestionnaireResponseStatus.COMPLETED);

    // Set questionnaire reference (using canonical URL format).
    final String questionnaireId = QUESTIONNAIRE_IDS[random.nextInt(QUESTIONNAIRE_IDS.length)];
    response.setQuestionnaire("https://pathling.csiro.au/fhir/Questionnaire/" + questionnaireId);

    // Set subject (patient) reference.
    final int patientId = random.nextInt(1000) + 1;
    response.setSubject(new Reference(baseUrl + "/Patient/patient-" + patientId));

    // Set authored date.
    final LocalDate baseDate = LocalDate.of(2024, 1, 1);
    final LocalDate authoredDate = baseDate.plusDays(random.nextInt(365));
    response.setAuthored(Date.from(authoredDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));

    // Add nested items based on questionnaire type.
    switch (questionnaireId) {
      case "patient-intake":
        addPatientIntakeItems(response);
        break;
      case "mental-health-screen":
        addMentalHealthScreenItems(response);
        break;
      case "medication-review":
        addMedicationReviewItems(response);
        break;
      case "symptom-assessment":
        addSymptomAssessmentItems(response);
        break;
      case "quality-of-life":
        addQualityOfLifeItems(response);
        break;
      default:
        break;
    }

    return response;
  }

  /**
   * Adds patient intake questionnaire items.
   *
   * @param response the QuestionnaireResponse to add items to
   */
  private static void addPatientIntakeItems(final QuestionnaireResponse response) {
    // Demographics section.
    final QuestionnaireResponseItemComponent demographics =
        new QuestionnaireResponseItemComponent();
    demographics.setLinkId("demographics");
    demographics.setText("Demographics");

    // Name.
    final QuestionnaireResponseItemComponent name = new QuestionnaireResponseItemComponent();
    name.setLinkId("demographics.name");
    name.setText("Full name");
    name.addAnswer().setValue(new StringType(generateRandomName()));
    demographics.addItem(name);

    // Age.
    final QuestionnaireResponseItemComponent age = new QuestionnaireResponseItemComponent();
    age.setLinkId("demographics.age");
    age.setText("Age");
    age.addAnswer().setValue(new IntegerType(random.nextInt(80) + 18));
    demographics.addItem(age);

    // Gender.
    final QuestionnaireResponseItemComponent gender = new QuestionnaireResponseItemComponent();
    gender.setLinkId("demographics.gender");
    gender.setText("Gender");
    final Coding genderCoding = new Coding();
    genderCoding.setSystem("http://hl7.org/fhir/administrative-gender");
    genderCoding.setCode(random.nextBoolean() ? "male" : "female");
    genderCoding.setDisplay(
        genderCoding.getCode().substring(0, 1).toUpperCase() + genderCoding.getCode().substring(1));
    gender.addAnswer().setValue(genderCoding);
    demographics.addItem(gender);

    response.addItem(demographics);

    // Contact section with nested address.
    final QuestionnaireResponseItemComponent contact = new QuestionnaireResponseItemComponent();
    contact.setLinkId("contact");
    contact.setText("Contact information");

    final QuestionnaireResponseItemComponent address = new QuestionnaireResponseItemComponent();
    address.setLinkId("contact.address");
    address.setText("Address");

    final QuestionnaireResponseItemComponent street = new QuestionnaireResponseItemComponent();
    street.setLinkId("contact.address.street");
    street.setText("Street");
    street.addAnswer().setValue(new StringType(random.nextInt(999) + 1 + " Main Street"));
    address.addItem(street);

    final QuestionnaireResponseItemComponent city = new QuestionnaireResponseItemComponent();
    city.setLinkId("contact.address.city");
    city.setText("City");
    city.addAnswer().setValue(new StringType(generateRandomCity()));
    address.addItem(city);

    final QuestionnaireResponseItemComponent postcode = new QuestionnaireResponseItemComponent();
    postcode.setLinkId("contact.address.postcode");
    postcode.setText("Postcode");
    postcode.addAnswer().setValue(new StringType(String.format("%04d", random.nextInt(10000))));
    address.addItem(postcode);

    contact.addItem(address);
    response.addItem(contact);
  }

  /**
   * Adds mental health screening questionnaire items.
   *
   * @param response the QuestionnaireResponse to add items to
   */
  private static void addMentalHealthScreenItems(final QuestionnaireResponse response) {
    // PHQ-9 style questions.
    final QuestionnaireResponseItemComponent phq9 = new QuestionnaireResponseItemComponent();
    phq9.setLinkId("phq9");
    phq9.setText("Patient Health Questionnaire-9");

    final String[] phq9Questions = {
      "Little interest or pleasure in doing things",
      "Feeling down, depressed, or hopeless",
      "Trouble falling or staying asleep, or sleeping too much",
      "Feeling tired or having little energy",
      "Poor appetite or overeating"
    };

    for (int i = 0; i < phq9Questions.length; i++) {
      final QuestionnaireResponseItemComponent item = new QuestionnaireResponseItemComponent();
      item.setLinkId("phq9." + (i + 1));
      item.setText(phq9Questions[i]);
      item.addAnswer().setValue(new IntegerType(random.nextInt(4))); // 0-3 scale.
      phq9.addItem(item);
    }

    response.addItem(phq9);

    // GAD-7 style questions.
    final QuestionnaireResponseItemComponent gad7 = new QuestionnaireResponseItemComponent();
    gad7.setLinkId("gad7");
    gad7.setText("Generalised Anxiety Disorder-7");

    final String[] gad7Questions = {
      "Feeling nervous, anxious, or on edge",
      "Not being able to stop or control worrying",
      "Worrying too much about different things"
    };

    for (int i = 0; i < gad7Questions.length; i++) {
      final QuestionnaireResponseItemComponent item = new QuestionnaireResponseItemComponent();
      item.setLinkId("gad7." + (i + 1));
      item.setText(gad7Questions[i]);
      item.addAnswer().setValue(new IntegerType(random.nextInt(4))); // 0-3 scale.
      gad7.addItem(item);
    }

    response.addItem(gad7);
  }

  /**
   * Adds medication review questionnaire items.
   *
   * @param response the QuestionnaireResponse to add items to
   */
  private static void addMedicationReviewItems(final QuestionnaireResponse response) {
    final int numMedications = random.nextInt(3) + 1;

    for (int i = 0; i < numMedications; i++) {
      final QuestionnaireResponseItemComponent medication =
          new QuestionnaireResponseItemComponent();
      medication.setLinkId("medication." + (i + 1));
      medication.setText("Medication " + (i + 1));

      // Medication name.
      final QuestionnaireResponseItemComponent medName = new QuestionnaireResponseItemComponent();
      medName.setLinkId("medication." + (i + 1) + ".name");
      medName.setText("Medication name");
      medName.addAnswer().setValue(new StringType(generateRandomMedication()));
      medication.addItem(medName);

      // Dosage.
      final QuestionnaireResponseItemComponent dosage = new QuestionnaireResponseItemComponent();
      dosage.setLinkId("medication." + (i + 1) + ".dosage");
      dosage.setText("Dosage");
      dosage.addAnswer().setValue(new StringType((random.nextInt(100) + 5) + "mg"));
      medication.addItem(dosage);

      // Frequency.
      final QuestionnaireResponseItemComponent frequency = new QuestionnaireResponseItemComponent();
      frequency.setLinkId("medication." + (i + 1) + ".frequency");
      frequency.setText("Frequency");
      final String[] frequencies = {"Once daily", "Twice daily", "Three times daily", "As needed"};
      frequency
          .addAnswer()
          .setValue(new StringType(frequencies[random.nextInt(frequencies.length)]));
      medication.addItem(frequency);

      // Adherence.
      final QuestionnaireResponseItemComponent adherence = new QuestionnaireResponseItemComponent();
      adherence.setLinkId("medication." + (i + 1) + ".adherence");
      adherence.setText("Taking as prescribed?");
      adherence.addAnswer().setValue(new BooleanType(random.nextDouble() > 0.2));
      medication.addItem(adherence);

      // If not adherent, add reason.
      if (!adherence.getAnswerFirstRep().getValueBooleanType().booleanValue()) {
        final QuestionnaireResponseItemComponent reason = new QuestionnaireResponseItemComponent();
        reason.setLinkId("medication." + (i + 1) + ".adherence.reason");
        reason.setText("Reason for non-adherence");
        final String[] reasons = {"Forgot", "Side effects", "Cost", "Don't think it's necessary"};
        reason.addAnswer().setValue(new StringType(reasons[random.nextInt(reasons.length)]));
        adherence.addItem(reason);
      }

      response.addItem(medication);
    }
  }

  /**
   * Adds symptom assessment questionnaire items.
   *
   * @param response the QuestionnaireResponse to add items to
   */
  private static void addSymptomAssessmentItems(final QuestionnaireResponse response) {
    final int numSymptoms = random.nextInt(3) + 1;

    for (int i = 0; i < numSymptoms; i++) {
      final int symptomIndex = random.nextInt(SYMPTOM_CODES.length);

      final QuestionnaireResponseItemComponent symptom = new QuestionnaireResponseItemComponent();
      symptom.setLinkId("symptom." + (i + 1));
      symptom.setText("Symptom " + (i + 1));

      // Symptom type.
      final QuestionnaireResponseItemComponent symptomType =
          new QuestionnaireResponseItemComponent();
      symptomType.setLinkId("symptom." + (i + 1) + ".type");
      symptomType.setText("Symptom type");
      final Coding symptomCoding = new Coding();
      symptomCoding.setSystem("http://snomed.info/sct");
      symptomCoding.setCode(SYMPTOM_CODES[symptomIndex]);
      symptomCoding.setDisplay(SYMPTOM_DISPLAYS[symptomIndex]);
      symptomType.addAnswer().setValue(symptomCoding);
      symptom.addItem(symptomType);

      // Severity.
      final QuestionnaireResponseItemComponent severity = new QuestionnaireResponseItemComponent();
      severity.setLinkId("symptom." + (i + 1) + ".severity");
      severity.setText("Severity");
      final int severityIndex = random.nextInt(SEVERITY_CODES.length);
      final Coding severityCoding = new Coding();
      severityCoding.setSystem("http://snomed.info/sct");
      severityCoding.setCode(SEVERITY_CODES[severityIndex]);
      severityCoding.setDisplay(SEVERITY_DISPLAYS[severityIndex]);
      severity.addAnswer().setValue(severityCoding);
      symptom.addItem(severity);

      // Duration.
      final QuestionnaireResponseItemComponent duration = new QuestionnaireResponseItemComponent();
      duration.setLinkId("symptom." + (i + 1) + ".duration");
      duration.setText("Duration (days)");
      duration.addAnswer().setValue(new IntegerType(random.nextInt(30) + 1));
      symptom.addItem(duration);

      // Onset date.
      final QuestionnaireResponseItemComponent onset = new QuestionnaireResponseItemComponent();
      onset.setLinkId("symptom." + (i + 1) + ".onset");
      onset.setText("Onset date");
      final LocalDate onsetDate = LocalDate.of(2024, 1, 1).plusDays(random.nextInt(365));
      onset
          .addAnswer()
          .setValue(
              new DateType(Date.from(onsetDate.atStartOfDay(ZoneId.systemDefault()).toInstant())));
      symptom.addItem(onset);

      // Pain scale (if pain symptom).
      if (SYMPTOM_CODES[symptomIndex].equals("22253000")) {
        final QuestionnaireResponseItemComponent painScale =
            new QuestionnaireResponseItemComponent();
        painScale.setLinkId("symptom." + (i + 1) + ".painScale");
        painScale.setText("Pain scale (0-10)");
        painScale.addAnswer().setValue(new IntegerType(random.nextInt(11)));
        symptom.addItem(painScale);

        // Pain location.
        final QuestionnaireResponseItemComponent location =
            new QuestionnaireResponseItemComponent();
        location.setLinkId("symptom." + (i + 1) + ".painScale.location");
        location.setText("Pain location");
        final String[] locations = {"Head", "Chest", "Abdomen", "Back", "Limbs"};
        location.addAnswer().setValue(new StringType(locations[random.nextInt(locations.length)]));
        painScale.addItem(location);
      }

      response.addItem(symptom);
    }
  }

  /**
   * Adds quality of life questionnaire items.
   *
   * @param response the QuestionnaireResponse to add items to
   */
  private static void addQualityOfLifeItems(final QuestionnaireResponse response) {
    // Physical health section.
    final QuestionnaireResponseItemComponent physical = new QuestionnaireResponseItemComponent();
    physical.setLinkId("physical");
    physical.setText("Physical health");

    final QuestionnaireResponseItemComponent mobility = new QuestionnaireResponseItemComponent();
    mobility.setLinkId("physical.mobility");
    mobility.setText("Mobility");
    mobility.addAnswer().setValue(new IntegerType(random.nextInt(5) + 1)); // 1-5 scale.
    physical.addItem(mobility);

    final QuestionnaireResponseItemComponent activities = new QuestionnaireResponseItemComponent();
    activities.setLinkId("physical.activities");
    activities.setText("Ability to perform daily activities");
    activities.addAnswer().setValue(new IntegerType(random.nextInt(5) + 1));
    physical.addItem(activities);

    final QuestionnaireResponseItemComponent pain = new QuestionnaireResponseItemComponent();
    pain.setLinkId("physical.pain");
    pain.setText("Pain/discomfort level");
    pain.addAnswer().setValue(new IntegerType(random.nextInt(5) + 1));
    physical.addItem(pain);

    response.addItem(physical);

    // Psychological health section.
    final QuestionnaireResponseItemComponent psychological =
        new QuestionnaireResponseItemComponent();
    psychological.setLinkId("psychological");
    psychological.setText("Psychological health");

    final QuestionnaireResponseItemComponent mood = new QuestionnaireResponseItemComponent();
    mood.setLinkId("psychological.mood");
    mood.setText("Overall mood");
    mood.addAnswer().setValue(new IntegerType(random.nextInt(5) + 1));
    psychological.addItem(mood);

    final QuestionnaireResponseItemComponent anxiety = new QuestionnaireResponseItemComponent();
    anxiety.setLinkId("psychological.anxiety");
    anxiety.setText("Anxiety level");
    anxiety.addAnswer().setValue(new IntegerType(random.nextInt(5) + 1));
    psychological.addItem(anxiety);

    response.addItem(psychological);

    // Social relationships section.
    final QuestionnaireResponseItemComponent social = new QuestionnaireResponseItemComponent();
    social.setLinkId("social");
    social.setText("Social relationships");

    final QuestionnaireResponseItemComponent relationships =
        new QuestionnaireResponseItemComponent();
    relationships.setLinkId("social.relationships");
    relationships.setText("Satisfaction with relationships");
    relationships.addAnswer().setValue(new IntegerType(random.nextInt(5) + 1));
    social.addItem(relationships);

    final QuestionnaireResponseItemComponent support = new QuestionnaireResponseItemComponent();
    support.setLinkId("social.support");
    support.setText("Social support");
    support.addAnswer().setValue(new IntegerType(random.nextInt(5) + 1));
    social.addItem(support);

    response.addItem(social);

    // Overall satisfaction.
    final QuestionnaireResponseItemComponent overall = new QuestionnaireResponseItemComponent();
    overall.setLinkId("overall");
    overall.setText("Overall life satisfaction");
    overall.addAnswer().setValue(new IntegerType(random.nextInt(10) + 1)); // 1-10 scale.
    response.addItem(overall);
  }

  /**
   * Generates a random name.
   *
   * @return a random full name
   */
  private static String generateRandomName() {
    final String[] firstNames = {
      "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
      "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica"
    };
    final String[] lastNames = {
      "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
      "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor"
    };
    return firstNames[random.nextInt(firstNames.length)]
        + " "
        + lastNames[random.nextInt(lastNames.length)];
  }

  /**
   * Generates a random city name.
   *
   * @return a random city name
   */
  private static String generateRandomCity() {
    final String[] cities = {
      "Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
      "Canberra", "Hobart", "Darwin", "Gold Coast", "Newcastle"
    };
    return cities[random.nextInt(cities.length)];
  }

  /**
   * Generates a random medication name.
   *
   * @return a random medication name
   */
  private static String generateRandomMedication() {
    final String[] medications = {
      "Paracetamol", "Ibuprofen", "Amoxicillin", "Atorvastatin", "Metformin",
      "Amlodipine", "Omeprazole", "Salbutamol", "Metoprolol", "Lisinopril",
      "Simvastatin", "Levothyroxine", "Azithromycin", "Albuterol", "Gabapentin"
    };
    return medications[random.nextInt(medications.length)];
  }
}
