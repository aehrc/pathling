/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.test.TestResources.assertJson;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.parser.AbstractParserTest;
import au.csiro.pathling.io.Database;
import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
    "pathling.auth.ga4ghPassports.patientIdSystem=https://github.com/synthetichealth/synthea",
    "pathling.storage.databaseName=parquet"})
@Tag("Tranche2")
@Slf4j
@SpringBootTest
class ManifestConverterTest extends AbstractParserTest {

  @Autowired
  ServerConfiguration configuration;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders fhirEncoders;

  @MockBean
  ThreadPoolTaskExecutor executor;

  static final String PATIENT_ID_1 = "0dc85075-4f59-4e4f-b75d-a2f601d0cf24";
  static final String PATIENT_ID_2 = "1f276fc3-7e91-4fc9-a287-be19228e8807";
  static final String PATIENT_ID_3 = "f34e77c9-df31-49c4-92e2-e871fa76026e";
  static final String PATIENT_ID_4 = "2cc8ffdd-6233-4dd4-ba71-36eccb8204e2";
  static final List<ResourceType> AVAILABLE_RESOURCE_TYPES = List.of(
      ResourceType.ALLERGYINTOLERANCE,
      ResourceType.CAREPLAN,
      ResourceType.CLAIM,
      ResourceType.CONDITION,
      ResourceType.DIAGNOSTICREPORT,
      ResourceType.ENCOUNTER,
      ResourceType.EXPLANATIONOFBENEFIT,
      ResourceType.GOAL,
      ResourceType.IMAGINGSTUDY,
      ResourceType.IMMUNIZATION,
      ResourceType.MEDICATIONREQUEST,
      ResourceType.OBSERVATION,
      ResourceType.ORGANIZATION,
      ResourceType.PATIENT,
      ResourceType.PRACTITIONER,
      ResourceType.PROCEDURE
  );

  @DynamicPropertySource
  @SuppressWarnings("unused")
  static void registerProperties(@Nonnull final DynamicPropertyRegistry registry) {
    final File warehouseDirectory = new File("src/test/resources/test-data");
    registry.add("pathling.storage.warehouseUrl",
        () -> "file://" + warehouseDirectory.getAbsoluteFile().toPath().toString()
            .replaceFirst("/$", ""));
  }

  @Test
  void convertsManifest() {
    dataSource = Database.forConfiguration(spark, fhirEncoders, configuration.getStorage());

    final PassportScope passportScope = new PassportScope();
    final VisaManifest manifest = new VisaManifest();
    manifest.setPatientIds(Arrays.asList(PATIENT_ID_1, PATIENT_ID_2, PATIENT_ID_3, PATIENT_ID_4));

    final ManifestConverter manifestConverter = new ManifestConverter(configuration, fhirContext);
    manifestConverter.populateScope(passportScope, manifest);

    // Convert the scope to JSON and compare it to a test fixture.
    final Gson gson = new GsonBuilder().create();
    final String json = gson.toJson(passportScope);
    assertJson("responses/ManifestConverterTest/convertsManifest.json", json);

    // Go through each filter in the scope and try it out on the test data, if we have test data for 
    // that resource type. There should be at least one resource of each type linked back to one of 
    // our test patients.
    for (final ResourceType resourceType : passportScope.keySet()) {
      if (AVAILABLE_RESOURCE_TYPES.contains(resourceType)) {
        boolean found = false;
        for (final String filter : passportScope.get(resourceType)) {
          final Dataset<Row> dataset = assertThatResultOf(resourceType, filter)
              .isElementPath(BooleanCollection.class)
              .selectResult()
              .apply(result -> result.filter(col(result.columns()[1])))
              .getDataset();
          if (dataset.count() > 0) {
            found = true;
          }
        }
        assertTrue(found, "No results found for " + resourceType.toCode());
      }
    }
  }

}
