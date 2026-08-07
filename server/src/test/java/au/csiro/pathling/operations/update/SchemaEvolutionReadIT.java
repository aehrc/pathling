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

package au.csiro.pathling.operations.update;

import au.csiro.pathling.util.DeltaSchemaFixtures;
import au.csiro.pathling.util.TestDataSetup;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end test for schema drift handling with {@code schemaAutoMerge} enabled, mirroring the
 * production incident where a server upgrade left the on-disk table schema behind the encoders.
 *
 * <p>The warehouse is prepared before the application context starts: the Patient Delta table's
 * schema is downgraded on disk by removing the top-level {@code gender} field and the {@code
 * prefix} and {@code suffix} fields nested inside the name element, simulating a table written by
 * an older server version whose encoder lacked them.
 *
 * <p>Covers the acceptance scenarios of user stories 1 and 2: a resource of the drifted type is
 * readable from boot without any prior write (startup migration), and a PUT followed immediately by
 * a GET and a type-level search succeeds with no restart (runtime refresh). The nested fields also
 * cover the read path against a struct the migration actually had to evolve, which the top-level
 * field alone does not.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "pathling.storage.schemaAutoMerge=true",
      // Bind the Spark driver to localhost so that executor class fetches do not depend on the
      // machine's LAN address being reachable (e.g. under a firewall or VPN).
      "spark.driver.bindAddress=localhost",
      "spark.driver.host=localhost"
    })
@ActiveProfiles({"integration-test"})
@TestMethodOrder(OrderAnnotation.class)
class SchemaEvolutionReadIT {

  /** A Patient id that exists in the prebuilt test data. */
  private static final String EXISTING_PATIENT_ID = "72df0f76-2758-fac4-67cd-de33c4a2c95e";

  private static final String NEW_PATIENT_ID = "schema-drift-test-patient";

  /** The id used to exercise the fields the migration added inside a nested struct. */
  private static final String NESTED_PATIENT_ID = "schema-drift-nested-patient";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static java.nio.file.Path warehouseDir;

  @BeforeEach
  void setUp() {
    // The first write against a fresh Spark session can take well over the default five second
    // response timeout.
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(java.time.Duration.ofSeconds(120))
            .build();
  }

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) throws Exception {
    // Copy the prebuilt Delta test data into a temporary warehouse, then downgrade the Patient
    // table's schema on disk. The top-level gender field covers the name-resolved case; prefix and
    // suffix, which live inside the name element's nested structs, cover the case that startup
    // migration actually has to evolve and that the decoder then has to read back at the right
    // offsets.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    DeltaSchemaFixtures.removeFieldsFromTableSchema(
        warehouseDir.resolve("delta").resolve("Patient.parquet"),
        Set.of("gender", "prefix", "suffix"));
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  /**
   * User story 2, acceptance scenario 2: with {@code schemaAutoMerge} enabled, a resource of the
   * drifted type is readable immediately after startup, with no prior write, because the table was
   * migrated at startup. This test must run before any write against the type.
   */
  @Test
  @Order(1)
  void getSucceedsAfterStartupWithNoPriorWrite() {
    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient/" + EXISTING_PATIENT_ID)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Patient")
        .jsonPath("$.id")
        .isEqualTo(EXISTING_PATIENT_ID);
  }

  /**
   * User story 1, acceptance scenarios 1 and 2: a PUT of a resource whose type's table schema is
   * behind the encoder succeeds, and an immediate GET by id and type-level search both succeed
   * without a server restart.
   */
  @Test
  @Order(2)
  void putThenGetAndSearchSucceedWithoutRestart() {
    final String patientJson =
        """
        {
          "resourceType": "Patient",
          "id": "%s",
          "gender": "female",
          "name": [
            {
              "family": "Drift",
              "given": ["Schema"]
            }
          ]
        }
        """
            .formatted(NEW_PATIENT_ID);

    // PUT the resource; with schemaAutoMerge enabled this succeeds regardless of table drift.
    webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Patient/" + NEW_PATIENT_ID)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(patientJson)
        .exchange()
        .expectStatus()
        .isOk();

    // An immediate GET by id must return the stored resource.
    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient/" + NEW_PATIENT_ID)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Patient")
        .jsonPath("$.id")
        .isEqualTo(NEW_PATIENT_ID)
        .jsonPath("$.gender")
        .isEqualTo("female");

    // A type-level search must also succeed and include the resource.
    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient?_count=200")
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle")
        .jsonPath("$.entry[?(@.resource.id == '%s')]".formatted(NEW_PATIENT_ID))
        .exists();
  }

  /**
   * The fields the startup migration added inside a nested struct must read back correctly, and so
   * must the fields that already surrounded them.
   *
   * <p>This closes a gap in the existing coverage: drifting only the top-level {@code gender}
   * column exercises nothing, because Delta resolves top-level columns by name and a table missing
   * one is simply read as null. A field inside the {@code name} element's struct is different - the
   * migration genuinely rewrites that struct's field list, and a decoder that resolved nested
   * fields by position would then read this row at the wrong offsets, returning wrong values or
   * faulting. That is the defect fixed in the core by {@code 048-name-based-nested-decode}, and
   * this is the server-side confirmation that the fix reaches the read and search paths.
   */
  @Test
  @Order(3)
  void nestedFieldsAddedByMigrationReadBackCorrectly() {
    final String patientJson =
        """
        {
          "resourceType": "Patient",
          "id": "%s",
          "gender": "male",
          "name": [
            {
              "prefix": ["Dr"],
              "family": "Nested",
              "given": ["Drift", "Field"],
              "suffix": ["Jr"]
            }
          ]
        }
        """
            .formatted(NESTED_PATIENT_ID);

    webTestClient
        .put()
        .uri("http://localhost:" + port + "/fhir/Patient/" + NESTED_PATIENT_ID)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(patientJson)
        .exchange()
        .expectStatus()
        .isOk();

    // Every field of the nested struct must come back as sent - the migrated ones and the ones that
    // surrounded them, whose offsets shifted when the struct was evolved.
    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient/" + NESTED_PATIENT_ID)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.name[0].prefix[0]")
        .isEqualTo("Dr")
        .jsonPath("$.name[0].family")
        .isEqualTo("Nested")
        .jsonPath("$.name[0].given[0]")
        .isEqualTo("Drift")
        .jsonPath("$.name[0].given[1]")
        .isEqualTo("Field")
        .jsonPath("$.name[0].suffix[0]")
        .isEqualTo("Jr")
        .jsonPath("$.gender")
        .isEqualTo("male");

    // A search reads the same rows through a different path, so it must agree.
    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient?_count=200")
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath(
            "$.entry[?(@.resource.id == '%s')].resource.name[0].family"
                .formatted(NESTED_PATIENT_ID))
        .isEqualTo("Nested");
  }
}
