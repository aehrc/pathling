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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.util.DeltaSchemaFixtures;
import au.csiro.pathling.util.TestDataSetup;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * End-to-end test for the diagnostics served when a drifted table cannot be migrated because {@code
 * schemaAutoMerge} is disabled (user story 3). This lives in its own class because the application
 * context must be started with a different configuration from {@link SchemaEvolutionReadIT}.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "pathling.storage.schemaAutoMerge=false",
      // Bind the Spark driver to localhost so that executor class fetches do not depend on the
      // machine's LAN address being reachable (e.g. under a firewall or VPN).
      "spark.driver.bindAddress=localhost",
      "spark.driver.host=localhost"
    })
@ActiveProfiles({"integration-test"})
class SchemaDriftFlagOffIT {

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static java.nio.file.Path warehouseDir;

  @BeforeEach
  void setUp() {
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
    // table's schema on disk so it is missing the gender field relative to the current encoder.
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    DeltaSchemaFixtures.removeFieldsFromTableSchema(
        warehouseDir.resolve("delta").resolve("Patient.parquet"), Set.of("gender"));
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  /**
   * User story 3, acceptance scenario 2: a request against the drifted type returns a 500 whose
   * OperationOutcome diagnostics name the type, the condition, and the remedies, per the
   * drifted-table-error contract.
   */
  @Test
  void getOfDriftedTypeReturnsActionableError() {
    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Patient/any-id")
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isEqualTo(500)
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("OperationOutcome")
        .jsonPath("$.issue[0].severity")
        .isEqualTo("error")
        .jsonPath("$.issue[0].code")
        .isEqualTo("processing")
        .jsonPath("$.issue[0].diagnostics")
        .value(
            diagnostics ->
                assertThat(diagnostics.toString())
                    .contains("Patient")
                    .contains("behind this server's encoders")
                    .contains("schemaAutoMerge"));
  }

  /** User story 3: resource types whose tables are not drifted continue to work normally. */
  @Test
  void getOfUndriftedTypeStillSucceeds() {
    webTestClient
        .get()
        .uri("http://localhost:" + port + "/fhir/Condition?_count=1")
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.resourceType")
        .isEqualTo("Bundle");
  }
}
