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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Integration test confirming that {@code $sqlquery-export} is not served when disabled by
 * configuration (FR-033), and is absent from the CapabilityStatement.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class SqlQueryExportDisabledIT extends AbstractSqlQueryExportIT {

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
    registry.add("pathling.operations.sqlQueryExportEnabled", () -> false);
  }

  @Test
  void operationIsNotServedWhenDisabled() {
    // The operation is not registered, so HAPI rejects the kick-off as an unknown operation
    // (a 4xx client error) rather than accepting it for processing.
    kickOff(systemLevelUri(), storedQuery("any-library", null)).expectStatus().is4xxClientError();
  }

  @Test
  void capabilityStatementDoesNotDeclareTheOperationWhenDisabled() {
    final byte[] body =
        webTestClient
            .get()
            .uri("http://localhost:" + port + "/fhir/metadata")
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    final String metadata = new String(body == null ? new byte[0] : body, StandardCharsets.UTF_8);
    assertThat(metadata).doesNotContain("sqlquery-export");
  }
}
