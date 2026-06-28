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

import java.nio.file.Path;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Integration tests for the type-level and instance-level {@code $sqlquery-export} operations.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlQueryExportTestConfiguration.class)
class SqlQueryExportInstanceIT extends AbstractSqlQueryExportIT {

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  @Test
  void instanceLevelExportProducesOneOutputForBoundLibrary() throws InterruptedException {
    final Map<String, Object> manifest =
        exportToCompletion(
            instanceLevelUri(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID), emptyParameters());

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    assertThat(paramsByName(manifest, "output")).hasSize(1);
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content).contains("\"family_name\":\"Smith\"");
  }

  @Test
  void instanceLevelExportForNonExistentLibraryReturns404() {
    kickOff(instanceLevelUri("does-not-exist"), emptyParameters()).expectStatus().isNotFound();
  }

  @Test
  void typeLevelExportWithQueryProducesOutput() throws InterruptedException {
    final Map<String, Object> manifest =
        exportToCompletion(
            typeLevelUri(), storedQuery(SqlQueryExportTestConfiguration.PATIENT_QUERY_ID, null));

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    assertThat(paramsByName(manifest, "output")).hasSize(1);
  }
}
