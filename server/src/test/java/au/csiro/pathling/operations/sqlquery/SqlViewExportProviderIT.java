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
 * End-to-end integration test for {@code $sqlquery-export} against stored SQLView {@code Library}
 * resources, confirming that the asynchronous export resolves and materialises the same dependency
 * graph as {@code $sqlquery-run} (US3 - run or export a SQLView directly).
 *
 * <p>Backed by {@link SqlViewTestConfiguration}, which holds the ViewDefinitions, SQLViews, and
 * FHIR data the queries resolve against.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
@Import(SqlViewTestConfiguration.class)
class SqlViewExportProviderIT extends AbstractSqlQueryExportIT {

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  @Test
  void instanceLevelExportOfStoredSqlViewProducesComposedRows() throws InterruptedException {
    // Exporting a stored SQLView at the instance level resolves its ViewDefinition dependency and
    // writes the composed rows, identically to running it.
    final Map<String, Object> manifest =
        exportToCompletion(
            instanceLevelUri(SqlViewTestConfiguration.ACTIVE_PATIENTS_ID), emptyParameters());

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    assertThat(paramsByName(manifest, "output")).hasSize(1);
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content)
        .contains("\"family_name\":\"Smith\"")
        .contains("\"family_name\":\"Johnson\"")
        .contains("\"family_name\":\"Williams\"");
  }

  @Test
  void typeLevelExportWithQueryReferenceToSqlViewProducesOutput() throws InterruptedException {
    // A SQLView referenced as a top-level query at the type level exports identically.
    final Map<String, Object> manifest =
        exportToCompletion(
            typeLevelUri(), storedQuery(SqlViewTestConfiguration.ACTIVE_PATIENTS_ID, null));

    assertThat(findParamValue(manifest, "status", "valueCode")).isEqualTo("completed");
    assertThat(paramsByName(manifest, "output")).hasSize(1);
    final String content =
        download(partValue(paramsByName(manifest, "output").get(0), "location", "valueUri"));
    assertThat(content).contains("\"family_name\":\"Smith\"");
  }
}
