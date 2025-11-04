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

package au.csiro.pathling.security;

import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.operations.bulkexport.ExportRequest;
import au.csiro.pathling.util.TestDataSetup;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;


/**
 * @see <a
 * href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a
 * href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@TestPropertySource(properties = {"pathling.auth.enabled=false"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SecurityDisabledOperationsTest extends SecurityTestForOperations<ExportRequest> {

  @TempDir
  private static Path tempDir;

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    TestDataSetup.staticCopyTestDataToTempDir(tempDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + tempDir.toAbsolutePath());
  }

  @BeforeEach
  void child_setup() {
    exportProvider = setup_scenario(tempDir, "Patient", "Encounter");
  }

  @Test
  void testPassExportIfExportWithNoAuth() {
    assertThatNoException().isThrownBy(this::perform_export);
  }

  @Test
  void testPassIfExportWithTypeWithNoAuth() {
    assertThatNoException().isThrownBy(
        () -> perform_export(ADMIN_USER, List.of("Patient", "Encounter"), false));
  }

  @Order(Order.DEFAULT + 100)
  @Test
  void testPassExportResultIfExportResultWithNoAuth() {
    perform_export();
    assertThatNoException().isThrownBy(
        () -> perform_export_result(job.getId(), "Patient.00000.ndjson", null));
  }

}
